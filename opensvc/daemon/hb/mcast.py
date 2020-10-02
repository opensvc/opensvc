"""
Multicast Heartbeat
"""
import sys
import socket
import threading
import struct
import uuid
import json
import time

import core.exceptions as ex
import daemon.shared as shared
from env import Env
from utilities.chunker import chunker
from utilities.string import bdecode
from .hb import Hb

MAX_MESSAGES = 100
MAX_FRAGMENTS = 1000

class HbMcast(Hb):
    """
    A class factorizing common methods and properties for the multicast
    heartbeat tx and rx child classes.
    """
    src_addr = None
    port = None
    intf = None
    addr = None
    sock = None
    max_data = 1000

    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "addr": self.addr,
            "port": self.port,
            "intf": self.intf,
            "src_addr": self.src_addr,
            "timeout": self.timeout,
            "interval": self.interval,
        }
        return data

    def configure(self):
        self.reset_stats()
        self._configure()

    def reconfigure(self):
        self._configure()

    def _configure(self):
        pass

    def apply_changes(self):
        changed = False
        if self.name not in shared.NODE.cd:
            # this thread will be stopped. don't reconfigure to avoid logging errors
            return changed
        self.get_hb_nodes()
        prev = {
            "port": self.port,
            "addr": self.addr,
            "intf": self.intf,
            "timeout": self.timeout,
            "interval": self.interval
        }
        self.port = shared.NODE.oget(self.name, "port")
        self.addr = shared.NODE.oget(self.name, "addr")
        self.timeout = shared.NODE.oget(self.name, "timeout")
        self.interval = shared.NODE.oget(self.name, "interval")
        group = socket.inet_aton(self.addr)
        try:
            self.intf = shared.NODE.conf_get(self.name, "intf")
            self.src_addr = self.get_ip_address(self.intf)
            self.mreq = group + socket.inet_aton(self.src_addr)
        except Exception as exc:
            self.alert("warning", "fallback to any intf: %s", exc)
            self.intf = "any"
            self.src_addr = "0.0.0.0"
            self.mreq = struct.pack("4sl", group, socket.INADDR_ANY)
        self.max_handlers = len(self.hb_nodes) * 4

        # log changes
        changes = []
        for key, val in prev.items():
            new_val = getattr(self, key)
            if val != new_val:
                changed = True
                if val is not None:
                    changes.append("%s %s => %s" % (key, val, new_val))
        if changes:
            self.log.info(", ".join(changes))
        return changed

    def set_if(self):
        if self.intf == "any":
            return
        try:
            intf_b = bytes(self.intf, "utf-8")
        except TypeError:
            intf_b = str(self.intf)
        try:
            # Linux only
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, intf_b)
        except AttributeError:
            pass
        if self.src_addr != "0.0.0.0":
            try:
                self.log.info("set socket options: multicast source address %s", self.src_addr)
                self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.src_addr))
            except Exception as exc:
                self.log.error("%s", exc)

class HbMcastTx(HbMcast):
    """
    The multicast heartbeat tx class.
    """
    sock_tmo = 2

    def __init__(self, name):
        HbMcast.__init__(self, name, role="tx")

    def _configure(self):
        changed = self.apply_changes()
        if not changed:
            return
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.set_if()
            ttl = struct.pack('b', 32)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            self.sock.settimeout(self.sock_tmo)
            self.group = (self.addr, self.port)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            raise ex.AbortAction

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction:
            return
        try:
            while True:
                self.do()
                if self.stopped():
                    self.sock.close()
                    sys.exit(0)
                with shared.HB_TX_TICKER:
                    shared.HB_TX_TICKER.wait(self.interval)
        except Exception as exc:
            self.log.exception(exc)

    def do(self):
        self.janitor_procs()
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        #self.log.info("sending to %s:%s", self.addr, self.port)
        try:
            idx = 1
            mid = str(uuid.uuid4())
            total = message_bytes // self.max_data
            if message_bytes % self.max_data:
                total += 1
            for chunk in chunker(message, self.max_data):
                payload = (json.dumps({
                    "id": mid,
                    "i": idx,
                    "n": total,
                    "c": chunk,
                }) + "\0").encode()
                sent = self.sock.sendto(payload, self.group)
                #self.log.info("send %s %d/%d", mid, idx, total)
                idx += 1
            self.set_last()
            self.push_stats(message_bytes)
        except socket.timeout as exc:
            self.push_stats()
            if self.get_last().success:
                self.log.warning("send timeout")
            self.set_last(success=False)
        except socket.error as exc:
            self.push_stats()
            if self.get_last().success:
                self.log.warning("send error: %s", exc)
            self.set_last(success=False)
        finally:
            self.set_beating()


#
class HbMcastRx(HbMcast):
    """
    The multicast heartbeat rx class.
    """
    sock_tmo = 2
    fragments = {}

    def __init__(self, name):
        HbMcast.__init__(self, name, role="rx")

    def _configure(self):
        changed = self.apply_changes()
        if not changed:
            return
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
        err = None
        for _ in range(3):
            try:
                self.configure_listener()
                return
            except socket.error as exc:
                time.sleep(1)
                err = str(exc)
        self.log.error("init error: %s", err)
        raise ex.AbortAction

    def configure_listener(self):
        addrinfo = socket.getaddrinfo(self.addr, None)[0]
        self.addr = addrinfo[4][0]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.set_if()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
        self.sock.bind(('', self.port))
        self.sock.settimeout(self.sock_tmo)
        self.log.info("listening on %s:%s", self.addr, self.port)

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction:
            return

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def do(self):
        def handle(data, addr):
            try:
                thr = threading.Thread(target=self.handle_client, args=(data, addr))
                thr.start()
                self.threads.append(thr)
            except RuntimeError as exc:
                self.log.warning(exc)

        self.reload_config()
        self.janitor_procs()
        self.janitor_threads()

        try:
            data, addr = self.sock.recvfrom(shared.MAX_MSG_SIZE)
            self.push_stats(len(data))
        except socket.timeout:
            self.set_peers_beating()
            return

        if len(self.threads) >= self.max_handlers:
            self.log.warning("drop message received from %s: too many running handlers (%d)",
                             addr, self.max_handlers)
            self.fragments = {}
            return

        try:
            payload = json.loads(bdecode(data).rstrip("\0\x00"))
        except (ValueError, TypeError) as exc:
            # old format ? try decrypt. will blacklist if failed.
            handle(data, addr)
            return

        try:
            mid = payload["id"]
            chunk = payload["c"]
            idx = payload["i"]
            total = payload["n"]
        except KeyError:
            return

        # verify message DoS
        if addr not in self.fragments:
            self.fragments[addr] = {}
        elif len(self.fragments[addr]) > MAX_MESSAGES:
            self.log.warning("too many pending messages. purge")
            self.fragments[addr] = {}

        # verify fragment DoS
        if mid not in self.fragments[addr]:
            self.fragments[addr][mid] = {}
        elif len(self.fragments[addr][mid]) > MAX_FRAGMENTS:
            self.log.warning("too many pending message fragments. purge")
            del self.fragments[addr][mid]
            return

        # store fragment
        self.fragments[addr][mid][idx] = chunk

        if len(self.fragments[addr][mid]) != total:
            # not yet complete
            return

        #self.log.debug("message %s complete", mid)
        message = ""
        for idx in sorted(self.fragments[addr][mid].keys()):
            message += self.fragments[addr][mid][idx]
        handle(message, addr)
        self.fragments[addr] = {}

    def handle_client(self, message, addr):
        clustername, nodename, data = self.decrypt(message, sender_id=addr[0])
        if clustername != self.cluster_name:
            # surely from drp node
            return
        if nodename is None or nodename == Env.nodename:
            # ignore hb data we sent ourself
            return
        elif nodename not in self.hb_nodes:
            return
        if data is None:
            self.push_stats()
            self.set_beating(nodename)
            return
        try:
            self.queue_rx_data(data, nodename)
            self.set_last(nodename)
        except Exception as exc:
            if self.get_last(nodename).success:
                self.log.error("%s", exc)
            self.set_last(nodename, success=False)
        finally:
            self.set_beating(nodename)
            self.set_peers_beating()



