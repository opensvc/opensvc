"""
Multicast Heartbeat
"""
import sys
import socket
import threading
import struct

import rcExceptions as ex
import osvcd_shared as shared
from rcGlobalEnv import rcEnv, Storage
from comm import Crypt
from hb import Hb

class HbMcast(Hb, Crypt):
    """
    A class factorizing common methods and properties for the multicast
    heartbeat tx and rx child classes.
    """
    DEFAULT_MCAST_PORT = 10000
    DEFAULT_MCAST_ADDR = "224.3.29.71"
    DEFAULT_MCAST_TIMEOUT = 15

    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data.stats = Storage(self.stats)
        data.config = {
            "addr": self.addr,
            "port": self.port,
            "intf": self.intf,
            "src_addr": self.src_addr,
            "timeout": self.timeout,
        }
        return data

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        self._configure()

    def reconfigure(self):
        self._configure()

    def _configure(self):
        try:
            self.port = self.config.getint(self.name, "port")
        except Exception:
            self.port = self.DEFAULT_MCAST_PORT
        try:
            self.addr = self.config.get(self.name, "addr")
        except Exception:
            self.addr = self.DEFAULT_MCAST_ADDR
        try:
            self.timeout = self.config.getint(self.name, "timeout")
        except Exception:
            self.timeout = self.DEFAULT_MCAST_TIMEOUT
        group = socket.inet_aton(self.addr)
        try:
            self.intf = self.config.get(self.name, "intf")
            self.src_addr = self.get_ip_address(self.intf)
            self.mreq = group + socket.inet_aton(self.src_addr)
        except Exception:
            self.intf = "any"
            self.src_addr = "0.0.0.0"
            self.mreq = struct.pack("4sl", group, socket.INADDR_ANY)

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
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, self.mreq)

class HbMcastTx(HbMcast):
    """
    The multicast heartbeat tx class.
    """
    def __init__(self, name):
        HbMcast.__init__(self, name, role="tx")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            ttl = struct.pack('b', 32)
            self.addr = addrinfo[4][0]
            self.group = (self.addr, self.port)
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.set_if()
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            self.sock.settimeout(2)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        try:
            while True:
                self.do()
                if self.stopped():
                    self.sock.close()
                    sys.exit(0)
                with shared.HB_TX_TICKER:
                    shared.HB_TX_TICKER.wait(self.default_hb_period)
        except Exception as exc:
            self.log.exception(exc)

    def do(self):
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        #self.log.info("sending to %s:%s", self.addr, self.port)
        try:
            sent = self.sock.sendto(message, self.group)
            self.set_last()
            self.stats.beats += 1
            self.stats.bytes += message_bytes
        except socket.timeout as exc:
            self.stats.errors += 1
            self.log.warning("send timeout")
        except socket.error as exc:
            self.stats.errors += 1
            self.log.warning("send error: %s", exc)
        finally:
            self.set_beating()


#
class HbMcastRx(HbMcast):
    """
    The multicast heartbeat rx class.
    """
    def __init__(self, name):
        HbMcast.__init__(self, name, role="rx")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return
        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.set_if()
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
            self.sock.bind(('', self.port))
            self.sock.settimeout(2)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        self.log.info("listening on %s:%s", self.addr, self.port)

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def do(self):
        self.reload_config()
        self.janitor_threads()

        try:
            data, addr = self.sock.recvfrom(shared.MAX_MSG_SIZE)
            self.stats.beats += 1
            self.stats.bytes += len(data)
        except socket.timeout:
            self.set_peers_beating()
            return
        thr = threading.Thread(target=self.handle_client, args=(data, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, message, addr):
        nodename, data = self.decrypt(message, sender_id=addr[0])
        if nodename is None or nodename == rcEnv.nodename:
            # ignore hb data we sent ourself
            return
        elif nodename not in self.cluster_nodes:
            # decrypt passed, trust it is a new node
            self.add_cluster_node(nodename)
        if data is None:
            self.stats.errors += 1
            self.set_beating(nodename)
            return
        #self.log.info("received data from %s %s", nodename, addr)
        with shared.CLUSTER_DATA_LOCK:
            shared.CLUSTER_DATA[nodename] = data
        self.set_last(nodename)
        self.set_beating(nodename)
        self.set_peers_beating()



