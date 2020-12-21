"""
Unicast Heartbeat module
"""
import sys
import socket
import threading
import time

import foreign.six as six
import core.exceptions as ex
import daemon.shared as shared
from env import Env
from .hb import Hb
from utilities.render.listener import fmt_listener


class HbUcast(Hb):
    """
    A class factorizing common methods and properties for the unicast
    heartbeat tx and rx child classes.
    """
    def __init__(self, name, role=None):
        super(HbUcast, self).__init__(name, role)
        self.peer_config = {}
        self.config_change = False
        self.timeout = None

    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "timeout": self.timeout,
        }
        try:
            data["config"]["addr"] = self.peer_config[Env.nodename]["addr"]
            data["config"]["port"] = self.peer_config[Env.nodename]["port"]
        except (TypeError, KeyError):
            # thread not configured yet
            pass
        return data

    def configure(self):
        self.reset_stats()
        self._configure()

    def reconfigure(self):
        self._configure()

    def _configure(self):
        self.get_hb_nodes()
        peer_config = {}

        if self.name not in shared.NODE.cd:
            # this thread will be stopped. don't reconfigure to avoid logging errors
            return

        # peers
        for nodename in self.hb_nodes:
            if nodename not in peer_config:
                addr = shared.NODE.oget(self.name, "addr", impersonate=nodename)
                port = shared.NODE.oget(self.name, "port", impersonate=nodename)
                if addr is not None:
                    pass
                elif nodename == Env.nodename:
                    addr = "::"
                else:
                    addr = nodename
                peer_config[nodename] = {
                    "addr": addr,
                    "port": port,
                }

        if peer_config != self.peer_config:
            self.config_change = True
            self.peer_config = peer_config

        timeout = shared.NODE.oget(self.name, "timeout")
        if timeout != self.timeout:
            self.config_change = True
            self.timeout = timeout

        interval = shared.NODE.oget(self.name, "interval")
        if interval != self.interval:
            self.config_change = True
            self.interval = interval

        self.max_handlers = len(self.hb_nodes) * 4


class HbUcastTx(HbUcast):
    """
    The unicast heartbeat tx class.
    """
    sock_tmo = 1.0

    def __init__(self, name, role="tx"):
        super(HbUcastTx, self).__init__(name, role=role)

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction as exc:
            self.log.exception("error during configure step", exc)
            return

        try:
            while True:
                self.do()
                if self.stopped():
                    self.log.info('sys.exit()')
                    sys.exit(0)
                with shared.HB_TX_TICKER:
                    shared.HB_TX_TICKER.wait(self.interval)
        except Exception as exc:
            self.log.exception(exc)

    def status(self, **kwargs):
        data = HbUcast.status(self, **kwargs)
        data["config"] = {}
        return data

    def do(self):
        self.janitor_procs()
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        for nodename, config in self.peer_config.items():
            if nodename == Env.nodename:
                continue
            self._do(message, message_bytes, nodename, config)

    def _do(self, message, message_bytes, nodename, config):
        sock = None
        try:
            # self.log.info("sending to %s:%s", config["addr"], config["port"])
            sock = socket.create_connection((config["addr"], config["port"]), self.sock_tmo)
            sock.sendall((message+"\0").encode())
            self.set_last(nodename)
            self.push_stats(message_bytes)
        except socket.timeout:
            self.push_stats()
            if self.get_last(nodename).success:
                self.log.warning("send to %s (%s:%d) timeout", nodename,
                                 config["addr"], config["port"])
            self.set_last(nodename, success=False)
        except socket.error as exc:
            self.push_stats()
            if self.get_last(nodename).success:
                self.log.warning("send to %s (%s:%d) error: %s", nodename,
                                 config["addr"], config["port"], str(exc))
            self.set_last(nodename, success=False)
        except Exception as exc:
            self.push_stats()
            if self.get_last(nodename).success:
                self.log.error("send to %s (%s:%d) unexpected error: %s", nodename,
                                 config["addr"], config["port"], str(exc))
            self.set_last(nodename, success=False)
        finally:
            self.set_beating(nodename)
            if sock is not None:
                sock.close()


class HbUcastRx(HbUcast):
    """
    The unicast heartbeat rx class.
    """
    def __init__(self, name, role="rx"):
        super(HbUcastRx, self).__init__(name, role=role)
        self.sock = None
        self.sock_accept_tmo = 2.0
        self.sock_recv_tmo = 5.0

    def _configure(self):
        super(HbUcastRx, self)._configure()
        if not self.config_change:
            return
        self.config_change = False
        if self.sock:
            self.sock.close()
        local_exception = None
        for _ in range(3):
            try:
                self.configure_listener()
                return
            except socket.error as exc:
                local_exception = exc
                time.sleep(1)
        self.log.error("init error: %s", str(local_exception))
        raise ex.AbortAction

    def configure_listener(self):
        addr = self.peer_config[Env.nodename]["addr"]
        port = self.peer_config[Env.nodename]["port"]
        af = socket.AF_INET6 if ":" in addr else socket.AF_INET
        self.sock = socket.socket(af, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((addr, port))
        self.sock.listen(5)
        self.sock.settimeout(self.sock_accept_tmo)
        self.log.info("listening on %s", fmt_listener(addr, port))

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction as exc:
            self.log.exception("error during configure step", exc)
            return
        except Exception as exc:
            self.log.error("%s", exc)
            raise exc

        while True:
            try:
                self.do()
            except Exception as exc:
                self.log.error("during do(): %s", exc)
                self.log.exception(exc)
                raise exc
            if self.stopped():
                self.join_threads()
                self.sock.close()
                self.log.info('sys.exit()')
                sys.exit(0)

    def do(self):
        self.reload_config()
        self.janitor_procs()
        self.janitor_threads()

        try:
            conn, addr = self.sock.accept()
            self.sock.settimeout(self.sock_recv_tmo)
        except socket.timeout:
            return
        finally:
            self.set_peers_beating()
        if len(self.threads) >= self.max_handlers:
            self.log.warning("drop message received from %s: too many running handlers (%d)",
                             addr, self.max_handlers)
            return
        try:
            thr = threading.Thread(target=self.handle_client, args=(conn, addr))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning(exc)
            conn.close()

    def handle_client(self, conn, addr):
        try:
            self._handle_client(conn, addr)
        finally:
            conn.close()

    def _handle_client(self, conn, addr):
        chunks = []
        buff_size = 4096
        while True:
            chunk = conn.recv(buff_size)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        data = six.b("").join(chunks)
        self.push_stats(len(data))
        del chunks

        clustername, nodename, data = self.decrypt(data, sender_id=addr[0])
        if clustername != self.cluster_name:
            return
        if nodename is None or nodename == Env.nodename:
            # ignore hb data we sent ourself
            return
        if nodename not in self.hb_nodes:
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
