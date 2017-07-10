"""
Unicast Heartbeat module
"""
import sys
import socket
import threading

import rcExceptions as ex
import osvcd_shared as shared
from rcGlobalEnv import rcEnv, Storage
from comm import Crypt
from hb import Hb

class HbUcast(Hb, Crypt):
    """
    A class factorizing common methods and properties for the unicast
    heartbeat tx and rx child classes.
    """
    DEFAULT_UCAST_PORT = 10000
    DEFAULT_UCAST_TIMEOUT = 15

    def status(self):
        data = Hb.status(self)
        data.stats = Storage(self.stats)
        data.config = {
            "addr": self.peer_config[rcEnv.nodename].addr,
            "port": self.peer_config[rcEnv.nodename].port,
            "timeout": self.timeout,
        }
        return data

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        self.peer_config = {}
        if hasattr(self, "node"):
            config = self.node.config
        else:
            config = self.config
        try:
            default_port = config.getint(self.name, "port")
        except Exception:
            default_port = self.DEFAULT_UCAST_PORT + 0

        for nodename in self.cluster_nodes:
            if nodename not in self.peer_config:
                self.peer_config[nodename] = Storage({
                    "addr": nodename,
                    "port": default_port,
                })
            if config.has_option(self.name, "addr@"+nodename):
                self.peer_config[nodename].addr = \
                    config.get(self.name, "addr@"+nodename)
            if config.has_option(self.name, "port@"+nodename):
                self.peer_config[nodename].port = \
                    config.getint(self.name, "port@"+nodename)

        # timeout
        if self.config.has_option(self.name, "timeout@"+rcEnv.nodename):
            self.timeout = \
                self.config.getint(self.name, "timeout@"+rcEnv.nodename)
        elif self.config.has_option(self.name, "timeout"):
            self.timeout = self.config.getint(self.name, "timeout")
        else:
            self.timeout = self.DEFAULT_UCAST_TIMEOUT

        for nodename in self.peer_config:
            if nodename == rcEnv.nodename:
                continue
            if self.peer_config[nodename].port is None:
                self.peer_config[nodename].port = \
                    self.peer_config[rcEnv.nodename].port

class HbUcastTx(HbUcast):
    """
    The unicast heartbeat tx class.
    """
    def __init__(self, name):
        HbUcast.__init__(self, name, role="tx")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.default_hb_period)

    def status(self):
        data = HbUcast.status(self)
        data["config"] = {}
        return data

    def do(self):
        #self.log.info("sending to %s:%s", self.addr, self.port)
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        for nodename, config in self.peer_config.items():
            if nodename == rcEnv.nodename:
                continue
            self._do(message, message_bytes, nodename, config)

    def _do(self, message, message_bytes, nodename, config):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            sock.bind((self.peer_config[rcEnv.nodename].addr, 0))
            sock.connect((config.addr, config.port))
            sock.sendall(message)
            self.set_last(nodename)
            self.stats.beats += 1
            self.stats.bytes += message_bytes
        except socket.timeout as exc:
            self.stats.errors += 1
            self.log.warning("send to %s (%s:%d) timeout", nodename,
                             config.addr, config.port)
        except socket.error as exc:
            self.stats.errors += 1
            self.log.error("send to %s (%s:%d) error: %s", nodename,
                           config.addr, config.port, str(exc))
            return
        finally:
            self.set_beating(nodename)
            sock.close()

class HbUcastRx(HbUcast):
    """
    The unicast heartbeat rx class.
    """
    def __init__(self, name):
        HbUcast.__init__(self, name, role="rx")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.peer_config[rcEnv.nodename].addr,
                            self.peer_config[rcEnv.nodename].port))
            self.sock.listen(5)
            self.sock.settimeout(0.5)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        self.log.info("listening on %s:%s",
                      self.peer_config[rcEnv.nodename].addr,
                      self.peer_config[rcEnv.nodename].port)

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
            conn, addr = self.sock.accept()
        except socket.timeout:
            return
        finally:
            self.set_peers_beating()
        thr = threading.Thread(target=self.handle_client, args=(conn, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, conn, addr):
        try:
            self._handle_client(conn, addr)
            self.stats.beats += 1
        finally:
            conn.close()

    def _handle_client(self, conn, addr):
        chunks = []
        buff_size = 4096
        while True:
            chunk = conn.recv(buff_size)
            self.stats.bytes += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if sys.version_info[0] >= 3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        del chunks

        nodename, data = self.decrypt(data, sender_id=addr[0])
        if nodename is None or nodename == rcEnv.nodename:
            # ignore hb data we sent ourself
            return
        elif nodename not in self.cluster_nodes:
            # decrypt passed, trust it is a new node
            self.add_cluster_node()
        if data is None:
            self.stats.errors += 1
            self.set_beating(nodename)
            return
        #self.log.info("received data from %s %s", nodename, addr)
        with shared.CLUSTER_DATA_LOCK:
            shared.CLUSTER_DATA[nodename] = data
        self.set_last(nodename)
        self.set_beating(nodename)



