"""
 Heartbeat parent class
"""
import logging
import time

import daemon.shared as shared
import core.exceptions as ex
import utilities.ifconfig
from env import Env
from utilities.storage import Storage

class Hb(shared.OsvcThread):
    """
    Heartbeat parent class
    """
    interval = 5
    timeout = None

    def __init__(self, name, role=None):
        shared.OsvcThread.__init__(self)
        self.name = name
        self.id = name + "." + role
        self.thread_data = self.daemon_status_data.view([self.id])
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd."+self.id), {"node": Env.nodename, "component": self.id})
        self.peers = {}
        self.reset_stats()
        self.hb_nodes = []
        self.get_hb_nodes()
        self.msg_type = None

    def get_hb_nodes(self):
        try:
            new_nodes = [node for node in shared.NODE.conf_get(self.name, "nodes")
                         if node in self.cluster_nodes]
        except ex.OptNotFound:
            new_nodes = self.cluster_nodes
        if new_nodes != self.hb_nodes:
            self.log.info('hb nodes: %s', new_nodes)
            self.hb_nodes = new_nodes

    def push_stats(self, _bytes=-1):
        if _bytes < 0:
            self.stats.errors += 1
        else:
            self.stats.beats += 1
            self.stats.bytes += _bytes

    def reset_stats(self):
        self.stats = Storage({
            "since": time.time(),
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        running = data.get("state") == "running"
        data["peers"] = {}
        for nodename in self.hb_nodes:
            if nodename == Env.nodename:
                data["peers"][nodename] = {}
                continue
            if "*" in self.peers:
                _data = self.peers["*"]
            else:
                _data = self.peers.get(nodename, Storage({
                    "last": 0,
                    "beating": False,
                    "success": True,
                }))
            data["peers"][nodename] = {
                "last": _data.last,
                "beating": _data.beating if running else False,
            }
        return data

    def set_last(self, nodename="*", success=True):
        if nodename not in self.peers:
            self.peers[nodename] = Storage({
                "last": 0,
                "beating": False,
                "success": True,
            })
        if success:
            self.peers[nodename].last = time.time()
            if not self.peers[nodename].beating:
                self.event("hb_beating", data={
                    "nodename": nodename,
                    "hb": {"name": self.name, "id": self.id},
                })
            self.peers[nodename].beating = True
        self.peers[nodename].success = success

    def get_last(self, nodename="*"):
        if nodename in self.peers:
            return self.peers[nodename]
        return Storage({
            "last": 0,
            "beating": False,
            "success": True,
        })

    def is_beating(self, nodename="*"):
        return self.peers.get(nodename, {"beating": False})["beating"]

    def set_peers_beating(self):
        for nodename in list(self.peers.keys()):
            self.set_beating(nodename)

    def set_beating(self, nodename="*"):
        now = time.time()
        if nodename not in self.peers:
            self.peers[nodename] = Storage({
                "last": 0,
                "beating": False,
                "success": True,
            })
        if now > self.peers[nodename].last + self.timeout:
            beating = False
        else:
            beating = True
        change = False
        if self.peers[nodename].beating != beating:
            change = True
            if beating:
                self.event("hb_beating", data={
                    "nodename": nodename,
                    "hb": {"name": self.name, "id": self.id},
                })
            else:
                self.event("hb_stale", data={
                    "nodename": nodename,
                    "hb": {
                        "name": self.name, "id": self.id,
                        "timeout": self.timeout,
                        "interval": self.interval,
                        "last": self.peers[nodename].last,
                    },
                }, level="warning")
        self.peers[nodename].beating = beating
        self.update_status()
        if not beating and self.peers[nodename].last > 0:
            self.forget_peer_data(nodename, change, origin=self.id)

    @staticmethod
    def get_ip_address(ifname):
        ifconfig = utilities.ifconfig.Ifconfig()
        intf = ifconfig.interface(ifname)
        if intf is None:
            raise AttributeError("interface %s not found" % ifname)
        if isinstance(intf.ipaddr, list):
            addr = intf.ipaddr[0]
        else:
            addr = intf.ipaddr
        return addr

    def get_message(self, nodename=None):
        begin, num = self.get_oldest_gen(nodename)
        if num == 0:
            # we're alone for now. don't send a full status payload.
            # sent a presence announce payload instead.
            self.log.debug("ping node %s", nodename if nodename else "*")
            if self.msg_type != 'ping':
                self.msg_type = 'ping'
                self.log.info('change message type to %s', self.msg_type)
            message = self.encrypt({
                "kind": "ping",
                "compat": shared.COMPAT_VERSION,
                "gen": self.get_gen(),
                "monitor": self.get_node_monitor(),
                "updated": time.time(), # for hb and relay readers
            }, encode=False)
            return message, len(message) if message else 0
        if begin == 0 or begin > shared.GEN:
            self.log.debug("send full node data to %s", nodename if nodename else "*")
            if not self.node_data.exists(["monitor"]):
                # no pertinent data to send yet (pre-init)
                self.log.debug("no pertinent data to send yet (pre-init)")
                return None, 0
            if self.msg_type != 'full':
                self.msg_type = 'full'
                self.log.info('change message type to %s', self.msg_type)
            with shared.HB_MSG_LOCK:
                if shared.HB_MSG is not None:
                    return shared.HB_MSG, shared.HB_MSG_LEN
                data = self.node_data.get()
                shared.HB_MSG = self.encrypt(data, encode=False)
                if shared.HB_MSG is None:
                    shared.HB_MSG_LEN = 0
                else:
                    shared.HB_MSG_LEN = len(shared.HB_MSG)
                return shared.HB_MSG, shared.HB_MSG_LEN
        else:
            self.log.debug("send gen %d-%d deltas to %s", begin, shared.GEN, nodename if nodename else "*") # COMMENT
            if self.msg_type != 'patch':
                self.msg_type = 'patch'
                self.log.info('change message type to %s', self.msg_type)
            data = {}
            for gen, delta in shared.GEN_DIFF.items():
                if gen <= begin:
                    continue
                data[gen] = delta
            message = self.encrypt({
                "kind": "patch",
                "deltas": data,
                "gen": self.get_gen(),
                "updated": time.time(), # for hb and relay readers
            }, encode=False)
            return message, len(message) if message else 0

    def queue_rx_data(self, data, nodename):
        shared.RX.put((nodename, data, self.name))
