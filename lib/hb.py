"""
 Heartbeat parent class
"""
import logging
import time
import socket
import datetime
import fcntl
import struct

import json_delta
import osvcd_shared as shared
from rcGlobalEnv import rcEnv, Storage

class Hb(shared.OsvcThread):
    """
    Heartbeat parent class
    """
    default_hb_period = 5

    def __init__(self, name, role=None):
        shared.OsvcThread.__init__(self)
        self.name = name
        self.id = name + "." + role
        self.log = logging.getLogger(rcEnv.nodename+".osvcd."+self.id)
        self.peers = {}

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data.peers = {}
        for nodename in self.cluster_nodes:
            if nodename == rcEnv.nodename:
                data.peers[nodename] = {}
                continue
            if "*" in self.peers:
                _data = self.peers["*"]
            else:
                _data = self.peers.get(nodename, Storage({
                    "last": 0,
                    "beating": False,
                    "success": True,
                }))
            data.peers[nodename] = {
                "last": datetime.datetime.utcfromtimestamp(_data.last)\
                                         .strftime('%Y-%m-%dT%H:%M:%SZ'),
                "beating": _data.beating,
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
                self.log.info("node %s hb status stale => beating", nodename)
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
        for nodename in self.peers:
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
                self.log.info("node %s hb status stale => beating", nodename)
            else:
                self.log.info("node %s hb status beating => stale", nodename)
        self.peers[nodename].beating = beating
        if not beating:
            self.forget_peer_data(nodename, change)

    @staticmethod
    def get_ip_address(ifname):
        try:
            ifname = bytes(ifname, "utf-8")
        except TypeError:
            ifname = str(ifname)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        info = fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )
        return socket.inet_ntoa(info[20:24])

    def get_message(self, nodename=None):
        begin, num = self.get_oldest_gen(nodename)
        if num == 0:
            # we're alone for now. don't send a full status payload.
            # sent a presence announce payload instead.
            self.log.debug("ping node %s", nodename if nodename else "*")
            gen = {rcEnv.nodename: shared.GEN}
            gen.update(shared.REMOTE_GEN)
            message = self.encrypt({
                "kind": "ping",
                "compat": shared.COMPAT_VERSION,
                "gen": gen,
                "monitor": self.get_node_monitor(),
                "updated": time.time(), # for hb and relay readers
            }, encode=False)
            return message, len(message)
        if begin == 0 or begin > shared.GEN:
            self.log.debug("send full node data to %s", nodename if nodename else "*")
            try:
                shared.CLUSTER_DATA[rcEnv.nodename]["monitor"]["status"]
            except KeyError:
                # no pertinent data to send yet (pre-init)
                self.log.debug("no pertinent data to send yet (pre-init)")
                return None, 0
            if shared.HB_MSG is not None:
                return shared.HB_MSG, shared.HB_MSG_LEN
            with shared.HB_MSG_LOCK:
                shared.HB_MSG = self.encrypt(shared.CLUSTER_DATA[rcEnv.nodename], encode=False)
                if shared.HB_MSG is None:
                    shared.HB_MSG_LEN = 0
                else:
                    shared.HB_MSG_LEN = len(shared.HB_MSG)
                return shared.HB_MSG, shared.HB_MSG_LEN
        else:
            #self.log.info("send gen %d-%d deltas to %s", begin, shared.GEN, nodename if nodename else "*")
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
            return message, len(message)

    def store_rx_data(self, data, nodename):
        if data is None:
            self.log.info("drop corrupted hb data from %s", nodename)
        current_gen = shared.REMOTE_GEN.get(nodename, 0)
        kind = data.get("kind", "full")
        if kind == "patch":
            deltas = data.get("deltas", [])
            gens = sorted([int(gen) for gen in deltas if int(gen) > current_gen])
            if len(gens) == 0:
                #self.log.info("no more recent gen in received deltas")
                return
            if nodename not in shared.CLUSTER_DATA:
                # happens during init. drop the patch, a full will follow
                return
            with shared.CLUSTER_DATA_LOCK:
                for gen in gens:
                    #self.log.debug("merge node %s gen %d (%d diffs)", nodename, gen, len(deltas[str(gen)]))
                    try:
                        json_delta.patch(shared.CLUSTER_DATA[nodename], deltas[str(gen)])
                        shared.EVENT_Q.put({
                            "nodename": nodename,
                            "kind": "patch",
                            "data": deltas[str(gen)],
                        })
                    except Exception as exc:
                        self.log.warning("failed to apply node %s dataset gen %d patch: %s. "
                                         "ask for a full: %s", nodename, gen, deltas[str(gen)], exc)
                        shared.REMOTE_GEN[nodename] = 0
                        shared.LOCAL_GEN[nodename] = data.get("gen", {}).get(rcEnv.nodename, 0)
                        return
                shared.REMOTE_GEN[nodename] = gen
                shared.LOCAL_GEN[nodename] = data.get("gen", {}).get(rcEnv.nodename, 0)
                self.log.debug("patch node %s dataset to gen %d, peer has gen %d of our dataset",
                              nodename, shared.REMOTE_GEN[nodename],
                              shared.LOCAL_GEN[nodename])
        elif kind == "ping":
            with shared.CLUSTER_DATA_LOCK:
                shared.REMOTE_GEN[nodename] = 0
                shared.LOCAL_GEN[nodename] = data.get("gen", {}).get(rcEnv.nodename, 0)
                if nodename not in shared.CLUSTER_DATA:
                    shared.CLUSTER_DATA[nodename] = {}
                shared.CLUSTER_DATA[nodename]["monitor"] = data["monitor"]
                self.log.debug("reset node %s dataset gen, peer has gen %d of our dataset",
                              nodename, shared.LOCAL_GEN[nodename])
        else:
            data_gen = data.get("gen", {}).get(nodename)
            if data_gen is not None and nodename in shared.LOCAL_GEN and data_gen == shared.LOCAL_GEN[nodename]:
                # already installed
                self.log.debug("already installed %d", data_gen)
                return
            node_status = data.get("monitor", {}).get("status")
            if node_status in ("init", "maintenance", "upgrade") and nodename in shared.CLUSTER_DATA:
                self.duplog("info", "reconduct last known instances status from "
                            "node %(nodename)s in %(node_status)s state",
                            nodename=nodename, node_status=node_status)
                data["services"]["status"] = shared.CLUSTER_DATA[nodename].get("services", {}).get("status", {})
            with shared.CLUSTER_DATA_LOCK:
                shared.CLUSTER_DATA[nodename] = data
                new_gen= data.get("gen", {}).get(nodename, 0)
                shared.LOCAL_GEN[nodename] = data.get("gen", {}).get(rcEnv.nodename, 0)
                if new_gen == shared.REMOTE_GEN.get(nodename):
                    return
                shared.REMOTE_GEN[nodename] = new_gen
                self.log.debug("install node %s dataset gen %d, peer has gen %d of our dataset",
                              nodename, shared.REMOTE_GEN[nodename],
                              shared.LOCAL_GEN[nodename])
        shared.wake_monitor("node %s %s dataset gen %d received through %s" % (nodename, kind, shared.REMOTE_GEN[nodename], self.name))

