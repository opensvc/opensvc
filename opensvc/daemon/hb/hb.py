"""
 Heartbeat parent class
"""
import logging
import time

import foreign.json_delta as json_delta
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
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd."+self.id), {"node": Env.nodename, "component": self.id})
        self.peers = {}
        self.reset_stats()
        self.hb_nodes = self.cluster_nodes

    def get_hb_nodes(self):
        try:
            self.hb_nodes = [node for node in shared.NODE.conf_get(self.name, "nodes")
                             if node in self.cluster_nodes]
        except ex.OptNotFound as exc:
            self.hb_nodes = self.cluster_nodes

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
        if not beating and self.peers[nodename].last > 0:
            self.forget_peer_data(nodename, change)

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
            try:
                shared.CLUSTER_DATA[Env.nodename]["monitor"]["status"]
            except KeyError:
                # no pertinent data to send yet (pre-init)
                self.log.debug("no pertinent data to send yet (pre-init)")
                return None, 0
            if shared.HB_MSG is not None:
                return shared.HB_MSG, shared.HB_MSG_LEN
            with shared.HB_MSG_LOCK:
                with shared.CLUSTER_DATA_LOCK:
                    shared.HB_MSG = self.encrypt(shared.CLUSTER_DATA[Env.nodename], encode=False)
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
            return message, len(message) if message else 0

    def store_rx_data(self, data, nodename):
        if data is None:
            self.log.info("drop corrupted hb data from %s", nodename)
        with shared.RX_LOCK:
            self._store_rx_data(data, nodename)

    def _store_rx_data(self, data, nodename):
        current_gen = shared.REMOTE_GEN.get(nodename, 0)
        our_gen_on_peer = data.get("gen", {}).get(Env.nodename, 0)
        kind = data.get("kind", "full")
        change = False
        if kind == "patch":
            if current_gen == 0:
                # waiting for a full: ignore patches
                return
            if nodename not in shared.CLUSTER_DATA:
                # happens during init. ignore the patch, and ask for a full
                shared.REMOTE_GEN[nodename] = 0
                shared.LOCAL_GEN[nodename] = our_gen_on_peer
                return
            deltas = data.get("deltas", [])
            gens = sorted([int(gen) for gen in deltas])
            gens = [gen for gen in gens if gen > current_gen]
            if len(gens) == 0:
                #self.log.info("no more recent gen in received deltas")
                if our_gen_on_peer > shared.LOCAL_GEN[nodename]:
                    shared.LOCAL_GEN[nodename] = our_gen_on_peer
                    shared.CLUSTER_DATA[nodename]["gen"][Env.nodename] = our_gen_on_peer
                return
            with shared.CLUSTER_DATA_LOCK:
                for gen in gens:
                    #self.log.debug("merge node %s gen %d (%d diffs)", nodename, gen, len(deltas[str(gen)]))
                    if gen - 1 != current_gen:
                        self.log.warning("unsynchronized node %s dataset. local gen %d, received %d. "
                                         "ask for a full.", nodename, current_gen, gen)
                        shared.REMOTE_GEN[nodename] = 0
                        shared.LOCAL_GEN[nodename] = our_gen_on_peer
                        shared.CLUSTER_DATA[nodename]["gen"] = {
                            nodename: gen,
                            Env.nodename: our_gen_on_peer,
                        }
                        break
                    try:
                        json_delta.patch(shared.CLUSTER_DATA[nodename], deltas[str(gen)])
                        current_gen = gen
                        shared.REMOTE_GEN[nodename] = gen
                        shared.LOCAL_GEN[nodename] = our_gen_on_peer
                        shared.CLUSTER_DATA[nodename]["gen"] = {
                            nodename: gen,
                            Env.nodename: our_gen_on_peer,
                        }
                        self.log.debug("patch node %s dataset to gen %d, peer has gen %d of our dataset",
                                       nodename, shared.REMOTE_GEN[nodename],
                                       shared.LOCAL_GEN[nodename])
                        if self.patch_has_nodes_info_change(deltas[str(gen)]):
                            self.on_nodes_info_change()
                        change = True
                    except Exception as exc:
                        self.log.warning("failed to apply node %s dataset gen %d patch: %s. "
                                         "ask for a full: %s", nodename, gen, deltas[str(gen)], exc)
                        shared.REMOTE_GEN[nodename] = 0
                        shared.LOCAL_GEN[nodename] = our_gen_on_peer
                        shared.CLUSTER_DATA[nodename]["gen"] = {
                            nodename: gen,
                            Env.nodename: our_gen_on_peer,
                        }
                        return
        elif kind == "ping":
            with shared.CLUSTER_DATA_LOCK:
                shared.REMOTE_GEN[nodename] = 0
                shared.LOCAL_GEN[nodename] = our_gen_on_peer
                if nodename not in shared.CLUSTER_DATA:
                    shared.CLUSTER_DATA[nodename] = {}
                shared.CLUSTER_DATA[nodename]["gen"] = {
                    nodename: 0,
                    Env.nodename: our_gen_on_peer,
                }
                shared.CLUSTER_DATA[nodename]["monitor"] = data["monitor"]
                self.log.debug("reset node %s dataset gen, peer has gen %d of our dataset",
                              nodename, shared.LOCAL_GEN[nodename])
                change = True
        else:
            data_gen = data.get("gen", {}).get(nodename)
            if data_gen is None:
                self.log.debug("no 'gen' in full dataset from %s: drop", nodename)
                return
            last_gen = shared.REMOTE_GEN.get(nodename)
            if last_gen is not None and last_gen >= data_gen:
                self.log.debug("already installed or beyond %s gen %d dataset: drop", nodename, data_gen)
                return
            node_status = data.get("monitor", {}).get("status")
            if node_status in ("init", "maintenance", "upgrade") and nodename in shared.CLUSTER_DATA:
                for path, idata in shared.CLUSTER_DATA[nodename].get("services", {}).get("status", {}).items():
                    if path in data["services"]["status"]:
                        continue
                    idata["preserved"] = True
                    data["services"]["status"][path] = idata
            with shared.CLUSTER_DATA_LOCK:
                shared.CLUSTER_DATA[nodename] = data
                new_gen = data.get("gen", {}).get(nodename, 0)
                shared.LOCAL_GEN[nodename] = our_gen_on_peer
                self.on_nodes_info_change()
                shared.REMOTE_GEN[nodename] = new_gen
                shared.CLUSTER_DATA[nodename]["gen"] = {
                    nodename: new_gen,
                    Env.nodename: our_gen_on_peer,
                }
                self.log.debug("install node %s dataset gen %d, peer has gen %d of our dataset",
                              nodename, shared.REMOTE_GEN[nodename],
                              shared.LOCAL_GEN[nodename])
                change = True
        if change:
            shared.wake_monitor("node %s %s dataset gen %d received through %s" % (nodename, kind, shared.REMOTE_GEN[nodename], self.name))

