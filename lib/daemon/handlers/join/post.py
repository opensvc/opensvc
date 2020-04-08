import os

import daemon.clusterlock
import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from env import Env

class Handler(daemon.handler.BaseHandler, daemon.clusterlock.LockMixin):
    """
    Join the cluster.
    """
    routes = (
        ("POST", "join"),
        (None, "join"),
    )
    prototype = []

    def action(self, nodename, thr=None, **kwargs):
        lock_id = self.lock_acquire(Env.nodename, "join", 30, thr=thr)
        if not lock_id:
            raise ex.HTTP(503, "Lock not acquired")
        with shared.JOIN_LOCK:
            data = self.join(nodename, thr=thr, **kwargs)
        self.lock_release("join", lock_id, thr=thr)
        return data

    def join(self, nodename, thr=None, **kwargs):
        if nodename in thr.cluster_nodes:
            new_nodes = thr.cluster_nodes
            thr.log.info("node %s rejoins", nodename)
        else:
            new_nodes = thr.cluster_nodes + [nodename]
            thr.add_cluster_node(nodename)
        result = {
            "status": 0,
            "data": {
                "node": {
                    "data": {
                        "node": {},
                        "cluster": {},
                    },
                },
            },
        }
        config = shared.NODE.private_cd
        node_section = config.get("node", {})
        cluster_section = config.get("cluster", {})
        if "env" in node_section:
            result["data"]["node"]["data"]["node"]["env"] = shared.NODE.env
        if "nodes" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["nodes"] = " ".join(new_nodes)
        if "name" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["name"] = thr.cluster_name
        if "drpnodes" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["drpnodes"] = " ".join(thr.cluster_drpnodes)
        if "id" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["id"] = thr.cluster_id
        if "quorum" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["quorum"] = thr.quorum
        if "dns" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["dns"] = " ".join(shared.NODE.dns)
        for section in config:
            if section.startswith("hb#") or \
               section.startswith("stonith#") or \
               section.startswith("pool#") or \
               section.startswith("network#") or \
               section.startswith("arbitrator#"):
                result["data"]["node"]["data"][section] = config[section]
        from core.objects.ccfg import Ccfg
        svc = Ccfg(volatile=True, node=shared.NODE)
        if svc.exists():
            result["data"]["cluster"] = {
                "data": svc.print_config_data(),
                "mtime": os.stat(svc.paths.cf).st_mtime,
            }
        return result

