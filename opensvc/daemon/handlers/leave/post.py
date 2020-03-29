import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Leave the cluster.
    """
    routes = (
        ("POST", "leave"),
        (None, "leave"),
    )
    prototype = []

    def action(self, nodename, thr=None, **kwargs):
        with shared.JOIN_LOCK:
            return self.leave(nodename, thr=thr, **kwargs)

    def leave(self, nodename, thr=None, **kwargs):
        if nodename not in thr.cluster_nodes:
            thr.log.info("node %s already left", nodename)
            return {"status": 0}
        thr.log.info("node %s is leaving", nodename)
        try:
            thr.remove_cluster_node(nodename)
            return {"status": 0}
        except Exception as exc:
            return {
                "status": 1,
                "error": [str(exc)],
            }
