import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the last relay heartbeat payload emitted by <nodename>.
    """
    routes = (
        ("GET", "relay_rx"),
        (None, "relay_rx"),
    )
    prototype = [
        {
            "name": "cluster_id",
            "desc": "The cluster.id keyword value of the emitting node.",
            "required": False,
            "format": "string",
            "default": "",
        },
        {
            "name": "slot",
            "desc": "The name of the node to fetch the last heartbeat message from.",
            "required": True,
            "format": "string",
            "default": "",
        },
    ]
    access = {
        "roles": ["heartbeat"],
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        key = "/".join([options.cluster_id, options.slot])
        with shared.RELAY_LOCK:
            if key not in shared.RELAY_DATA:
                return {"status": 1, "error": "no data"}
            return {
                "status": 0,
                "data": shared.RELAY_DATA[key]["msg"],
                "updated": shared.RELAY_DATA[key]["updated"],
            }

