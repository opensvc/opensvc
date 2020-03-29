import time

import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Store a relay heartbeat payload emitted by <nodename>.
    """
    routes = (
        ("POST", "relay_tx"),
        (None, "relay_tx"),
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
            "name": "cluster_name",
            "desc": "The cluster.name keyword value of the emitting node.",
            "required": False,
            "format": "string",
            "default": "",
        },
        {
            "name": "msg",
            "desc": "The heartbeat message payload.",
            "required": False,
            "format": "string",
            "default": None,
        },
        {
            "name": "addr",
            "desc": "The sender [ipaddr, port] tuple.",
            "required": False,
            "format": "list",
            "default": [""],
        },
    ]
    access = {
        "roles": ["heartbeat"],
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        key = "/".join([options.cluster_id, nodename])
        with shared.RELAY_LOCK:
            shared.RELAY_DATA[key] = {
                "msg": options.msg,
                "updated": time.time(),
                "cluster_name": options.cluster_name,
                "cluster_id": options.cluster_id,
                "ipaddr": options.addr[0],
            }
        return {"status": 0}

