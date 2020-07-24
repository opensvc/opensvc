import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the relay's list of clients, with their last update time, payload size, ip address and cluster id/name.
    """
    routes = (
        ("GET", "relay_status"),
        ("GET", "daemon_relay_status"),
        (None, "daemon_relay_status"),
    )
    prototype = []
    access = {
        "roles": ["heartbeat"],
    }

    def action(self, nodename, thr=None, **kwargs):
        data = {}
        with shared.RELAY_LOCK:
            for _nodename, _data in shared.RELAY_DATA.items():
                data[_nodename] = {
                    "cluster_name": _data.get("cluster_name", ""),
                    "updated": _data.get("updated", 0),
                    "ipaddr": _data.get("ipaddr", ""),
                    "size": len(_data.get("msg", "")),
                }
        return data
