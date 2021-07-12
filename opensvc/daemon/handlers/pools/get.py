import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the cluster's storage pools information.
    """
    routes = (
        ("GET", "pools"),
        (None, "get_pools"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        mon_status = thr.daemon_status_data.get(["monitor"])
        data = shared.NODE.pool_status_data(mon_status=mon_status)
        return data

