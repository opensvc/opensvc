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
        data = shared.NODE.pool_status_data()
        return data

