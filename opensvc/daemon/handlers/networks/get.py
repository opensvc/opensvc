import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the cluster's network backends information.
    """
    routes = (
        ("GET", "networks"),
        (None, "get_networks"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        data = shared.NODE.network_status_data()
        return data

