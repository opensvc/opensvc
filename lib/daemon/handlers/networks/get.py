import daemon.handlers.handler as handler
import daemon.shared as shared

class Handler(handler.Handler):
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

