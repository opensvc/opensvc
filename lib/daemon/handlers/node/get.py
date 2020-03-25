import daemon.handlers.handler as handler
import daemon.shared as shared

class Handler(handler.Handler):
    """
    Return the node information.
    """
    routes = (
        ("GET", "node"),
        ("GET", "get_node"),
        (None, "get_node"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        data = shared.NODE.asset.get_asset_dict()
        return data

