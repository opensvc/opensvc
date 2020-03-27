import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
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

