import daemon.handler

class Handler(daemon.handler.BaseHandler):
    """
    Return a hash indexed by nodename, containing the info
    required by the node selector algorithm.
    """
    routes = (
        ("GET", "nodes_info"),
        (None, "nodes_info"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        return {"status": 0, "data": thr.nodes_info()}

