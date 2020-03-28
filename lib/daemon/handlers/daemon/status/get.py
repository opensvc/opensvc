import daemon.handler

class Handler(daemon.handler.BaseHandler):
    """
    Return a hash indexed by thead id, containing the status data
    structure of each thread.
    """
    routes = (
        ("GET", "daemon_status"),
        (None, "daemon_status"),
    )
    prototype = [
        {
            "name": "selector",
            "desc": "An object selector expression to filter the dataset with.",
            "required": False,
            "format": "string",
        },
        {
            "name": "namespace",
            "desc": "A namespace name to filter the dataset with.",
            "required": False,
            "format": "string",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    # This handler filters data based on user grants.
    # Don't allow multiplexing to avoid filtering with escalated privs
    multiplex = "never"

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        data = thr.daemon_status()
        namespaces = thr.get_namespaces()
        return thr.filter_daemon_status(
            data,
            namespace=options.namespace,
            namespaces=namespaces,
            selector=options.selector,
        )

