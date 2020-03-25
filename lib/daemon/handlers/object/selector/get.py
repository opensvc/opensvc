import daemon.handlers.handler as handler

class Handler(handler.Handler):
    """
    Return the object list expanded from the <selector> expression.
    """
    routes = (
        ("GET", "object_selector"),
        (None, "object_selector"),
    )
    prototype = [
        {
            "name": "selector",
            "required": False,
            "format": "string",
            "desc": "An object selector expression.",
        },
        {
            "name": "namespace",
            "required": False,
            "format": "string",
            "desc": "A namespace to limit the expansion to.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        namespaces = thr.get_namespaces()
        return thr.object_selector(options.selector, options.namespace, namespaces)

