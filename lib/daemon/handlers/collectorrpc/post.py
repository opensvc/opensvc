import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Enqueue a remote procedure call to the collector, usually a information push.
    """
    routes = (
        ("POST", "collector_xmlrpc"),
        (None, "collector_xmlrpc"),
    )
    prototype = [
        {
            "name": "args",
            "default": [],
            "required": False,
            "desc": "The arguments list passed to the collector xmlrpc wrapper.",
        },
        {
            "name": "kwargs",
            "default": {},
            "required": False,
            "desc": "The keyword arguments dict passed to the collector xmlrpc wrapper.",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        shared.COLLECTOR_XMLRPC_QUEUE.insert(0, (options.args, options.kwargs))
        result = {
            "status": 0,
            "info": ["collector rpc queued"],
        }
        return result

