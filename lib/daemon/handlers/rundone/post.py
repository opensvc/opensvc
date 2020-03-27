import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Signal the daemon a scheduler task is done.
    """
    routes = (
        ("POST", "run_done"),
        (None, "run_done"),
    )
    prototype = [
        {
            "name": "action",
            "desc": "The action executed by the scheduler task",
            "required": True,
            "format": "string",
        },
        {
            "name": "path",
            "desc": "An object selector expression.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "rids",
            "desc": "An object selector expression.",
            "required": False,
            "format": "list",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if options.rids is None:
            rids = options.rid
        else:
            rids = ",".join(sorted(options.rids))
        if not options.action:
            return {"status": 0}
        sig = (options.action, options.path, rids)
        with shared.RUN_DONE_LOCK:
            shared.RUN_DONE.add(sig)
        return {"status": 0}

