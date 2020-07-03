import daemon.clusterlock
import daemon.handler

class Handler(daemon.handler.BaseHandler, daemon.clusterlock.LockMixin):
    """
    Release a clusterwide lock identified by <name> and the <lock_id>
    returned by the lock handler call.
    """
    routes = (
        ("POST", "unlock"),
        (None, "unlock"),
    )
    prototype = [
        {
            "name": "name",
            "desc": "The lock name.",
            "required": True,
            "format": "string",
        },
        {
            "name": "lock_id",
            "desc": "The lock id returned by the lock handler call.",
            "required": True,
            "format": "string",
        },
        {
            "name": "timeout",
            "desc": "The maximum time to wait for lock release before returning an error.",
            "required": False,
            "format": "duration",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        self.lock_release(options.name, options.lock_id, timeout=options.timeout, thr=thr)
        result = {"status": 0}
        return result

