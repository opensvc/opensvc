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
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        self.lock_release(options.name, options.lock_id, thr=thr)
        result = {"status": 0}
        return result

