import daemon.clusterlock
import daemon.handler

class Handler(daemon.handler.BaseHandler, daemon.clusterlock.LockMixin):
    """
    Acquire a clusterwide lock identified by <name>.
    Other lockers for the same <name> will wait for <timeout> until release.
    """
    routes = (
        ("POST", "lock"),
        (None, "lock"),
    )
    prototype = [
        {
            "name": "name",
            "desc": "The lock name.",
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
        lock_id = self.lock_acquire(nodename, options.name, options.timeout, thr=thr)
        if lock_id:
            result = {
                "data": {
                    "id": lock_id,
                },
                "status": 0,
            }
        else:
            result = {"status": 1}
        return result


