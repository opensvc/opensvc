import daemon.clusterlock
import daemon.handler


class Handler(daemon.handler.BaseHandler, daemon.clusterlock.LockMixin):
    """
    Return cluster held locks
    """
    routes = (
        ("GET", "cluster/locks"),
    )
    prototype = []

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        locks = self.locks()
        return {
            "data": locks,
            "status": 0,
        }
