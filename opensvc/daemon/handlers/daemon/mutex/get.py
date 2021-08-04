import daemon.handler
from daemon.shared import daemon_mutex_status


class Handler(daemon.handler.BaseHandler):
    """
    Return daemon mutex status
    """
    routes = (
        ("GET", "daemon_mutex"),
        (None, "daemon_mutex"),
    )
    prototype = []

    def action(self, nodename, thr=None, **kwargs):
        return {
            "data": {"mutexes": daemon_mutex_status(thr.log)},
            "status": 0,
        }
