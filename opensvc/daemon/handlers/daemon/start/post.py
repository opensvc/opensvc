import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Start the daemon thread identified by <thr_id>.
    """
    routes = (
        ("POST", "daemon_start"),
        (None, "daemon_start"),
    )
    prototype = [
        {
            "name": "thr_id",
            "required": True,
            "desc": "The id of a thread to start.",
            "example": "hb#1.tx",
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        with shared.THREADS_LOCK:
            has_thr = options.thr_id in shared.THREADS
        if not has_thr:
            thr.log_request("start thread requested on non-existing thread", nodename, **kwargs)
            return {"error": "thread does not exist"*50, "status": 1}
        thr.log_request("start thread %s" % options.thr_id, nodename, **kwargs)
        with shared.THREADS_LOCK:
            shared.THREADS[options.thr_id].unstop()
        return {"status": 0}

