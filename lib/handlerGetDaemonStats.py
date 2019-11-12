import time

import handler
import osvcd_shared as shared

class Handler(handler.Handler):
    """
    Return a hash indexed by thead id, containing the status data
    structure of each thread.
    """
    routes = (
        ("GET", "daemon_stats"),
        (None, "daemon_stats"),
    )
    prototype = []
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, **kwargs):
        data = {
            "timestamp": time.time(),
            "daemon": shared.DAEMON.stats(),
            "node": {
                "cpu": {
                    "time": shared.NODE.cpu_time(),
                 },
            },
            "services": {},
        }
        with shared.THREADS_LOCK:
            for dthr_id, dthr in shared.THREADS.items():
                data[dthr_id] = dthr.thread_stats()
        with shared.SERVICES_LOCK:
            for svc in shared.SERVICES.values():
                _data = svc.pg_stats()
                if _data:
                    data["services"][svc.path] = _data
        return {"status": 0, "data": data}

