import time

import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return a system's resources usage for the daemon threads and for each service.
    """
    routes = (
        ("GET", "daemon_stats"),
        (None, "daemon_stats"),
    )
    prototype = [
        {
            "name": "selector",
            "desc": "An object selector expression to filter the dataset with.",
            "required": False,
            "format": "string",
        },
        {
            "name": "namespace",
            "desc": "A namespace name to filter the dataset with.",
            "required": False,
            "format": "string",
        },
    ]
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
        options = self.parse_options(kwargs)
        namespaces = thr.get_namespaces()
        paths = thr.object_selector(selector=options.selector or '**', namespace=options.namespace, namespaces=namespaces)
        with shared.THREADS_LOCK:
            for dthr_id, dthr in shared.THREADS.items():
                data[dthr_id] = dthr.thread_stats()
        for path in paths:
            try:
                svc = shared.SERVICES[path]
            except KeyError:
                continue
            _data = svc.pg_stats()
            if _data:
                data["services"][path] = _data
        return {"status": 0, "data": data}

