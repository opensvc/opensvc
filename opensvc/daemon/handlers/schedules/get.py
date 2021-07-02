import datetime

import daemon.handler
import daemon.shared as shared

from env import Env

class Handler(daemon.handler.BaseHandler):
    """
    Return a system's resources usage for the daemon threads and for each service.
    """
    routes = (
        ("GET", "schedules"),
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
        data = []
        options = self.parse_options(kwargs)
        namespaces = thr.get_namespaces()
        with shared.THREADS_LOCK:
            sched_status = shared.THREADS["scheduler"].status()

        # node
        if not options.selector:
            for d in shared.NODE.sched.print_schedule_data(with_next=False):
                _d = {
                    "node": Env.nodename,
                    "path": "",
                    "action": d["action"],
                    "config_parameter": d["config_parameter"],
                    "last_run": d["last_run"],
                    "schedule_definition": d["schedule_definition"],
                }
                task = self.find_delayed("", d["action"], d["config_parameter"], sched_status["delayed"])
                if task:
                    _d["next"] = task["expire"]
                data.append(_d)

        # objects
        paths = thr.object_selector(selector=options.selector or '**', namespace=options.namespace, namespaces=namespaces)
        for path in paths:
            try:
                svc = shared.SERVICES[path]
            except KeyError:
                continue
            for d in svc.sched.print_schedule_data(with_next=False):
                _d = {
                    "node": Env.nodename,
                    "path": path,
                    "action": d["action"],
                    "config_parameter": d["config_parameter"],
                    "last_run": d["last_run"],
                    "schedule_definition": d["schedule_definition"],
                }
                task = self.find_delayed(path, d["action"], d["config_parameter"], sched_status["delayed"])
                if task:
                    _d["next_run"] = task["expire"]
                data.append(_d)

        return {"status": 0, "data": data}

    @staticmethod
    def find_delayed(path, action, param, delayed):
        rid = param.split(".", 1)[0]
        if not "#" in rid:
            rid = ""
        for d in delayed:
            if d["action"] != action:
                continue
            if rid and d["rid"] != rid:
                continue
            if path and d["path"] != path:
                continue
            return d

