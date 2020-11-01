import daemon.handler
import daemon.shared as shared

from env import Env

class Handler(daemon.handler.BaseHandler):
    """
    Load an instance status data.
    """
    routes = (
        ("POST", "object_status"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "data",
            "desc": "The instance status dataset.",
            "required": True,
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        with thr.daemon_status_data.lock:
            options.data["monitor"] = thr.get_service_monitor(options.path)
            thr.daemon_status_data._set_lk(["monitor", "nodes", Env.nodename, "services", "status", options.path], options.data)
        shared.wake_monitor("%s status change" % options.path, immediate=True)
