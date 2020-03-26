import os

import daemon.handlers.handler as handler
import daemon.shared as shared
import core.exceptions as ex
from rcUtilities import split_path

class Handler(handler.Handler):
    """
    Return the <path> object logs back to <backlog> bytes.
    """
    routes = (
        ("GET", "object_backlogs"),
        (None, "object_backlogs"),
        (None, "service_backlogs"),
    )
    prototype = [
        {
            "name": "path",
            "required": True,
            "format": "object_path",
            "desc": "The object path.",
        },
        {
            "name": "backlog",
            "required": False,
            "format": "size",
            "default": "10k",
            "desc": "The per-instance backlog size.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "FROM:path",
    }

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        options = self.parse_options(kwargs)
        svc = thr.get_service(options.path)
        if svc is None:
            raise ex.HTTP(404, "%s not found" % options.path)
        logfile = os.path.join(svc.log_d, svc.name+".log")
        ofile = thr._action_logs_open(logfile, options.backlog, svc.path)
        return thr.read_file_lines(ofile)

