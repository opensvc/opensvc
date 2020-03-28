import os

import daemon.handler
import core.exceptions as ex
from utilities.string import bdecode

try:
    from foreign.hyper.common.headers import HTTPHeaderMap
except Exception:
    HTTPHeaderMap = dict

class Handler(daemon.handler.BaseHandler):
    """
    Feed the <path> object logs.
    """
    routes = (
        ("GET", "object_logs"),
        (None, "object_logs"),
        (None, "service_logs"),
    )
    prototype = [
        {
            "name": "path",
            "required": True,
            "format": "object_path",
            "desc": "The object path.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "FROM:path",
    }
    stream = True

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        options = self.parse_options(kwargs)
        svc = thr.get_service(options.path)
        if svc is None:
            raise ex.HTTP(404, "%s not found" % options.path)
        request_headers = HTTPHeaderMap(thr.streams[stream_id]["request"].headers)
        try:
            content_type = bdecode(request_headers.get("accept").pop())
        except:
            content_type = "application/json"
        thr.streams[stream_id]["content_type"] = content_type
        logfile = os.path.join(svc.log_d, svc.name+".log")
        ofile = thr._action_logs_open(logfile, 0, svc.path)
        thr.streams[stream_id]["pushers"].append({
            "o": self,
            "fn": "h2_push_logs",
            "args": [ofile, True],
        })

