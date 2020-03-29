import os

import daemon.handler
from env import Env
from utilities.string import bdecode

try:
    from foreign.hyper.common.headers import HTTPHeaderMap
except Exception:
    HTTPHeaderMap = dict

class Handler(daemon.handler.BaseHandler):
    """
    Feed node logs.
    """
    routes = (
        ("GET", "node_logs"),
        (None, "node_logs"),
    )
    prototype = []
    stream = True

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        logfile = os.path.join(Env.paths.pathlog, "node.log")
        ofile = thr._action_logs_open(logfile, 0, "node")
        request_headers = HTTPHeaderMap(thr.streams[stream_id]["request"].headers)
        try:
            content_type = bdecode(request_headers.get("accept").pop())
        except:
            content_type = "application/json"
        thr.streams[stream_id]["content_type"] = content_type
        thr.streams[stream_id]["pushers"].append({
            "o": self,
            "fn": "h2_push_logs",
            "args": [ofile, True],
        })

