import time

from foreign.six.moves import queue
import daemon.handler
from env import Env
from utilities.string import bdecode

try:
    from foreign.hyper.common.headers import HTTPHeaderMap
except Exception:
    HTTPHeaderMap = dict

class Handler(daemon.handler.BaseHandler):
    """
    Subscribe to the daemon events stream.
    """
    routes = (
        ("GET", "events"),
        (None, "events"),
    )
    prototype = [
        {
            "name": "selector",
            "required": False,
            "format": "string",
            "desc": "An object selector to filter the events.",
        },
        {
            "name": "full",
            "required": False,
            "default": False,
            "format": "boolean",
            "desc": "Send a first message containing a the full cluster data the following patch events apply to.",
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }
    stream = True

    # This handler filters data based on user grants.
    # Don't allow multiplexing to avoid filtering with escalated privs
    multiplex = "never"

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        options = self.parse_options(kwargs)
        thr.selector = options.selector
        if not thr.event_queue:
            thr.event_queue = queue.Queue()
        if options.full:
            data = thr.daemon_status()
            namespaces = thr.get_namespaces()
            fevent = {
                "nodename": Env.nodename,
                "ts": time.time(),
                "kind": "full",
                "data": thr.filter_daemon_status(
                    data,
                    namespaces=namespaces,
                    selector=options.selector
                ),
            }
            if thr.h2conn:
                _msg = fevent
            elif thr.encrypted:
                _msg = thr.encrypt(fevent)
            else:
                _msg = thr.msg_encode(fevent)
            thr.event_queue.put(_msg)
        if not thr in thr.parent.events_clients:
            thr.parent.events_clients.append(thr)
        if not stream_id in thr.events_stream_ids:
            thr.events_stream_ids.append(stream_id)
        if thr.h2conn:
            request_headers = HTTPHeaderMap(thr.streams[stream_id]["request"].headers)
            try:
                content_type = bdecode(request_headers.get("accept").pop())
            except:
                content_type = "application/json"
            thr.streams[stream_id]["content_type"] = content_type
            thr.streams[stream_id]["pushers"].append({
                "fn": "h2_push_action_events",
            })
        else:
            thr.raw_push_action_events()


