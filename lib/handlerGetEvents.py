from six.moves import queue
import handler
import osvcd_shared as shared
from rcUtilities import bdecode

try:
    from hyper.common.headers import HTTPHeaderMap
except Exception:
    HTTPHeaderMap = dict

class Handler(handler.Handler):
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
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        options = self.parse_options(kwargs)
        thr.selector = options.selector
        if not thr.event_queue:
            thr.event_queue = queue.Queue()
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


