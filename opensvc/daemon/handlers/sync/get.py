import time

from foreign.six.moves import queue
import daemon.handler
import daemon.shared as shared


class Handler(daemon.handler.BaseHandler):
    """
    Wait for the current data generation number to reach all live nodes.
    """
    routes = (
        ("GET", "sync"),
    )
    prototype = [
        {
            "name": "timeout",
            "desc": "Time to wait for the current local dataset generation number to reach all nodes. Return a status 1 response if the timeout is exceeded.",
            "default": "60s",
            "format": "duration",
        }
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        options = self.parse_options(kwargs)
        thr.selector = ""
        ref_gen = shared.GEN
        if not thr.event_queue:
            thr.event_queue = queue.Queue()
        if not thr in thr.parent.events_clients:
            thr.parent.events_clients.append(thr)
        if self.match(ref_gen):
            return {"status": 0, "data": {"satisfied": True, "gen": ref_gen}}
        timeout = time.time() + options.timeout
        end = False
        while True:
            left = timeout - time.time()
            if left < 0:
                left = 0
            try:
                thr.event_queue.get(True, left if left < 3 else 3)
            except queue.Empty:
                if left < 3:
                    end = True
            if self.match(ref_gen):
                return {"status": 0, "data": {"satisfied": True, "gen": ref_gen}}
            if end:
                return {"status": 1, "data": {"satisfied": False, "gen": ref_gen}}

    def match(self, ref_gen):
        for node, gen in shared.LOCAL_GEN.items():
            if gen < ref_gen:
                return False
        return True

