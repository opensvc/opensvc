import time

import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.naming import split_path

class Handler(daemon.handler.BaseHandler):
    """
    Freeze the node and shutdown all running object instances.
    Return only when done.
    """
    routes = (
        ("POST", "node_drain"),
        (None, "node_drain"),
    )
    prototype = [
        {
            "name": "wait",
            "desc": "Don't return until the node is drained.",
            "default": False,
            "required": False,
            "format": "boolean",
        },
        {
            "name": "time",
            "desc": "The maximum wait time. If not specified, no timeout is set.",
            "required": False,
            "format": "duration",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        thr.log_request("drain node", nodename, **kwargs)
        thr.event("node_freeze", data={"reason": "drain"})
        thr.freezer.node_freeze()

        if thr.stopped() or shared.NMON_DATA.status in ("draining", "shutting"):
            thr.log.info("already %s", shared.NMON_DATA.status)
            # wait for service shutdown to finish before releasing the dup client
            if options.wait:
                while True:
                    if shared.THREADS["monitor"]._shutdown or shared.NMON_DATA.status not in ("draining", "shutting"):
                        break
                    time.sleep(0.3)
            return {"status": 0}
        try:
            thr.set_nmon("draining")
            for path in shared.SMON_DATA:
                _, _, kind = split_path(path)
                if kind not in ("svc", "vol"):
                    continue
                thr.set_smon(path, local_expect="shutdown")
            if options.wait:
                try:
                    self.wait_shutdown(timeout=options.time)
                except ex.TimeOut:
                    return {"status": 1, "error": "timeout"}
        except Exception as exc:
            thr.log.exception(exc)

        return {"status": 0}

    def wait_shutdown(self, timeout=None):
        def still_shutting():
            for smon in shared.SMON_DATA.values():
                if smon.local_expect == "shutdown":
                    return True
            return False
        while still_shutting():
            if timeout is not None and timeout <= 0:
                raise ex.TimeOut
            timeout -= 1
            time.sleep(1)

