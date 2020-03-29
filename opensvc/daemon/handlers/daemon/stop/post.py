import time

import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Stop the agent daemon, leaving objects in their current state.
    If a thr_id is specified, stop only this daemon thread.
    A daemon stop leaves the services instances in their current state.
    The daemon announces a maintenance period to its peers before going offline, so the peers won't takeover services until the maintenance grace period expires.
    """
    routes = (
        ("POST", "daemon_stop"),
        (None, "daemon_stop"),
    )
    prototype = [
        {
            "name": "thr_id",
            "required": False,
            "desc": "The id of a thread to stop. The special value 'tx' causes all tx threads to stop.",
            "example": "hb#1.tx",
            "format": "string",
        },
    ]

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        if not options.thr_id:
            thr.log_request("stop daemon", nodename, **kwargs)
            if options.get("upgrade"):
                thr.set_nmon(status="upgrade")
                thr.log.info("announce upgrade state")
            else:
                thr.set_nmon(status="maintenance")
                thr.log.info("announce maintenance state")
            time.sleep(5)
            shared.DAEMON_STOP.set()
            return {"status": 0}
        elif options.thr_id == "tx":
            thr_ids = [thr_id for thr_id in shared.THREADS.keys() if thr_id.endswith("tx")]
        else:
            thr_ids = [options.thr_id]
        for thr_id in thr_ids:
            with shared.THREADS_LOCK:
                has_thr = thr_id in shared.THREADS
            if not has_thr:
                thr.log_request("stop thread requested on non-existing thread", nodename, **kwargs)
                return {"error": "thread does not exist"*50, "status": 1}
            thr.log_request("stop thread %s" % thr_id, nodename, **kwargs)
            with shared.THREADS_LOCK:
                shared.THREADS[thr_id].stop()
            if thr_id == "scheduler":
                shared.wake_scheduler()
            elif thr_id == "monitor":
                shared.wake_monitor("shutdown")
            elif thr_id.endswith("tx"):
                shared.wake_heartbeat_tx()
            if options.get("wait", False):
                with shared.THREADS_LOCK:
                    shared.THREADS[thr_id].join()
        return {"status": 0}

