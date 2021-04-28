import time

import daemon.handler
import daemon.shared as shared
from utilities.naming import split_path

class Handler(daemon.handler.BaseHandler):
    """
    Shutdown the agent daemon and return only when done.

    Shutting the daemon shuts down all services instances running on the node.
    Use the POST /daemon_stop handler instead to preserve the services instances.

    Beware, once shutdown, you won't be able to start the daemon from the api.
    This handler is meant to be used by the node shutdown sequence only.
    """
    routes = (
        ("POST", "daemon_shutdown"),
        (None, "daemon_shutdown"),
    )
    prototype = []

    def action(self, nodename, thr=None, **kwargs):
        """
        Care with locks
        """
        thr.log_request("shutdown daemon", nodename, **kwargs)
        with shared.THREADS_LOCK:
            shared.THREADS["scheduler"].stop()
            mon = shared.THREADS["monitor"]
        nmon = thr.get_node_monitor()
        if thr.stopped() or nmon.status == "shutting":
            thr.log.info("already shutting")
            # wait for service shutdown to finish before releasing the dup client
            while True:
                if mon._shutdown:
                    break
                time.sleep(0.3)
            return {"status": 0}
        try:
            thr.set_nmon("shutting")
            mon.kill_procs()
            for path in thr.node_data.get(["services", "status"]):
                _, _, kind = split_path(path)
                if kind not in ("svc", "vol"):
                    continue
                thr.set_smon(path, local_expect="shutdown")
            self.wait_shutdown(thr=thr)

            # send a last status to peers so they can takeover asap
            mon.update_hb_data()

            mon._shutdown = True
            shared.wake_monitor("services shutdown done")
        except Exception as exc:
            thr.log.exception(exc)

        thr.log.info("services are now shutdown")
        while True:
            with shared.THREADS_LOCK:
                if not shared.THREADS["monitor"].is_alive():
                    break
            time.sleep(0.3)
        shared.DAEMON_STOP.set()
        return {"status": 0}

    def wait_shutdown(self, thr=None):
        def still_shutting(thr=None):
            for path, smon in thr.iter_local_services_monitors():
                if smon.local_expect == "shutdown":
                    return True
            return False
        while still_shutting(thr=thr):
            time.sleep(1)

