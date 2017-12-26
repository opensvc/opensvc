"""
Scheduler Thread
"""
import os
import sys
import logging
import time
import datetime
from subprocess import Popen, PIPE

import osvcd_shared as shared
import rcExceptions as ex
from rcGlobalEnv import rcEnv

class Scheduler(shared.OsvcThread):
    interval = 60
    delayed = {}
    running = set()

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["running"] = len(self.running)
        data["delayed"] = [{
            "cmd": " ".join(entry["cmd"]),
            "queued": entry["queued"].strftime(shared.JSON_DATEFMT),
            "expire": entry["expire"].strftime(shared.JSON_DATEFMT),
        } for entry in self.delayed.values()]
        return data

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.scheduler")
        self.log.info("scheduler started")
        if hasattr(os, "devnull"):
            devnull = os.devnull
        else:
            devnull = "/dev/null"
        self.devnull = os.open(devnull, os.O_RDWR)

        while True:
            try:
                self.do()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                self.terminate_procs()
                sys.exit(0)

    def do(self):
        self.reload_config()
        self.run_scheduler()
        for _ in range(self.interval):
            with shared.SCHED_TICKER:
                shared.SCHED_TICKER.wait(1)
            self.janitor_procs()
            if self.stopped():
                break

    def drop_running(self, sig):
        self.running -= set([sig])

    def exec_action(self, sig, cmd):
        if sig in self.running:
            self.log.debug("drop already running action '%s'", " ".join(cmd))
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=True)
        try:
            proc = Popen(cmd, **kwargs)
        except KeyboardInterrupt:
            return
        self.running.add(sig)
        self.push_proc(proc=proc,
                       on_success="drop_running",
                       on_success_args=[sig],
                       on_error="drop_running",
                       on_error_args=[sig])

    def queue_action(self, cmd, delay, sig):
        if delay == 0:
            self.log.debug("immediate exec of action '%s'" % ' '.join(cmd))
            self.exec_action(sig, cmd)
            return [sig]
        if sig in self.delayed:
            self.log.debug("drop already queued delayed action '%s'", " ".join(cmd))
            return []
        now = datetime.datetime.utcnow()
        exp = now + datetime.timedelta(seconds=delay)
        self.delayed[sig] = {
            "queued": now,
            "expire": exp,
            "cmd": cmd,
        }
        self.log.debug("queued action '%s' delayed until %s" % (' '.join(cmd), exp))
        return []

    def dequeue_action(self, sig, data, now):
        if now < data["expire"]:
            return False
        self.log.info("dequeue action '%s' queued at %s" % (data["cmd"], data["queued"]))
        self.exec_action(sig, data["cmd"])
        return True

    def dequeue_actions(self):
        now = datetime.datetime.utcnow()
        dequeued = []
        for sig, data in self.delayed.items():
            ret = self.dequeue_action(sig, data, now)
            if ret:
                dequeued.append(sig)
        for sig in dequeued:
            del self.delayed[sig]

    def run_scheduler(self):
        #self.log.info("run schedulers")
        nonprov = []
        run = []

        self.dequeue_actions()
        if shared.NODE:
            shared.NODE.options.cron = True
            for action in shared.NODE.sched.scheduler_actions:
                try:
                    delay = shared.NODE.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                cmd = [rcEnv.paths.nodemgr, action, "--cron"]
                sig = ":".join(["node", action])
                run += self.queue_action(cmd, delay, sig)
        for svc in shared.SERVICES.values():
            svc.options.cron = True
            try:
                provisioned = shared.AGG[svc.svcname].provisioned
            except KeyError:
                continue
            if provisioned is not True:
                nonprov.append(svc.svcname)
                continue
            for action in svc.sched.scheduler_actions:
                try:
                    data = svc.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                cmd = [rcEnv.paths.svcmgr, "-s", svc.svcname, action, "--cron", "--waitlock=5"]
                try:
                    rids, delay = data
                    rids = ','.join(rids)
                    sig = ":".join(["svc", action, rids])
                    cmd += ["--rid", rids]
                except TypeError:
                    delay = data
                    sig = ":".join(["svc", action])
                run += self.queue_action(cmd, delay, sig)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(run) > 0:
            msg.append("ran: %s." % ", ".join(run))
        if len(msg) > 0:
            self.log.info(" ".join(msg))

