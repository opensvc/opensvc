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

    def max_tasks(self):
        if self.node_overloaded():
            return 2
        else:
            return 6

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
        last = time.time()
        done = 0
        while True:
            now = time.time()
            if done == 0:
                with shared.SCHED_TICKER:
                    shared.SCHED_TICKER.wait(1)
            if self.stopped():
                break
            self.dequeue_actions()
            if last + self.interval < now:
                last = time.time()
                self.run_scheduler()
            done = self.janitor_procs()

    def drop_running(self, sig):
        self.running -= set([sig])

    def exec_action(self, sig, cmd):
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=os.name!="nt")
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
        if sig in self.delayed:
            self.log.debug("drop already queued action '%s'", " ".join(cmd))
            self.log.debug("drop already queued sig '%s'", sig)
            return []
        if sig in self.running:
            self.log.debug("drop already running action '%s'", " ".join(cmd))
            return []
        now = datetime.datetime.utcnow()
        exp = now + datetime.timedelta(seconds=delay)
        self.delayed[sig] = {
            "queued": now,
            "expire": exp,
            "cmd": cmd,
        }
        if delay == 0:
            self.log.debug("queued action '%s' for run asap" % ' '.join(cmd))
        else:
            self.log.debug("queued action '%s' delayed until %s" % (' '.join(cmd), exp))
        return []

    def dequeue_actions(self):
        now = datetime.datetime.utcnow()
        dequeued = []
        to_run = [sig for sig, task in self.delayed.items() if task["expire"] < now]
        to_run = sorted(to_run, key=lambda sig: self.delayed[sig]["queued"])
        open_slots = max(self.max_tasks() - len(self.running), 0)
        for sig in to_run[:open_slots]:
            self.log.info("dequeue action '%s' queued at %s",
                          " ".join(self.delayed[sig]["cmd"]),
                          self.delayed[sig]["queued"])
            self.exec_action(sig, self.delayed[sig]["cmd"])
            dequeued.append(sig)
        for sig in dequeued:
            del self.delayed[sig]

    def run_scheduler(self):
        #self.log.info("run schedulers")
        nonprov = []
        run = []

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
        for svcname in list(shared.SERVICES.keys()):
            try:
                svc = shared.SERVICES[svcname]
            except KeyError:
                # deleted during previous iterations
                continue
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
                    sig = ":".join(["svc", svc.svcname, action, rids])
                    cmd += ["--rid", rids]
                except TypeError:
                    delay = data
                    sig = ":".join(["svc", svc.svcname, action])
                run += self.queue_action(cmd, delay, sig)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(run) > 0:
            msg.append("ran: %s." % ", ".join(run))
        if len(msg) > 0:
            self.log.info(" ".join(msg))

