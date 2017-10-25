"""
Scheduler Thread
"""
import os
import sys
import logging
import time
from subprocess import Popen, PIPE

import osvcd_shared as shared
import rcExceptions as ex
from rcGlobalEnv import rcEnv

class Scheduler(shared.OsvcThread):
    interval = 60

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

    def run_scheduler(self):
        #self.log.info("run schedulers")
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=True)
        run = []
        nonprov = []

        if shared.NODE:
            shared.NODE.options.cron = True
            _run = []
            for action in shared.NODE.sched.scheduler_actions:
                try:
                    shared.NODE.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                _run.append(action)
                cmd = [rcEnv.paths.nodemgr, action, "--cron"]
                try:
                    proc = Popen(cmd, **kwargs)
                except KeyboardInterrupt:
                    return
                self.push_proc(proc=proc)
            if len(_run) > 0:
                run.append("node:%s" % ",".join(_run))
        for svc in shared.SERVICES.values():
            svc.options.cron = True
            try:
                provisioned = shared.AGG[svc.svcname].provisioned
            except KeyError:
                continue
            if provisioned is not True:
                nonprov.append(svc.svcname)
                continue
            _run = []
            for action in svc.sched.scheduler_actions:
                try:
                    rids = svc.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                cmd = [rcEnv.paths.svcmgr, "-s", svc.svcname, action, "--cron", "--waitlock=5"]
                if rids:
                    cmd += ["--rid", ",".join(rids)]
                    _run.append("%s(%s)" % (action, ','.join(rids)))
                else:
                    _run.append(action)
                try:
                    proc = Popen(cmd, **kwargs)
                except KeyboardInterrupt:
                    return
                self.push_proc(proc=proc)
            if len(_run) > 0:
                run.append("%s:%s" % (svc.svcname, ",".join(_run)))

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(run) > 0:
            msg.append("ran: %s." % ", ".join(run))
        if len(msg) > 0:
            self.log.info(" ".join(msg))

