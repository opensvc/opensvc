"""
Scheduler Thread
"""
import os
import sys
import logging
import time
from subprocess import Popen, PIPE

import osvcd_shared as shared
from rcGlobalEnv import rcEnv

class Scheduler(shared.OsvcThread):
    max_runs = 2
    interval = 60

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.scheduler")
        self.log.info("scheduler started")
        self.last_run = time.time()
        if hasattr(os, "devnull"):
            devnull = os.devnull
        else:
            devnull = "/dev/null"
        self.devnull = os.open(devnull, os.O_RDWR)
        self.cmd = [rcEnv.paths.nodemgr, 'schedulers']

        while True:
            try:
                self.do()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                self.terminate_procs()
                sys.exit(0)

    def do(self):
        self.janitor_procs()
        self.reload_config()
        now = time.time()
        with shared.SCHED_TICKER:
            shared.SCHED_TICKER.wait(self.interval)

        if len(self.procs) > self.max_runs:
            self.log.warning("%d scheduler runs are already in progress. "
                             "skip this run.", self.max_runs)
            return

        self.last_run = now
        self.run_scheduler()

    def run_scheduler(self):
        #self.log.info("run schedulers")
        try:
            proc = Popen(self.cmd, stdout=self.devnull, stderr=self.devnull,
                         stdin=self.devnull, close_fds=True)
        except KeyboardInterrupt:
            return
        self.push_proc(proc=proc)


