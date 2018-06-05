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

ACTIONS_SKIP_ON_UNPROV = [
    "sync_all",
    "compliance_auto",
]

class Scheduler(shared.OsvcThread):
    interval = 60
    delayed = {}
    running = set()

    def max_tasks(self):
        if self.node_overloaded():
            return 2
        else:
            return shared.NODE.max_parallel

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["running"] = len(self.running)
        data["delayed"] = [{
            "cmd": " ".join(self.format_cmd(action, svcname, rids)),
            "queued": entry["queued"].strftime(shared.JSON_DATEFMT),
            "expire": entry["expire"].strftime(shared.JSON_DATEFMT),
        } for (action, delay, svcname, rids), entry in self.delayed.items()]
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
            if shared.NMON_DATA.status not in ("init", "upgrade"):
                self.dequeue_actions()
                if last + self.interval < now:
                    last = time.time()
                    self.run_scheduler()
            done = self.janitor_procs()

    def drop_running(self, sigs):
        self.running -= set(sigs)

    def exec_action(self, sigs, cmd):
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=os.name!="nt")
        try:
            proc = Popen(cmd, **kwargs)
        except KeyboardInterrupt:
            return
        self.running |= set(sigs)
        self.push_proc(proc=proc,
                       on_success="drop_running",
                       on_success_args=[sigs],
                       on_error="drop_running",
                       on_error_args=[sigs])

    def format_cmd(self, action, svcname=None, rids=None):
        if svcname is None:
            cmd = [rcEnv.paths.nodemgr, action]
        elif isinstance(svcname, list):
            cmd = [rcEnv.paths.svcmgr, "-s", ",".join(svcname), action, "--waitlock=5", "--parallel"]
        else:
            cmd = [rcEnv.paths.svcmgr, "-s", svcname, action, "--waitlock=5"]
        if rids:
            cmd += ["--rid", rids]
        cmd.append("--cron")
        return cmd

    def queue_action(self, action, delay=0, svcname=None, rids=None):
        if rids:
            rids = ",".join(rids)
        sig = (action, delay, svcname, rids)
        if sig in self.delayed:
            self.log.debug("drop already queued action '%s'", str(sig))
            return []
        if sig in self.running:
            self.log.debug("drop already running action '%s'", str(sig))
            return []
        now = datetime.datetime.utcnow()
        exp = now + datetime.timedelta(seconds=delay)
        self.delayed[sig] = {
            "queued": now,
            "expire": exp,
        }
        if delay > 0:
            self.log.debug("queued action %s for run asap" % str(sig))
        else:
            self.log.debug("queued action %s delayed until %s" % (str(sig), exp))
        return []

    def get_todo(self):
        """
        Merge queued tasks, sort by queued date, and return the first
        <n> tasks, where <n> is the number of open slots in the running
        queue.
        """
        todo = {}
        open_slots = max(self.max_tasks() - len(self.running), 0)
        now = datetime.datetime.utcnow()
        for sig, task in self.delayed.items():
            if task["expire"] > now:
                continue
            action, delay, svcname, rids = sig
            merge_key = (svcname is None, action, rids)
            if merge_key not in todo:
                if svcname:
                    _svcname = [svcname]
                else:
                    _svcname = None
                todo[merge_key] = {
                    "action": action,
                    "rids": rids,
                    "svcname": _svcname,
                    "sigs": [(action, delay, svcname, rids)],
                    "queued": task["queued"],
                }
            else:
                if svcname:
                    todo[merge_key]["svcname"].append(svcname)
                todo[merge_key]["sigs"].append((action, delay, svcname, rids))
                if task["queued"] < todo[merge_key]["queued"]:
                    todo[merge_key]["queued"] = task["queued"]
        return sorted(todo.values(), key=lambda task: task["queued"])[:open_slots]

    def dequeue_actions(self):
        """
        Get merged tasks to run from get_todo(), execute them and purge the
        delayed hash.
        """
        dequeued = []
        for task in self.get_todo():
            cmd = self.format_cmd(task["action"], task["svcname"], task["rids"])
            self.log.info("run '%s' queued at %s", " ".join(cmd), task["queued"])
            self.exec_action(task["sigs"], cmd)
            dequeued += task["sigs"]
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
                run += self.queue_action(action, delay)
        for svcname in list(shared.SERVICES.keys()):
            try:
                svc = shared.SERVICES[svcname]
            except KeyError:
                # deleted during previous iterations
                continue
            svc.configure_scheduler()
            svc.options.cron = True
            try:
                provisioned = shared.AGG[svc.svcname].provisioned
            except KeyError:
                continue
            for action in svc.sched.scheduler_actions:
                if provisioned is not True and action in ACTIONS_SKIP_ON_UNPROV:
                    nonprov.append(action+"@"+svc.svcname)
                    continue
                try:
                    data = svc.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                try:
                    rids, delay = data
                except TypeError:
                    delay = data
                    rids = None
                run += self.queue_action(action, delay, svc.svcname, rids)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(run) > 0:
            msg.append("queued: %s." % ", ".join(run))
        if len(msg) > 0:
            self.log.info(" ".join(msg))

