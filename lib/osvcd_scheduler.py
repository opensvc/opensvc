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
    dropped_via_notify = set()

    def max_tasks(self):
        if self.node_overloaded():
            return 2
        else:
            return shared.NODE.max_parallel

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["running"] = len(self.running)
        data["delayed"] = [{
            "action": action,
            "svcname": svcname,
            "rid": rid,
            "queued": entry["queued"].strftime(shared.JSON_DATEFMT),
            "expire": entry["expire"].strftime(shared.JSON_DATEFMT),
        } for (action, svcname, rid), entry in self.delayed.items()]
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
                self.kill_procs()
                sys.exit(0)

    def do(self):
        self.reload_config()
        last = time.time()
        done = 0
        while True:
            now = time.time()
            self.janitor_run_done()
            if done == 0:
                with shared.SCHED_TICKER:
                    shared.SCHED_TICKER.wait(1)
            if self.stopped():
                break
            if shared.NMON_DATA.status not in ("init", "upgrade", "shutting"):
                self.dequeue_actions()
                if last + self.interval < now:
                    last = time.time()
                    self.run_scheduler()
            done = self.janitor_procs()

    def janitor_run_done(self):
        with shared.RUN_DONE_LOCK:
            sigs = set(shared.RUN_DONE)
            shared.RUN_DONE = set()
        if not sigs:
            return
        inter = sigs & self.running
        if not inter:
            return
        self.log.debug("run done notifications: %s", inter)
        self.running -= inter
        self.dropped_via_notify |= inter
        #self.log.debug("dropped_via_notify: %s", self.dropped_via_notify)

    def drop_running(self, sigs):
        """
        Drop for running tasks signatures those not yet dropped via
        notifications.
        """
        sigs = set(sigs)
        not_dropped_yet = sigs - self.dropped_via_notify
        self.running -= not_dropped_yet
        self.dropped_via_notify -= sigs

    def exec_action(self, sigs, cmd):
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=os.name!="nt",
                      env=os.environ.copy())
        try:
            proc = Popen(cmd, **kwargs)
        except KeyboardInterrupt:
            return
        self.running |= set(sigs)
        self.push_proc(proc=proc,
                       cmd=cmd,
                       on_success="drop_running",
                       on_success_args=[sigs],
                       on_error="drop_running",
                       on_error_args=[sigs])

    def format_cmd(self, action, svcname=None, rids=None):
        if svcname is None:
            cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "nodemgr.py"), action]
        elif isinstance(svcname, list):
            cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "svcmgr.py"), "-s", ",".join(svcname), action, "--waitlock=5", "--parallel"]
        else:
            cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "svcmgr.py"), "-s", svcname, action, "--waitlock=5"]
        if rids:
            cmd += ["--rid", ",".join(sorted(list(rids)))]
        cmd.append("--cron")
        return cmd

    def promote_queued_action(self, sig, delay):
        if delay == 0 and self.delayed[sig]["delay"] > 0:
            self.log.debug("promote queued action %s from delayed to asap", sig)
            now = datetime.datetime.utcnow()
            self.delayed[sig]["delay"] = 0
            self.delayed[sig]["expire"] = now

    def queue_action(self, action, delay=0, svcname=None, rid=None):
        sig = (action, svcname, rid)
        if sig in self.running:
            self.log.debug("drop already running action '%s'", sig)
            return
        if sig in self.delayed:
            self.promote_queued_action(sig, delay)
            self.log.debug("drop already queued action %s", sig)
            return
        now = datetime.datetime.utcnow()
        exp = now + datetime.timedelta(seconds=delay)
        self.delayed[sig] = {
            "queued": now,
            "expire": exp,
            "delay": delay,
        }
        if delay > 0:
            self.log.debug("queued action %s for run asap", sig)
        else:
            self.log.debug("queued action %s delayed until %s", sig, exp)
        return

    def get_todo(self):
        """
        Merge queued tasks, sort by queued date, and return the first
        <n> tasks, where <n> is the number of open slots in the running
        queue.
        """
        todo = {}
        merge = {}
        open_slots = max(self.max_tasks() - len(self.procs), 0)
        now = datetime.datetime.utcnow()
        for sig, task in self.delayed.items():
            if task["expire"] > now:
                continue
            action, svcname, rid = sig
            merge_key = (action, svcname)
            if merge_key not in merge:
                if svcname:
                    _svcname = [svcname]
                else:
                    _svcname = None
                merge[merge_key] = {"rids": set([rid]), "task": task}
            else:
                merge[merge_key]["rids"].add(rid)
                if task["queued"] < merge[merge_key]["task"]["queued"]:
                    merge[merge_key]["task"]["queued"] = task["queued"]

        for (action, svcname), data in merge.items():
            if None in data["rids"]:
                data["rids"] = None
                sigs = [(action, svcname, None)]
                merge_key = (svcname is None, action, None)
            else:
                sigs = [(action, svcname, rid) for rid in data["rids"]]
                merge_key = (svcname is None, action, tuple(sorted(list(data["rids"]))))
            if merge_key not in todo:
                if svcname:
                    _svcname = [svcname]
                else:
                    _svcname = None
                todo[merge_key] = {
                    "action": action,
                    "rids": data["rids"],
                    "svcname": _svcname,
                    "sigs": sigs,
                    "queued": task["queued"],
                }
            else:
                todo[merge_key]["sigs"] += sigs
                if svcname:
                    todo[merge_key]["svcname"].append(svcname)
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
            try:
                del self.delayed[sig]
            except KeyError:
                #print(sig, self.delayed)
                pass

    def run_scheduler(self):
        #self.log.info("run schedulers")
        nonprov = []

        if shared.NODE:
            shared.NODE.options.cron = True
            for action in shared.NODE.sched.scheduler_actions:
                try:
                    delay = shared.NODE.sched.validate_action(action)
                except ex.excAbortAction:
                    continue
                self.queue_action(action, delay)
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
                if rids is None:
                    self.queue_action(action, delay, svc.svcname, rids)
                else:
                    for rid in rids:
                        self.queue_action(action, delay, svc.svcname, rid)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(msg) > 0:
            self.log.info(" ".join(msg))

