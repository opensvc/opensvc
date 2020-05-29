"""
Scheduler Thread
"""
import os
import sys
import logging
import time
from subprocess import Popen

import daemon.shared as shared
import core.exceptions as ex
from env import Env
from utilities.converters import print_duration

MIN_PARALLEL = 6
MIN_OVERLOADED_PARALLEL = 2
DEQUEUE_INTERVAL = 2
ENQUEUE_INTERVAL = 30
FUTURE = 60
MIN_DEQUEUE_INTERVAL = 0.5
JANITOR_CERTS_INTERVAL = 3600
ACTIONS_SKIP_ON_UNPROV = [
    "sync_all",
    "compliance_auto",
    "resource_monitor",
    "run",
]

class Scheduler(shared.OsvcThread):
    name = "scheduler"
    delayed = {}
    running = set()
    dropped_via_notify = set()
    certificates = {}
    last_janitor_certs = 0

    def max_tasks(self):
        if self.node_overloaded():
            return MIN_OVERLOADED_PARALLEL
        elif shared.NODE.max_parallel > MIN_PARALLEL:
            return shared.NODE.max_parallel
        else:
            return MIN_PARALLEL

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["running"] = len(self.running)
        data["delayed"] = []

        # thread-safe delayed dump
        keys = list(self.delayed)
        for key in keys:
            action, path, rid = key
            try:
                entry = self.delayed[key]
            except KeyError:
                # deleted during iteration
                continue
            data["delayed"].append({
                "action": action,
                "path": path,
                "rid": rid,
                "queued": entry["queued"],
                "expire": entry["expire"],
            })
        return data

    def run(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.scheduler"), {"node": Env.nodename, "component": self.name})
        self.log.info("scheduler started")
        self.cluster_ca = "system/sec/ca-"+self.cluster_name
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
        last = 0
        done = 0
        init = True
        while True:
            if self.stopped():
                break
            now = time.time()
            self.now = self.run_time(now)
            future = self.now + FUTURE
            self.janitor_run_done()
            self.janitor_certificates()
            if shared.NMON_DATA.status not in ("init", "upgrade", "shutting"):
                if init:
                    init = False
                    last = now
                    self.run_scheduler(self.now)
                elif now - last >= ENQUEUE_INTERVAL:
                    last = now
                    self.run_scheduler(future)
                self.dequeue_actions()
            done = self.janitor_procs()
            if not done:
                self.do_wait()

    def do_wait(self):
        with shared.SCHED_TICKER:
            shared.SCHED_TICKER.wait(self.dequeue_delay())

    def dequeue_delay(self):
        """
        Returns DEQUEUE_INTERVAL with drift correction, to try and
        do a loop on regular interval, whatever the loop duration.
        """
        delay = DEQUEUE_INTERVAL - (time.time() - self.now)
        if delay < MIN_DEQUEUE_INTERVAL:
            delay = MIN_DEQUEUE_INTERVAL
        return delay

    def janitor_certificates(self):
        if self.now < self.last_janitor_certs + JANITOR_CERTS_INTERVAL:
            return
        if self.first_available_node() != Env.nodename:
            return
        self.last_janitor_certs = time.time()
        for path in [p for p in shared.SERVICES]:
            try:
                obj = shared.SERVICES[path]
            except KeyError:
                continue
            if obj.kind not in ("sec", "usr"):
                continue
            try:
                ca = obj.oget("DEFAULT", "ca")
            except Exception as exc:
                continue
            if ca != self.cluster_ca:
                continue
            cf_mtime = shared.CLUSTER_DATA.get(Env.nodename, {}).get("services", {}).get("config", {}).get(obj.path, {}).get("updated")
            if cf_mtime is None:
                continue
            if obj.path not in self.certificates or self.certificates[obj.path]["mtime"] < cf_mtime:
                try:
                    expire = obj.get_cert_expire()
                except ex.Error:
                    # usr in creation
                    expire = None
                self.certificates[obj.path] = {
                    "mtime": cf_mtime,
                    "expire": expire,
                }
            expire = self.certificates[obj.path]["expire"]
            if not expire:
                continue
            expire_delay = expire - self.now
            #print(obj.path, "expire in:", print_duration(expire_delay))
            if expire_delay < 3600:
                self.log.info("renew %s certificate, expiring in %s", obj.path, print_duration(expire_delay))
                obj.gen_cert()

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
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        env["OSVC_SCHED_TIME"] = str(self.now)
        kwargs = dict(stdout=self.devnull, stderr=self.devnull,
                      stdin=self.devnull, close_fds=os.name!="nt",
                      env=env)
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

    def format_cmd(self, action, path=None, rids=None):
        if path is None:
            cmd = Env.om + ["node", action]
        elif isinstance(path, list):
            cmd = Env.om + ["svc", "-s", ",".join(path), action, "--waitlock=5"]
            if len(path) > 1:
                cmd.append("--parallel")
        else:
            cmd = Env.om + ["svc", "-s", path, action, "--waitlock=5"]
        if rids:
            cmd += ["--rid", ",".join(sorted(list(rids)))]
        cmd.append("--cron")
        return cmd

    def format_log_cmd(self, action, path=None, rids=None):
        if path is None:
            cmd = ["om", "node", action]
        elif isinstance(path, list):
            cmd = ["om", "svc", "-s", ",".join(path), action, "--waitlock=5"]
            if len(path) > 1:
                cmd.append("--parallel")
        else:
            cmd = ["om", "svc", "-s", path, action, "--waitlock=5"]
        if rids:
            cmd += ["--rid", ",".join(sorted(list(rids)))]
        cmd.append("--cron")
        return cmd

    def promote_queued_action(self, sig, delay, now):
        if delay == 0 and self.delayed[sig]["delay"] > 0 and self.delayed[sig]["expire"] > now:
            self.log.debug("promote queued action '%s' to run asap", sig)
            self.delayed[sig]["delay"] = 0
            self.delayed[sig]["expire"] = now
        else:
            self.log.debug("skip already queued action '%s'", sig)

    def queue_action(self, action, delay=0, path=None, rid=None, now=None):
        sig = (action, path, rid)
        if delay is None:
            delay = 0
        if sig in self.running:
            self.log.debug("skip already running action '%s'", sig)
            return
        if sig in self.delayed:
            self.promote_queued_action(sig, delay, now)
            return
        exp = now + delay
        self.delayed[sig] = {
            "queued": self.now,
            "expire": exp,
            "delay": delay,
        }
        if not delay:
            self.log.debug("queued action '%s' for run in %s", sig, print_duration(exp-self.now))
        else:
            self.log.debug("queued action '%s' for run in %s + %s delay", sig, print_duration(exp-self.now), print_duration(delay))
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
        if not open_slots:
            return []
        self.janitor_delayed()

        for sig, task in self.delayed.items():
            if task["expire"] > self.now:
                continue
            action, path, rid = sig
            merge_key = (action, path)
            if merge_key not in merge:
                merge[merge_key] = {"rids": set([rid]), "task": task}
            else:
                merge[merge_key]["rids"].add(rid)
                if task["queued"] < merge[merge_key]["task"]["queued"]:
                    merge[merge_key]["task"]["queued"] = task["queued"]

        for (action, path), data in merge.items():
            if None in data["rids"]:
                data["rids"] = None
                sigs = [(action, path, None)]
                merge_key = (path is None, action, None)
            else:
                sigs = [(action, path, rid) for rid in data["rids"]]
                merge_key = (path is None, action, tuple(sorted(list(data["rids"]))))
            if merge_key not in todo:
                todo[merge_key] = {
                    "action": action,
                    "rids": data["rids"],
                    "path": [path] if path else None,
                    "sigs": sigs,
                    "queued": data["task"]["queued"],
                }
            else:
                todo[merge_key]["sigs"] += sigs
                if path:
                    todo[merge_key]["path"].append(path)
                if data["task"]["queued"] < todo[merge_key]["queued"]:
                    todo[merge_key]["queued"] = data["task"]["queued"]
        return sorted(todo.values(), key=lambda task: task["queued"])[:open_slots]

    def janitor_delayed(self):
        drop = []
        for sig, task in self.delayed.items():
            action, path, rid = sig
            if not path:
                continue
            if not rid:
                continue
            try:
                shared.SERVICES[path].get_resource(rid).check_requires(action, cluster_data=shared.CLUSTER_DATA)
            except KeyError:
                # deleted during previous iterations
                drop.append(sig)
                continue
            except (ex.Error, ex.ContinueAction) as exc:
                self.log.info("drop queued %s on %s %s: %s", action, path, rid, exc)
                drop.append(sig)
                continue
            except Exception as exc:
                self.log.error("drop queued %s on %s %s: %s", action, path, rid, exc)
                drop.append(sig)
                continue
        self.delete_queued(drop)

    def dequeue_actions(self):
        """
        Get merged tasks to run from get_todo(), execute them and purge the
        delayed hash.
        """
        dequeued = []
        for task in self.get_todo():
            cmd = self.format_cmd(task["action"], task["path"], task["rids"])
            log_cmd = self.format_log_cmd(task["action"], task["path"], task["rids"])
            self.log.info("run '%s' queued %s ago", " ".join(log_cmd), print_duration(self.now - task["queued"]))
            self.exec_action(task["sigs"], cmd)
            dequeued += task["sigs"]
        self.delete_queued(dequeued)

    def delete_queued(self, sigs):
        if not isinstance(sigs, list):
            sigs = [sigs]
        for sig in sigs:
            try:
                del self.delayed[sig]
            except KeyError:
                #print(sig, self.delayed)
                pass

    def get_lasts(self, svc):
        data = {}
        for nodename in svc.peers:
            instance = self.get_service_instance(svc.path, nodename)
            if not instance:
                continue
            for rid, rdata in instance.get("resources", {}).items():
                try:
                    sdata = rdata["info"]["sched"]
                except KeyError:
                    continue
                if not sdata:
                    continue
                if rid not in data:
                    data[rid] = sdata
                else:
                    for action, adata in sdata.items():
                        if "last" not in adata:
                            continue
                        if action not in data[rid]:
                            data[rid][action] = adata
                            continue
                        try:
                            if data[rid][action]["last"] < adata["last"]:
                                data[rid][action] = adata
                        except KeyError:
                            pass
        return data

    def run_time(self, now):
        return int(now // 60 * 60)

    def run_scheduler(self, now):
        #self.log.info("run scheduler")
        nonprov = []

        if shared.NODE:
            shared.NODE.options.cron = True
            for action in shared.NODE.sched.actions:
                try:
                    delay = shared.NODE.sched.validate_action(action, now=now)
                except ex.AbortAction:
                    continue
                self.queue_action(action, delay, now=now)
        for path in list(shared.SERVICES):
            try:
                svc = shared.SERVICES[path]
            except KeyError:
                # deleted during previous iterations
                continue
            svc.options.cron = True
            svc.sched.configure()
            try:
                provisioned = shared.AGG[path].provisioned
            except KeyError:
                continue
            lasts = self.get_lasts(svc)
            for action, parms in svc.sched.actions.items():
                if provisioned in ("mixed", False) and action in ACTIONS_SKIP_ON_UNPROV:
                    nonprov.append(action+"@"+path)
                    continue
                try:
                    data = svc.sched.validate_action(action, lasts=lasts, now=now)
                except ex.AbortAction as exc:
                    self.log.debug("skip %s on %s: validation", action, path)
                    continue
                try:
                    rids, delay = data
                except TypeError:
                    delay = data
                    rids = None
                if rids is None:
                    self.queue_action(action, delay, path, rids, now=now)
                else:
                    for rid in rids:
                        self.queue_action(action, delay, path, rid, now=now)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(msg) > 0:
            self.log.debug(" ".join(msg))

