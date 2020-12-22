"""
Scheduler Thread
"""
import logging
import os
import sys
import time
import uuid

from multiprocessing import Process

import daemon.shared as shared
import core.exceptions as ex
from env import Env
from utilities.cache import purge_cache_session
from utilities.converters import print_duration
from utilities.storage import Storage
from utilities.lazy import lazy, unset_lazy

try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO

MIN_PARALLEL = 6
MIN_OVERLOADED_PARALLEL = 2
JANITOR_PROCS_INTERVAL = 0.95
SCHEDULE_INTERVAL = 10
DEQUEUE_INTERVAL = 5
JANITOR_CERTS_INTERVAL = 3600
ACTIONS_SKIP_ON_UNPROV = [
    "sync_all",
    "compliance_auto",
    "resource_monitor",
    "run",
]
NMON_STATUS_OFF = [
    "init",
    "upgrade",
    "shutting",
    "maintenance",
]

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = lambda x: None

def wrapper(path, action, options, now, session_id, cmd):
    os.environ["OSVC_ACTION_ORIGIN"] = "daemon"
    os.environ["OSVC_SCHED_TIME"] = str(now)
    os.environ["OSVC_PARENT_SESSION_UUID"] = session_id
    sys.argv = cmd
    Env.session_uuid = session_id
    from core.node import Node
    from utilities.naming import split_path, factory
    node = Node()
    if path is None:
        o = node
    else:
        name, namespace, kind = split_path(path)
        o = factory(kind)(name, namespace, node=node, log_handlers=["file", "syslog"])
    try:
        setproctitle(" ".join(cmd))
    except Exception as exc:
        print(exc)
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    o.action(action, options)

class Scheduler(shared.OsvcThread):
    name = "scheduler"
    delayed = {}
    blacklist = {}
    lasts = {}
    session_ids = {}
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
                "csum": entry["csum"],
            })
        return data

    def run(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.scheduler"), {"node": Env.nodename, "component": self.name})
        self.log.info("scheduler started")
        self.update_status()
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
                time.sleep(0.2)
            if self.stopped():
                self.kill_procs()
                sys.exit(0)

    def do(self):
        self.reload_config()
        last = 0
        while True:
            if self.stopped():
                break
            changed = False
            now = time.time()
            done = self.janitor_procs()
            self.janitor_run_done()
            if done or last + SCHEDULE_INTERVAL <= now:
                last = now
                self.janitor_certificates(now)
                self.janitor_blacklist()
                nmon = self.get_node_monitor()
                if nmon and nmon.status not in NMON_STATUS_OFF:
                    self.run_scheduler(now)
                    changed = True
            if now >= self.next_expire(now):
                self.dequeue_actions(now)
                changed = True
            if changed:
                self.update_status()
            self.sleep()

    def sleep(self):
        with shared.SCHED_TICKER:
            shared.SCHED_TICKER.wait(JANITOR_PROCS_INTERVAL)

    def janitor_certificates(self, now):
        if now < self.last_janitor_certs + JANITOR_CERTS_INTERVAL:
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
            try:
                cf_mtime = self.get_service_config(obj.path, Env.nodename).updated
            except AttributeError:
                continue
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
            expire_delay = expire - now
            #print(obj.path, "expire in:", print_duration(expire_delay))
            if expire_delay < 3600:
                self.log.info("renew %s certificate, expiring in %s", obj.path, print_duration(expire_delay))
                obj.gen_cert()

    def janitor_run_done(self):
        with shared.RUN_DONE_LOCK:
            sigs = set(shared.RUN_DONE)
            shared.RUN_DONE = set()
        if not sigs:
            return 0
        inter = sigs & self.running
        if not inter:
            return
        self.log.debug("run done notifications: %s", inter)
        self.running -= inter
        self.dropped_via_notify |= inter
        #self.log.debug("dropped_via_notify: %s", self.dropped_via_notify)
        return

    def drop_running(self, sigs):
        """
        Drop for running tasks signatures those not yet dropped via
        notifications.
        """
        sigs = set(sigs)
        not_dropped_yet = sigs - self.dropped_via_notify
        self.running -= not_dropped_yet
        self.dropped_via_notify -= sigs
        self.purge_cache()

    def purge_cache(self):
        purged = []
        for session_id, sigset in self.session_ids.items():
            if not sigset & self.running:
                purge_cache_session(session_id)
                purged.append(session_id)
        for session_id in purged:
            del self.session_ids[session_id]

    def exec_action(self, sigs, path, action, rids, queued, now, session_id):
        options = self.format_options(rids)
        cmd = self.format_log_cmd(action, path, rids)
        self.log.info("run '%s'", " ".join(cmd))
        try:
            proc = Process(group=None, target=wrapper, args=(path, action, options, now, session_id, cmd, ))
            proc.start()
        except KeyboardInterrupt:
            return
        sigset = set(sigs)
        self.running |= sigset
        try:
            self.session_ids[session_id] |= sigset
        except KeyError:
            self.session_ids[session_id] = sigset
        for sig in sigs:
            self.lasts[sig] = now
        self.push_proc(proc=proc,
                       cmd=cmd,
                       on_success="drop_running",
                       on_success_args=[sigs],
                       on_error="drop_running",
                       on_error_args=[sigs])

    def format_options(self, rids=None):
        options = Storage({
            "waitlock": 1,
            "cron": True,
        })
        if rids:
            options.rid = ",".join(sorted(list(rids)))
        return options

    def format_log_cmd(self, action, path=None, rids=None):
        if path is None:
            cmd = ["om", "node", action]
        else:
            cmd = ["om", "svc", "-s", path, action, "--waitlock=1"]
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

    def next_expire(self, now):
        try:
            return min([t["expire"] for t in self.delayed.values()])
        except ValueError:
            return now + DEQUEUE_INTERVAL

    def queue_action(self, action, delay=0, path=None, rid=None, now=None, csum=None):
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
            "queued": now,
            "expire": exp,
            "delay": delay,
            "csum": csum,
        }
        if not delay:
            self.log.debug("queued action '%s' for run in %s", sig, print_duration(exp-now))
        else:
            self.log.debug("queued action '%s' for run in %s + %s delay", sig, print_duration(exp-now), print_duration(delay))
        return

    def get_todo(self, now):
        """
        Merge queued tasks, sort by queued date, and return the first
        <n> tasks, where <n> is the number of open slots in the running
        queue.
        """
        todo = []
        merge = {}
        open_slots = max(self.max_tasks() - len(self.procs), 0)
        if not open_slots:
            return []
        self.janitor_delayed()

        for sig, task in self.delayed.items():
            if task["expire"] > now:
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
            else:
                sigs = [(action, path, rid) for rid in data["rids"]]
            todo.append({
                "action": action,
                "rids": data["rids"],
                "path": path,
                "sigs": sigs,
                "queued": data["task"]["queued"],
            })
        return sorted(todo, key=lambda task: task["queued"])[:open_slots]

    def janitor_blacklist(self):
        csum = self.csum()
        for sig in list(self.blacklist):
            action, path, rid = sig
            _csum = self.blacklist[sig]
            if path is None:
                if _csum != csum:
                    self.log.info("remove from blacklist: %s", action)
                    del self.blacklist[sig]
            else:
                ocsum = self.node_data.get(["services", "config", path, "csum"], None)
                if _csum != ocsum:
                    self.log.info("remove from blacklist: %s %s %s", path, action, rid or None)
                    del self.blacklist[sig]

    def janitor_delayed(self):
        drop = []
        csum = self.csum()
        for sig, task in self.delayed.items():
            action, path, rid = sig
            if not path:
                if csum and csum != task["csum"]:
                    self.log.info("drop action '%s' on %s%s: node or cluster config changed",
                                  action, path, " " + rid if rid else "")
                    drop.append(sig)
                continue
            if path not in shared.SERVICES:
                self.log.info("drop action '%s' on %s%s: object deleted",
                              action, path, " " +rid if rid else "")
                drop.append(sig)
                continue
            ocsum = self.node_data.get(["services", "config", path, "csum"], None)
            if ocsum and ocsum != task["csum"]:
                self.log.info("drop action '%s' on %s%s: object config changed",
                              action, path, " " + rid if rid else "")
                drop.append(sig)
                continue
            if not rid:
                continue
            try:
                shared.SERVICES[path].get_resource(rid).check_requires(action, cluster_data=self.run_cluster_data)
            except KeyError:
                # deleted during previous iterations
                drop.append(sig)
                continue
            except (ex.Error, ex.ContinueAction) as exc:
                self.log.info("drop action '%s' on %s %s: %s", action, path, rid, exc)
                drop.append(sig)
                continue
            except Exception as exc:
                self.log.error("drop action '%s' on %s %s: %s", action, path, rid, exc)
                drop.append(sig)
                continue
        self.delete_queued(drop)

    def dequeue_actions(self, now):
        """
        Get merged tasks to run from get_todo(), execute them and purge the
        delayed hash.
        """
        unset_lazy(self, "run_cluster_data")
        dequeued = []
        session_id = str(uuid.uuid4())
        for task in self.get_todo(now):
            self.exec_action(task["sigs"], task["path"], task["action"], task["rids"], task["queued"], now, session_id)
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

    def csum(self):
        ncsum = self.node_data.get(["config", "csum"], None)
        if not ncsum:
            return
        ccsum = self.node_data.get(["services", "config", "cluster", "csum"], None)
        if not ccsum:
            return
        return ",".join([ncsum, ccsum])

    def local_last(self, sig, fname, obj):
        try:
            last = self.lasts[sig]
        except KeyError:
            last = obj.sched.get_last(fname)
            if last:
                last = time.mktime(last.timetuple())
                self.lasts[sig] = last
        return last

    @lazy
    def run_cluster_data(self):
        return self.nodes_data.get()

    def run_scheduler(self, now):
        #self.log.info("run scheduler")
        nonprov = []

        if not shared.NODE:
            return

        shared.NODE.options.cron = True

        csum = self.csum()
        if not csum:
            return

        for action, parms in shared.NODE.sched.actions.items():
            for p in parms:
                if p.req_collector and not shared.NODE.collector_env.dbopensvc:
                    continue
                sig = (action, None, None)
                if sig in self.delayed:
                    continue
                if sig in self.blacklist:
                    continue
                if sig in self.running:
                    continue
                last = self.local_last(sig, p.fname, shared.NODE)
                try:
                    _next, _interval = shared.NODE.sched.get_schedule(p.section, p.schedule_option).get_next(now, last)
                except Exception as exc:
                    self.log.warning("node %s %s: %s", p.section, action, exc)
                    self.blacklist[sig] = csum
                    continue
                if not _next:
                    self.blacklist[sig] = csum
                    continue
                _next = time.mktime(_next.timetuple())
                delay = _next - now
                self.queue_action(action, delay, None, None, now=now, csum=csum)

        for path in list(shared.SERVICES):
            try:
                svc = shared.SERVICES[path]
            except KeyError:
                # deleted during previous iterations
                continue
            svc.options.cron = True
            svc.sched.configure()
            csum = self.node_data.get(["services", "config", path, "csum"], None)
            agg = self.get_service_agg(path)
            if not agg:
                continue
            lasts = self.get_lasts(svc)
            for action, parms in svc.sched.actions.items():
                if agg.provisioned in ("mixed", False) and action in ACTIONS_SKIP_ON_UNPROV:
                    nonprov.append(action+"@"+path)
                    continue
                for p in parms:
                    if p.req_collector and not shared.NODE.collector_env.dbopensvc:
                        continue
                    rid = p.section if p.section != "DEFAULT" else None
                    sig = (action, path, rid)
                    if sig in self.delayed:
                        continue
                    if sig in self.blacklist:
                        continue
                    if sig in self.running:
                        continue
                    last = self.local_last(sig, p.fname, svc)
                    try:
                        cluster_last = lasts[p.section][action]["last"]
                        if not last or cluster_last > last:
                            # local last may be more up-to-date due to CRM task runs notifications
                            last = cluster_last
                    except KeyError:
                        pass
                    try:
                        _next, _interval = svc.sched.get_schedule(p.section, p.schedule_option).get_next(now, last)
                    except Exception as exc:
                        self.log.warning("%s %s %s: %s", path, p.section, action, exc)
                        self.log.exception(exc)
                        self.blacklist[sig] = csum
                        continue
                    if not _next:
                        self.blacklist[sig] = csum
                        continue
                    if rid:
                        try:
                            svc.get_resource(rid).check_requires(action, cluster_data=self.run_cluster_data)
                        except (KeyError, AttributeError):
                            continue
                        except (ex.Error, ex.ContinueAction) as exc:
                            # run_requires not satisfied
                            continue
                    _next = time.mktime(_next.timetuple())
                    delay = _next - now
                    self.queue_action(action, delay, path, rid, now=now, csum=csum)

        # log a scheduler loop digest
        msg = []
        if len(nonprov) > 0:
            msg.append("non provisioned service skipped: %s." % ", ".join(nonprov))
        if len(msg) > 0:
            self.log.debug(" ".join(msg))

