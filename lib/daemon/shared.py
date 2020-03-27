"""
A module to share variables used by osvcd threads.
"""
import os
import threading
import time
import fnmatch
import hashlib
import json
import json_delta
import re
import tempfile
import shutil
from subprocess import Popen, PIPE

import six
# noinspection PyUnresolvedReferences
from six.moves import queue

import core.exceptions as ex
from jsonpath_ng.ext import parse
from rcUtilities import lazy, unset_lazy, factory, split_path, normalize_paths
from rcGlobalEnv import rcEnv
from utilities.storage import Storage
from freezer import Freezer
from comm import Crypt
from .events import EVENTS


class DebugRLock(object):
    def __init__(self):
        self._lock = threading.RLock()
        self.t = 0
        self.name = ""

    def acquire(self, *args, **kwargs):
        from traceback import format_stack
        print("=== %s acquire\n%s" % (self.name, "".join(format_stack()[2:-1])))
        self._lock.acquire(*args, **kwargs)
        print("=== %s acquired\n%s" % (self.name, "".join(format_stack()[2:-1])))

    def release(self, *args, **kwargs):
        from traceback import format_stack
        print("=== %s release\n%s" % (self.name, "".join(format_stack()[2:-1])))
        self._lock.release()

    def __enter__(self):
        self.t = time.time()
        self._lock.acquire()

    def __exit__(self, type, value, traceback):
        self._lock.release()
        d = time.time() - self.t
        if d < 1:
            return
        from traceback import format_stack
        print("=== %s held %.2fs\n%s" % (self.name, d, "".join(format_stack()[2:-1])))


# RLock = DebugRLock
RLock = threading.RLock

# a global to store the Daemon() instance
DAEMON = None

# daemon_status cache
LAST_DAEMON_STATUS = {}
DAEMON_STATUS_LOCK = RLock()
DAEMON_STATUS = {}
PATCH_ID = 0

# disable orchestration if a peer announces a different compat version than
# ours
COMPAT_VERSION = 10

# expose api handlers version
API_VERSION = 6

# node and cluster conf lock to block reading changes during a multi-write
# transaction (ex daemon join)
CONFIG_LOCK = RLock()

# the event queue to feed to clients listening for changes
EVENT_Q = queue.Queue()

# current generation of the dataset on the local node
GEN = 1

# track the generation of the local dataset on peer nodes
LOCAL_GEN = {}

# track the generation of the peer datasets we merged
REMOTE_GEN = {}

# track the local dataset gen diffs pending merge by peers
GEN_DIFF = {}

DATEFMT = "%Y-%m-%dT%H:%M:%S.%fZ"
JSON_DATEFMT = "%Y-%m-%dT%H:%M:%SZ"
MAX_MSG_SIZE = 1024 * 1024

# The threads store
THREADS = {}
THREADS_LOCK = RLock()

# A node object instance. Used to access node properties and methods.
NODE = None
NODE_LOCK = RLock()

# CRM services objects. Used to access services properties.
# The monitor thread reloads a new Svc object when the corresponding
# configuration file changes.
SERVICES = {}
SERVICES_LOCK = RLock()

# Per service aggretated data of instances.
# Refresh on monitor status eval, and embedded in the returned data
AGG = {}
AGG_LOCK = RLock()

# The encrypted message all the heartbeat tx threads send.
# It is refreshed in the monitor thread loop.
HB_MSG = None
HB_MSG_LEN = 0
HB_MSG_LOCK = RLock()

# the local service monitor data, where the listener can set expected states
SMON_DATA = {}
SMON_DATA_LOCK = RLock()

# the local node monitor data, where the listener can set expected states
NMON_DATA = Storage({
    "status": "init",
    "status_updated": time.time(),
})
NMON_DATA_LOCK = RLock()

# the node monitor states evicting a node from ranking algorithms
NMON_STATES_PRESERVED = (
   "maintenance",
   "upgrade",
   "init",
   "shutting",
)

# a boolean flag used to signal the monitor it has to do the long loop asap
MON_CHANGED = []

# cluster wide locks, aquire/release via the listener (usually the unix socket),
# consensus via the heartbeat links.
LOCKS = {}
LOCKS_LOCK = RLock()

# The per-threads configuration, stats and states store
# The monitor thread states include cluster-wide aggregated data
CLUSTER_DATA = {rcEnv.nodename: {}}
CLUSTER_DATA_LOCK = RLock()

# The lock to serialize CLUSTER_DATA updates from rx threads
RX_LOCK = RLock()

# thread loop conditions and helpers
DAEMON_STOP = threading.Event()
MON_TICKER = threading.Condition()
COLLECTOR_TICKER = threading.Condition()
SCHED_TICKER = threading.Condition()
HB_TX_TICKER = threading.Condition()

# a queue of xmlrpc calls to do, fed by the lsnr, purged by the
# collector thread
COLLECTOR_XMLRPC_QUEUE = []

# a set of run action signatures done, fed by the crm to the lsnr,
# purged by the scheduler thread
RUN_DONE_LOCK = RLock()
RUN_DONE = set()

# min interval between thread stats refresh
STATS_INTERVAL = 1

# to prevent concurrent join handler execution
JOIN_LOCK = RLock()

# Agent as a relay heartbeart server
RELAY_DATA = {}
RELAY_LOCK = RLock()
RELAY_SLOT_MAX_AGE = 24 * 60 * 60
RELAY_JANITOR_INTERVAL = 10 * 60

# try to give a name to the locks, for debugging when using
# the pure python locks (native locks don't support setattr)
try:
    DAEMON_STATUS_LOCK.name = "DAEMON_STATUS"
    CONFIG_LOCK.name = "CONFIG"
    THREADS_LOCK.name = "THREADS"
    NODE_LOCK.name = "NODE"
    SERVICES_LOCK.name = "SERVICES"
    AGG_LOCK.name = "AGG"
    HB_MSG_LOCK.name = "HB_MSG"
    SMON_DATA_LOCK.name = "SMON_DATA"
    NMON_DATA_LOCK.name = "NMON_DATA"
    LOCKS_LOCK.name = "LOCKS_LOCK"
    CLUSTER_DATA_LOCK.name = "CLUSTER_DATA"
    RUN_DONE_LOCK.name = "RUN_DONE"
except AttributeError:
    pass


def wake_heartbeat_tx():
    """
    Notify the heartbeat tx thread to do they periodic job immediatly
    """
    with HB_TX_TICKER:
        HB_TX_TICKER.notify_all()


def wake_monitor(reason="unknown", immediate=False):
    """
    Notify the monitor thread to do they periodic job immediatly
    """
    global MON_CHANGED
    with MON_TICKER:
        MON_CHANGED.append(reason)
        if immediate:
            MON_TICKER.notify_all()


def wake_collector():
    """
    Notify the scheduler thread to do they periodic job immediatly
    """
    with COLLECTOR_TICKER:
        COLLECTOR_TICKER.notify_all()


def wake_scheduler():
    """
    Notify the scheduler thread to do they periodic job immediatly
    """
    with SCHED_TICKER:
        SCHED_TICKER.notify_all()


#############################################################################
#
# Base Thread class
#
#############################################################################
class OsvcThread(threading.Thread, Crypt):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """
    stop_tmo = 60

    def __init__(self):
        super(OsvcThread, self).__init__()
        self.log = None
        self.alerts = []
        self._stop_event = threading.Event()
        self._node_conf_event = threading.Event()
        self.created = time.time()
        self.configured = self.created
        self.threads = []
        self.procs = []
        self.tid = None
        self.stats_data = None
        self.last_stats_refresh = 0
        self.arbitrators_data = None

        # hash for log dups avoiding
        self.duplog_data = {}

        self.rankers = {
            "nodes order": "placement_ranks_nodes_order",
            "shift": "placement_ranks_shift",
            "spread": "placement_ranks_spread",
            "score": "placement_ranks_score",
            "load avg": "placement_ranks_load_avg",
            "none": "placement_ranks_none",
        }

    def alert(self, lvl, fmt, *args):
        if lvl == "info":
            fn = self.log.info
        elif lvl == "warning":
            fn = self.log.warning
        elif lvl == "error":
            fn = self.log.error
        else:
            self.log.error('alert called with invalid lvl %s', lvl)
            fn = self.log.error
        fn(fmt, *args)
        self.alerts.append({
            "severity": lvl,
            "message": fmt % args,
        })

    def notify_config_change(self):
        """
        Notify thread the node configuration file changed.
        """
        self._node_conf_event.set()

    def stop(self):
        """
        Notify thread they need to stop.
        """
        self._stop_event.set()

    def unstop(self):
        """
        Notify daemon it is free to restart the thread.
        """
        self._stop_event.clear()

    def stopped(self):
        """
        Return True if the thread excepted state is stopped.
        """
        return self._stop_event.is_set()

    def status(self, **kwargs):
        """
        Return the thread status data structure to embed in the 'daemon
        status' data.
        """
        if self.stopped():
            if self.is_alive():
                state = "stopping"
            else:
                state = "stopped"
        else:
            if self.is_alive():
                state = "running"
            else:
                state = "terminated"
        data = {
            "state": state,
            "created": self.created,
            "configured": self.configured,
        }
        if self.alerts:
            data["alerts"] = self.alerts
        if self.tid:
            data["tid"] = self.tid
        return data

    def thread_stats(self):
        if self.tid is None:
            return
        now = time.time()
        if self.stats_data and now - self.last_stats_refresh < STATS_INTERVAL:
            return self.stats_data
        try:
            tid_cpu_time = NODE.tid_cpu_time(self.tid)
        except Exception:
            tid_cpu_time = 0.0
        try:
            tid_mem_total = NODE.tid_mem_total(self.tid)
        except Exception:
            tid_mem_total = 0
        self.stats_data = {
            "threads": len(self.threads),
            "procs": len(self.procs),
            "cpu": {
                "time": tid_cpu_time,
            },
            "mem": {
                "total": tid_mem_total,
            },
        }
        self.last_stats_refresh = now
        return self.stats_data

    def set_tid(self):
        self.tid = NODE.get_tid()

    def has_proc(self, cmd):
        for proc in self.procs:
            if proc.cmd == cmd:
                return True
        return False

    def push_proc(self, proc,
                  on_success=None, on_success_args=None,
                  on_success_kwargs=None, on_error=None,
                  on_error_args=None, on_error_kwargs=None,
                  cmd=None):
        """
        Enqueue a structure including a Popen() result and the success and
        error callbacks.
        """
        self.procs.append(Storage({
            "proc": proc,
            "cmd": cmd,
            "on_success": on_success,
            "on_success_args": on_success_args if on_success_args else [],
            "on_success_kwargs": on_success_kwargs if on_success_kwargs else {},
            "on_error": on_error,
            "on_error_args": on_error_args if on_error_args else [],
            "on_error_kwargs": on_error_kwargs if on_error_kwargs else {},
        }))

    def kill_procs(self):
        """
        Send a kill() to all procs in the queue and wait for their
        completion.
        """
        if six.PY2:
            # noinspection PyShadowingBuiltins
            ProcessLookupError = OSError
        for data in self.procs:
            # noinspection PyUnboundLocalVariable
            try:
                data.proc.kill()
            except ProcessLookupError:  # pylint: disable=undefined-variable
                continue
            for _ in range(self.stop_tmo):
                data.proc.poll()
                if data.proc.returncode is not None:
                    data.proc.communicate()
                    break
                time.sleep(1)

    def janitor_procs(self):
        done = []
        for idx, data in enumerate(self.procs):
            data.proc.poll()
            if data.proc.returncode is not None:
                data.proc.communicate()
                done.append(idx)
                if data.proc.returncode == 0 and data.on_success:
                    getattr(self, data.on_success)(*data.on_success_args,
                                                   **data.on_success_kwargs)
                elif data.proc.returncode != 0 and data.on_error:
                    getattr(self, data.on_error)(*data.on_error_args,
                                                 **data.on_error_kwargs)
        for idx in sorted(done, reverse=True):
            del self.procs[idx]
        return len(done)

    def join_threads(self, timeout=10):
        for thr in self.threads:
            if not hasattr(thr, "stop"):
                continue
            self.log.info("stop %s", thr)
            thr.stop()
        while timeout > 0:
            self.janitor_threads()
            if len(self.threads) == 0:
                return
            timeout -= 1
            time.sleep(1)
        self.log.warning("timeout waiting for threads to terminate. %s left alive.", len(self.threads))
        self.log_status()

    def log_status(self):
        from utilities.render.color import format_str_flat_json
        data = self.status()
        for line in format_str_flat_json(data).splitlines():
            self.log.info(line)

    def janitor_threads(self):
        done = []
        for idx, thr in enumerate(self.threads):
            thr.join(0)
            if not thr.is_alive():
                done.append(idx)
        for idx in sorted(done, reverse=True):
            del self.threads[idx]
        if len(self.threads) > 2:
            self.log.debug("threads queue length %d", len(self.threads))
        return len(done)

    @lazy
    def freezer(self):
        return Freezer("node")

    def reload_config(self):
        if not self._node_conf_event.is_set():
            return
        if NMON_DATA.status not in (None, "idle"):
            return
        with SMON_DATA_LOCK:
            for data in SMON_DATA.values():
                if data.status not in (None, "idle") and "failed" not in data.status:
                    return
        self._node_conf_event.clear()
        self.event("node_config_change")
        unset_lazy(self, "config")
        unset_lazy(self, "quorum")
        unset_lazy(self, "vip")
        unset_lazy(NODE, "arbitrators")
        unset_lazy(self, "cluster_name")
        unset_lazy(self, "cluster_names")
        unset_lazy(self, "cluster_key")
        unset_lazy(self, "cluster_id")
        unset_lazy(self, "cluster_nodes")
        unset_lazy(self, "sorted_cluster_nodes")
        unset_lazy(self, "maintenance_grace_period")
        unset_lazy(self, "rejoin_grace_period")
        unset_lazy(self, "ready_period")
        self.arbitrators_data = None
        self.alerts = []
        if not hasattr(self, "reconfigure"):
            self.configured = time.time()
            return
        try:
            getattr(self, "reconfigure")()
        except Exception as exc:
            self.log.error("reconfigure error: %s", str(exc))
            self.stop()
        self.configured = time.time()

    @staticmethod
    def get_service(path):
        try:
            return SERVICES[path]
        except KeyError:
            return

    @staticmethod
    def patch_has_nodes_info_change(patch):
        for _patch in patch:
            try:
                if _patch[0][0] == "labels":
                    return True
            except KeyError:
                continue
            try:
                if _patch[0][0] == "targets":
                    return True
            except KeyError:
                continue
        return False

    def set_nmon(self, status=None, local_expect=None, global_expect=None):
        global NMON_DATA
        changed = False
        with NMON_DATA_LOCK:
            if status:
                if status != NMON_DATA.status:
                    self.log.info(
                        "node monitor status change: %s => %s",
                        NMON_DATA.status if
                        NMON_DATA.status else "none",
                        status
                    )
                    changed = True
                    NMON_DATA.status = status
                    NMON_DATA.status_updated = time.time()
                    NMON_DATA.global_expect_updated = time.time()

            if local_expect:
                if local_expect == "unset":
                    local_expect = None
                if local_expect != NMON_DATA.local_expect:
                    self.log.info(
                        "node monitor local expect change: %s => %s",
                        NMON_DATA.local_expect if
                        NMON_DATA.local_expect else "none",
                        local_expect
                    )
                    changed = True
                    NMON_DATA.local_expect = local_expect

            if global_expect:
                if global_expect == "unset":
                    global_expect = None
                if global_expect != NMON_DATA.global_expect:
                    self.log.info(
                        "node monitor global expect change: %s => %s",
                        NMON_DATA.global_expect if
                        NMON_DATA.global_expect else "none",
                        global_expect
                    )
                    changed = True
                    NMON_DATA.global_expect = global_expect
                    NMON_DATA.global_expect_updated = time.time()

        if changed:
            wake_monitor(reason="node mon change")

    def set_smon(self, path, status=None, local_expect=None,
                 global_expect=None, reset_retries=False,
                 stonith=None, expected_status=None):
        global SMON_DATA
        instance = self.get_service_instance(path, rcEnv.nodename)
        if instance and not instance.get("resources", {}) \
                and not status \
                and ((global_expect is None and local_expect is None and status == "idle")  # TODO refactor this
                     or global_expect not in (
                             "frozen",
                             "thawed",
                             "aborted",
                             "unset",
                             "deleted",
                             "purged")):
            # skip slavers, wrappers, scalers
            return
        # will set changed to True if an update occur, this will avoid wake_monitor calls
        changed = False
        with SMON_DATA_LOCK:
            if path not in SMON_DATA:
                SMON_DATA[path] = Storage({
                    "status": "idle",
                    "status_updated": time.time(),
                    "global_expect_updated": time.time(),
                })
                changed = True
            if status:
                reset_placement = False
                if status != SMON_DATA[path].status \
                        and (not expected_status or expected_status == SMON_DATA[path].status):
                    self.log.info(
                        "service %s monitor status change: %s => %s",
                        path,
                        SMON_DATA[path].status if
                        SMON_DATA[path].status else "none",
                        status
                    )
                    if SMON_DATA[path].status is not None \
                            and "failed" in SMON_DATA[path].status \
                            and "failed" not in status:
                        # the placement might become "leader" after transition
                        # from "failed" to "not-failed". recompute asap so the
                        # orchestrator won't take an undue "stop_instance"
                        # decision.
                        reset_placement = True
                    SMON_DATA[path].status = status
                    SMON_DATA[path].status_updated = time.time()
                    changed = True
                if reset_placement:
                    SMON_DATA[path].placement = \
                        self.get_service_placement(path)
                    SMON_DATA[path].status_updated = time.time()
                    changed = True

            if local_expect:
                if local_expect == "unset":
                    local_expect = None
                if local_expect != SMON_DATA[path].local_expect:
                    self.log.info(
                        "service %s monitor local expect change: %s => %s",
                        path,
                        SMON_DATA[path].local_expect if
                        SMON_DATA[path].local_expect else "none",
                        local_expect
                    )
                    SMON_DATA[path].local_expect = local_expect
                    changed = True

            if global_expect:
                if global_expect == "unset":
                    global_expect = None
                if global_expect != SMON_DATA[path].global_expect:
                    self.log.info(
                        "service %s monitor global expect change: %s => %s",
                        path,
                        SMON_DATA[path].global_expect if
                        SMON_DATA[path].global_expect else "none",
                        global_expect
                    )
                    SMON_DATA[path].global_expect = global_expect
                    SMON_DATA[path].global_expect_updated = time.time()
                    changed = True
            if reset_retries and "restart" in SMON_DATA[path]:
                self.log.info("service %s monitor resources restart count "
                              "reset", path)
                del SMON_DATA[path]["restart"]
                changed = True

            if stonith:
                if stonith == "unset":
                    stonith = None
                if stonith != SMON_DATA[path].stonith:
                    self.log.info(
                        "service %s monitor stonith change: %s => %s",
                        path,
                        SMON_DATA[path].stonith if
                        SMON_DATA[path].stonith else "none",
                        stonith
                    )
                    SMON_DATA[path].stonith = stonith
                    changed = True
        if changed:
            wake_monitor(reason="service %s mon change" % path)

    def get_node_monitor(self, nodename=None):
        """
        Return the Monitor data of the node.
        """
        if nodename is None:
            with NMON_DATA_LOCK:
                data = Storage(NMON_DATA)
        else:
            with CLUSTER_DATA_LOCK:
                if nodename not in CLUSTER_DATA:
                    return
                data = Storage(CLUSTER_DATA[nodename].get("monitor", {}))
        return data

    def get_service_monitor(self, path):
        """
        Return the Monitor data of a service.
        """
        try:
            data = Storage(SMON_DATA[path])
        except KeyError:
            self.set_smon(path, "idle")
            data = Storage(SMON_DATA[path])
        data["placement"] = self.get_service_placement(path)
        return data

    def get_service_placement(self, path):
        try:
            svc = SERVICES[path]
        except KeyError:
            return ""
        if self.placement_leader(svc, silent=True):
            return "leader"
        return ""

    def hook_command(self, cmd, data):
        """
        A generic nodemgr command Popen wrapper.
        """
        cmd = list(cmd)
        eid = data.get("data", {}).get("id")
        self.log.info("execute %s hook: %s", eid, " ".join(cmd))
        try:
            proc = Popen(cmd, stdout=None, stderr=None, stdin=PIPE,
                         close_fds=True)
            proc.stdin.write(json.dumps(data).encode())
            proc.stdin.close()
        except Exception as exc:
            self.log.error("%s hook %s execution error: %s", eid,
                           " ".join(cmd), exc)
            return
        return proc

    def node_command(self, cmd):
        """
        A generic nodemgr command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        _cmd = [] + rcEnv.python_cmd
        _cmd += [os.path.join(rcEnv.paths.pathlib, "nodemgr.py")]
        self.log.info("execute: nodemgr %s", " ".join(cmd))
        proc = Popen(_cmd+cmd, stdout=None, stderr=None, stdin=None,
                     close_fds=True, env=env)
        return proc

    def service_command(self, path, cmd, stdout=None, stderr=None, stdin=None, local=True):
        """
        A generic svcmgr command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        _cmd = [] + rcEnv.python_cmd
        _cmd += [os.path.join(rcEnv.paths.pathlib, "svcmgr.py")]
        if path:
            cmd = ["-s", path] + cmd
        if local:
            cmd += ["--local"]
        self.log.info("execute: svcmgr %s", " ".join(cmd))
        if stdin is not None:
            _stdin = PIPE
        else:
            _stdin = None
        proc = Popen(_cmd+cmd, stdout=stdout, stderr=stderr, stdin=_stdin,
                     close_fds=True, env=env)
        if stdin:
            proc.stdin.write(stdin.encode())
        return proc

    def add_cluster_node(self, nodename):
        NODE.unset_lazy("cd")
        NODE.unset_lazy("private_cd")
        unset_lazy(self, "cluster_nodes")
        if nodename in self.cluster_nodes:
            return
        nodes = self.cluster_nodes + [nodename]
        if "nodes" in NODE.private_cd.get("cluster", {}):
            NODE.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
        else:
            from core.objects.ccfg import Ccfg
            svc = Ccfg()
            svc.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
            del svc

    def remove_cluster_node(self, nodename):
        NODE.unset_lazy("cd")
        NODE.unset_lazy("private_cd")
        unset_lazy(self, "cluster_nodes")
        if nodename not in self.cluster_nodes:
            return
        nodes = [node for node in self.cluster_nodes if node != nodename]
        from core.objects.ccfg import Ccfg
        svc = Ccfg()
        buff = "cluster.nodes="+" ".join(nodes)
        self.log.info("set %s in cluster config" % buff)
        svc.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
        self.log.info("unset cluster.nodes in node config")
        NODE.unset_multi(["cluster.nodes"])
        del svc

    @lazy
    def quorum(self):
        return NODE.oget("cluster", "quorum")

    @lazy
    def maintenance_grace_period(self):
        return NODE.oget("node", "maintenance_grace_period")

    @lazy
    def rejoin_grace_period(self):
        return NODE.oget("node", "rejoin_grace_period")

    @lazy
    def ready_period(self):
        return NODE.oget("node", "ready_period")

    def in_maintenance_grace_period(self, nmon):
        if nmon.status in ("upgrade", "init"):
            return True
        if nmon.status == "maintenance" and \
           nmon.status_updated > time.time() - self.maintenance_grace_period:
            return True
        return False

    def arbitrators_votes(self):
        votes = []
        for arbitrator in NODE.arbitrators:
            ret = NODE._ping(arbitrator["name"], arbitrator["timeout"])
            if ret == 0:
                votes.append(arbitrator["name"])
        return votes

    def split_handler(self):
        if not self.quorum:
            self.duplog("info",
                        "cluster is split, ignore as cluster.quorum is "
                        "false", msgid="quorum disabled")
            return
        if self.freezer.node_frozen():
            self.duplog("info",
                        "cluster is split, ignore as the node is frozen",
                        msgid="quorum disabled")
            return
        total = len(self.cluster_nodes) + len(NODE.arbitrators)
        live = len(CLUSTER_DATA)
        extra_votes = self.arbitrators_votes()
        n_extra_votes = len(extra_votes)
        if live + n_extra_votes > total / 2:
            self.duplog("info", "cluster is split, we have quorum: "
                        "%(live)d+%(avote)d out of %(total)d votes (%(a)s)",
                        live=live, avote=n_extra_votes, total=total,
                        a=",".join(extra_votes))
            return
        self.event("crash", {
            "reason": "split",
            "node_votes": live,
            "arbitrator_votes": n_extra_votes,
            "voting": total,
            "pro_voters": [nod for nod in CLUSTER_DATA] + extra_votes,
        })
        # give a little time for log flush
        NODE.sys_crash(delay=2)

    def forget_peer_data(self, nodename, change=False):
        """
        Purge a stale peer data if all rx threads are down.
        """
        nmon = self.get_node_monitor(nodename=nodename)
        if nmon is None:
            return
        if not self.peer_down(nodename):
            if change:
                self.log.info("other rx threads still receive from node %s",
                              nodename)
            return
        if self.in_maintenance_grace_period(nmon):
            if change:
                self.log.info("preserve node %s data in %s since %d "
                              "(grace %s)", nodename, nmon.status,
                              time.time()-nmon.status_updated,
                              self.maintenance_grace_period)
            return
        nmon_status = nmon.status
        self.event(
            "forget_peer",
            {
                "reason": "no_rx",
                "peer": nodename,
            }
        )
        with CLUSTER_DATA_LOCK:
            try:
                del CLUSTER_DATA[nodename]
            except KeyError:
                pass
            try:
                del LOCAL_GEN[nodename]
            except KeyError:
                pass
            try:
                # will ask for a full when the node comes back again
                del REMOTE_GEN[nodename]
            except KeyError:
                pass
        wake_monitor(reason="forget node %s data" % nodename)
        if nmon_status == "shutting":
            self.log.info("cluster is not split, the lost node %s last known "
                          "monitor state is '%s'", nodename, nmon_status)
        else:
            self.split_handler()

    def peer_down(self, nodename):
        """
        Return True if no rx threads receive data from the specified peer
        node.
        """
        with THREADS_LOCK:
            thr_ids = [key for key in THREADS if key.endswith(".rx")]
        for thr_id in thr_ids:
            try:
                rx_status = THREADS[thr_id].status()
                peer_status = rx_status["peers"][nodename]["beating"]
            except KeyError:
                continue
            if peer_status:
                return False
        return True

    @staticmethod
    def get_service_instance(path, nodename):
        """
        Return the specified service status structure on the specified node.
        """
        try:
            return Storage(
                CLUSTER_DATA[nodename]["services"]["status"][path]
            )
        except (TypeError, KeyError):
            return

    def get_service_instances(self, path, discard_empty=False):
        """
        Return the specified service status structures on all nodes.
        """
        instances = {}
        for nodename in self.cluster_nodes:
            try:
                instance = CLUSTER_DATA[nodename]["services"]["status"][path]
                # provoke a KeyError on foreign instance, to discard them
                # noinspection PyStatementEffect
                instance["updated"]
            except (TypeError, KeyError):
                # foreign
                continue
            if discard_empty and not instance:
                continue
            instances[nodename] = instance
        return instances

    @staticmethod
    def get_service_agg(path):
        """
        Return the specified service aggregated status structure.
        """
        try:
            with AGG_LOCK:
                return AGG[path]
        except KeyError:
            return

    #########################################################################
    #
    # Placement policies
    #
    #########################################################################
    def placement_candidates(self, svc, discard_frozen=True,
                             discard_na=True,
                             discard_overloaded=True,
                             discard_preserved=True,
                             discard_unprovisioned=True,
                             discard_constraints_violation=True,
                             discard_start_failed=True):
        """
        Return the list of service nodes meeting the following criteria:
        * we have valid service instance data (not unknown, has avail)
        * the node is not in maintenance, shutting, init or upgrade (default)
        * the node is not frozen (default)
        * the node is not overloaded (default)
        * the service is not frozen (default)
        * the service instance is provisioned (default)
        * the service instance smon status is not "start failed" (default)
        * the service instance constraints are eval'ed True (default)
        """
        candidates = []
        if svc is None:
            return []
        for nodename in self.cluster_nodes:
            try:
                data = CLUSTER_DATA[nodename]
            except KeyError:
                continue
            if nodename not in svc.peers:
                # can happen if the same service is deployed on
                # differrent cluster segments
                continue
            if data == "unknown":
                continue
            if discard_preserved and \
               data.get("monitor", {}).get("status") in NMON_STATES_PRESERVED:
                continue
            if discard_frozen and data.get("frozen"):
                # node frozen
                continue
            instance = self.get_service_instance(svc.path, nodename)
            if instance is None:
                continue
            if discard_start_failed and \
               instance["monitor"].get("status") in (
                   "start failed",
                   "place failed"
               ):
                continue
            if "avail" not in instance:
                # deleting
                continue
            if discard_na and instance.avail == "n/a":
                continue
            if discard_frozen and instance.frozen:
                continue
            if discard_unprovisioned and instance.provisioned is False:
                continue
            if discard_constraints_violation and \
               not instance.get("constraints", True):
                continue
            if discard_overloaded and self.node_overloaded(nodename):
                continue
            candidates.append(nodename)
        return candidates

    def placement_ranks(self, svc, candidates=None):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        try:
            return getattr(self, self.rankers[svc.placement])(svc, candidates)
        except AttributeError:
            return [rcEnv.nodename]

    def duplog(self, lvl, msg, **kwargs):
        sig = str(kwargs.items())
        if sig is None:
            return
        if sig in self.duplog_data and msg == self.duplog_data[sig]:
            return
        self.duplog_data[sig] = msg
        if lvl == "info":
            fn = self.log.info
        elif lvl == "warning":
            fn = self.log.warning
        elif lvl == "error":
            fn = self.log.error
        else:
            return
        fn(msg, kwargs)

    def placement_leaders(self, svc, candidates=None):
        ranks = self.placement_ranks(svc, candidates=candidates)
        if not ranks:
            return []
        elif svc.topology == "failover":
            return ranks[0:1]
        elif svc.topology == "flex":
            return ranks[0:svc.flex_target]
        else:
            return []

    def placement_leader(self, svc, candidates=None, silent=False):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        if len(candidates) == 0:
            if not silent:
                self.duplog("info",
                            "placement constraints prevent us from starting "
                            "service %(path)s on any node",
                            path=svc.path)
            return False
        if rcEnv.nodename not in candidates:
            if not silent:
                self.duplog("info",
                            "placement constraints prevent us from starting "
                            "service %(path)s on this node",
                            path=svc.path)
            return False
        if len(candidates) == 1:
            if not silent:
                self.duplog("info",
                            "we have the greatest placement priority for "
                            "service %(path)s (alone)",
                            path=svc.path)
            return True

        ranks = self.placement_ranks(svc, candidates=candidates)
        if ranks == []:
            return False
        elif svc.topology == "failover":
            if rcEnv.nodename == ranks[0]:
                if not silent:
                    self.duplog("info",
                                "we have the highest '%(placement)s' "
                                "placement priority for failover service "
                                "%(path)s",
                                placement=svc.placement, path=svc.path)
                return True
            else:
                if not silent:
                    self.duplog("info",
                                "node %(nodename)s is alive and has a higher "
                                "'%(placement)s' placement priority for "
                                "failover service %(path)s",
                                nodename=ranks[0], placement=svc.placement,
                                path=svc.path)
                return False
        elif svc.topology == "flex":
            index = ranks.index(rcEnv.nodename) + 1
            if not silent:
                self.duplog("info",
                            "we have the %(idx)d/%(tgt)d '%(placement)s' "
                            "placement priority for flex service %(path)s",
                            idx=index, tgt=svc.flex_target,
                            placement=svc.placement, path=svc.path)
            if index <= svc.flex_target:
                return True
            else:
                return False

    def placement_ranks_none(self, svc, candidates, silent=False):
        """
        Always return an empty list.
        """
        return []

    def placement_ranks_spread(self, svc, candidates, silent=False):
        """
        hash together each candidate nodename+path, and sort the resulting
        list.
        """
        def fn(s):
            h = hashlib.md5()
            h.update(s.encode())
            return h.digest()
        return [nodename for nodename in
                sorted(candidates, key=lambda x: fn(svc.path+x))]

    def placement_ranks_score(self, svc, candidates, silent=False):
        data = []
        with CLUSTER_DATA_LOCK:
            for nodename in CLUSTER_DATA:
                if nodename not in candidates:
                    continue
                try:
                    load = CLUSTER_DATA[nodename]["stats"]["score"]
                except KeyError:
                    pass
                data.append((nodename, load))
        return [nodename for nodename, _ in sorted(data, key=lambda x: -x[1])]

    def placement_ranks_load_avg(self, svc, candidates, silent=False):
        data = []
        with CLUSTER_DATA_LOCK:
            for nodename in CLUSTER_DATA:
                if nodename not in candidates:
                    continue
                try:
                    load = CLUSTER_DATA[nodename]["stats"]["load_15m"]
                except KeyError:
                    try:
                        load = CLUSTER_DATA[nodename]["load"]["15m"]
                    except KeyError:
                        continue
                data.append((nodename, load))
        return [nodename for nodename, _ in sorted(data, key=lambda x: x[1])]

    def placement_ranks_nodes_order(self, svc, candidates, silent=False):
        return [nodename for nodename in svc.ordered_peers
                if nodename in candidates]

    def placement_ranks_shift(self, svc, candidates, silent=False):
        ranks = self.placement_ranks_nodes_order(svc, candidates,
                                                 silent=silent) * 2
        n_candidates = len(candidates)
        if n_candidates == 0:
            idx = 0
        else:
            idx = svc.slave_num % n_candidates
        return ranks[idx:idx+n_candidates]

    def first_available_node(self):
        for n in self.cluster_nodes:
            try:
                # noinspection PyStatementEffect
                CLUSTER_DATA[n]["monitor"]["status"]
                return n
            except KeyError:
                continue

    def get_oldest_gen(self, nodename=None):
        """
        Get oldest generation of the local dataset on peers.
        """
        if nodename is None:
            gens = LOCAL_GEN.values()
            if len(gens) == 0:
                return 0, 0
            gen = min(gens)
            num = len(gens)
            # self.log.info("oldest gen is %d amongst %d nodes", gen, num)
        else:
            if nodename not in LOCAL_GEN:
                return 0, 0
            gen = LOCAL_GEN.get(nodename, 0)
            num = 1
            # self.log.info("gen on node %s is %d", nodename, gen)
        return gen, num

    def purge_log(self):
        oldest, num = self.get_oldest_gen()
        if num == 0:
            # alone, truncate the log, we'll do a full
            to_remove = [gen for gen in GEN_DIFF]
        else:
            to_remove = [gen for gen in GEN_DIFF if gen < oldest]
        for gen in to_remove:
            # self.log.info("purge gen %d", gen)
            del GEN_DIFF[gen]

    @staticmethod
    def mon_changed():
        return MON_CHANGED != []

    @staticmethod
    def unset_mon_changed():
        global MON_CHANGED
        MON_CHANGED = []

    @staticmethod
    def get_gen(inc=False):
        global GEN
        if inc:
            GEN += 1
        gen = {rcEnv.nodename: GEN}
        gen.update(REMOTE_GEN)
        return gen

    def node_overloaded(self, nodename=None):
        if nodename is None:
            nodename = rcEnv.nodename
        node_data = CLUSTER_DATA.get(nodename)
        if node_data is None:
            return False
        for key in ("mem", "swap"):
            limit = node_data.get("min_avail_"+key, 0)
            total = node_data.get("stats", {}).get(key+"_total", 0)
            val = node_data.get("stats", {}).get(key+"_avail", 0)
            if total > 0 and val < limit:
                return True
        return False

    def nodes_info(self):
        data = {}
        for node in self.cluster_nodes:
            data[node] = {}
        with CLUSTER_DATA_LOCK:
            for node, _data in CLUSTER_DATA.items():
                data[node] = {
                    "labels": _data.get("labels", {}),
                    "targets": _data.get("targets", {}),
                }
        return data

    def dump_nodes_info(self):
        try:
            with open(rcEnv.paths.nodes_info, "r") as ofile:
                data = json.load(ofile)
        except Exception:
            data = {}
        new_data = {}
        for node, ndata in data.items():
            if node not in self.cluster_nodes:
                # drop nodes no longer in cluster
                continue
            new_data[node] = ndata
        for node, ndata in self.nodes_info().items():
            if not ndata and data.get(node):
                # preserve data we had info for
                continue
            new_data[node] = ndata
        if new_data == data:
            return
        try:
            tmpf = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.paths.pathtmp)
            fpath = tmpf.name
            tmpf.close()
            with open(fpath, "w") as ofile:
                json.dump(new_data, ofile)
            shutil.move(fpath, rcEnv.paths.nodes_info)
        except Exception as exc:
            self.alert("warning", "failed to refresh %s: %s", rcEnv.paths.nodes_info, exc)
        self.log.info("%s updated", rcEnv.paths.nodes_info)

    def on_nodes_info_change(self):
        """
        Rewrite the on-disk nodes info cache and flush caches of all
        information referencing labels.

        Object configuration references to #nodes and nodes can change on
        label changes, so refresh the status.json to expose those changes
        in the cluster data.

        For example:
        nodes = mylabel=a
        flex_target={#nodes}
        """
        NODE.unset_lazy("nodes_info")
        self.dump_nodes_info()
        for path in [p for p in SERVICES]:
            try:
                svc = SERVICES[path]
            except KeyError:
                # deleted
                continue
            svc.unset_conf_lazy()
            if NMON_DATA.status != "init":
                svc.print_status_data_eval(refresh=False, write_data=True, clear_rstatus=True)
                try:
                    # trigger status.json reload by the mon thread
                    CLUSTER_DATA[rcEnv.nodename]["services"]["status"][path]["updated"] = 0
                except KeyError:
                    pass
        wake_monitor(reason="nodes info change")

    def speaker(self):
        nodename = None
        for nodename in self.sorted_cluster_nodes:
            if nodename in CLUSTER_DATA and \
               CLUSTER_DATA[nodename] != "unknown":
                break
        if nodename == rcEnv.nodename:
            return True
        return False

    def event(self, eid, data=None, log_data=None, level="info"):
        """
        Put an "event"-kind event in the events queue, then log in node.log
        and in the service.log if a path is provided in <data>. If a
        <log_data> is passed, merge it in <data> before formatting the messages
        to log.
        """
        evt = {
            "nodename": rcEnv.nodename,
            "ts": time.time(),
            "kind": "event",
        }
        if not isinstance(data, dict):
            data = {}
        data["id"] = eid
        path = data.get("path")
        data["monitor"] = Storage(self.get_node_monitor())
        if path:
            try:
                data["service"] = Storage(self.get_service_agg(path))
            except TypeError:
                data["service"] = Storage()
            try:
                data["instance"] = Storage(
                    self.get_service_instance(path, rcEnv.nodename)
                )
            except TypeError:
                data["instance"] = Storage()
            try:
                data["instance"]["monitor"] = Storage(
                    self.get_service_monitor(path)
                )
            except TypeError:
                data["instance"]["monitor"] = Storage()
            rid = data.get("rid")
            resource = data.get("resource")
            if resource:
                try:
                    data["resource"] = Storage(data["resource"])
                except TypeError:
                    data["resource"] = Storage()
            elif rid:
                try:
                    data["resource"] = Storage(
                        data["instance"].get("resources", {}).get(rid, {})
                    )
                except TypeError:
                    data["resource"] = Storage()
            try:
                del data["instance"]["resources"]
            except KeyError:
                pass

        evt["data"] = data
        EVENT_Q.put(evt)
        hooks = NODE.hooks.get(eid, set()) | NODE.hooks.get("all", set())
        for hook in hooks:
            proc = self.hook_command(hook, evt)
            if proc:
                self.push_proc(proc, cmd=" ".join(hook))

        if not level:
            return

        key = eid, data.get("reason")
        fmt = EVENTS.get(key)
        if not fmt:
            # fallback to a generic message
            key = eid, None
            fmt = EVENTS.get(key)
        if not fmt:
            return

        fmt_data = {}
        fmt_data.update(data)
        if isinstance(log_data, dict):
            fmt_data.update(log_data)

        path = fmt_data.get("path")
        if path:
            # log to node.log with a "service <path> " prefix
            node_fmt = "service {path} "+fmt
            getattr(self.log, level)(node_fmt.format(**fmt_data))

            # log to <name>.log
            try:
                svc = SERVICES[path]
                getattr(svc.log, level)(fmt.format(**fmt_data))
            except KeyError:
                pass
        else:
            # log to node.log with no prefix
            getattr(self.log, level)(fmt.format(**fmt_data))

    @lazy
    def vip(self):

        def parse_vip(s):
            try:
                addr, netmask = s.split("/", 1)
                netmask, ipdev = netmask.split("@", 1)
                return addr, netmask, ipdev
            except Exception:
                return

        default_vip = NODE.oget("cluster", "vip")
        if not default_vip:
            return
        try:
            default_addr, default_netmask, default_ipdev = parse_vip(default_vip)
        except Exception:
            return
        template = [
            ("sync#i0", "disable", "true"),
            ("DEFAULT", "orchestrate", "ha"),
            ("DEFAULT", "nodes", "*"),
            ("DEFAULT", "monitor_action", "switch"),
            ("DEFAULT", "monitor_schedule", "@1m"),
            ("ip#0", "monitor", "true"),
            ("ip#0", "restart", "1"),
        ]
        self.log.info("cluster vip %s" % default_vip)
        for node in self.cluster_nodes:
            vip = NODE.oget("cluster", "vip", impersonate=node)
            if vip is None:
                self.log.info("cluster vip not set for node %s", node)
                continue
            try:
                addr, netmask, ipdev = parse_vip(vip)
            except Exception as exc:
                self.log.info("cluster vip not set or malformed: %s", exc)
                return
            t = ("ip#0", "ipname", addr)
            if t not in template:
                template.append(t)
            t = ("ip#0", "netmask", netmask)
            if t not in template:
                template.append(t)
            if ipdev == default_ipdev:
                t = ("ip#0", "ipdev", ipdev)
                if t not in template:
                    template.append(t)
            else:
                template += [
                    ("ip#0", "ipdev@"+node, ipdev),
                ]
        svc = factory("svc")("vip", namespace="system", node=NODE)
        existed = svc.exists()
        kws = []
        changes = []
        current = svc.print_config_data()
        for section, keyword, value in template:
            kws.append("%s.%s" % (section, keyword))
            try:
                val = svc._get("%s.%s" % (section, keyword))
            except (ex.OptNotFound, ex.RequiredOptNotFound):
                val = None
            if val != value:
                changes.append("%s.%s=%s" % (section, keyword, value))
        extraneous = []
        for kw in svc.print_config_data().get("ip#0", {}):
            if kw == "tags":
                # never discard tag customization (ex: tags=noaction)
                continue
            _kw = "ip#0."+kw
            if _kw not in kws:
                extraneous.append(_kw)
        if changes:
            for k in changes:
                self.log.info("set %s: %s", svc.path, k)
            svc.set_multi(changes, validation=False)
        if extraneous:
            for k in extraneous:
                self.log.info("unset %s: %s (undue)", svc.path, k)
            svc.unset_multi(extraneous)
        if not existed:
            self.set_smon(svc.path, global_expect="provisioned")
        return svc

    def get_node(self):
        """
        helper for the comm module to find the Node(), for accessing
        its configuration.
        """
        return NODE

    def _daemon_status(self):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        data = {
            "pid": DAEMON.pid,
            "cluster": {
                "name": self.cluster_name,
                "id": self.cluster_id,
                "nodes": self.cluster_nodes,
            }
        }
        for thr_id in list(THREADS):
            try:
                data[thr_id] = THREADS[thr_id].status()
            except KeyError:
                continue
        return data

    def update_daemon_status(self):
        global LAST_DAEMON_STATUS
        global DAEMON_STATUS
        global EVENT_Q
        global PATCH_ID
        LAST_DAEMON_STATUS = json.loads(json.dumps(DAEMON_STATUS))
        DAEMON_STATUS = self._daemon_status()
        diff = json_delta.diff(
            LAST_DAEMON_STATUS, DAEMON_STATUS,
            verbose=False, array_align=False, compare_lengths=False
        )
        if not diff:
            return
        PATCH_ID += 1
        EVENT_Q.put({
            "kind": "patch",
            "id": PATCH_ID,
            "ts": time.time(),
            "data": diff,
        })

    def daemon_status(self):
        return json.loads(json.dumps(LAST_DAEMON_STATUS))

    def filter_daemon_status(self, data, namespace=None, namespaces=None, selector=None):
        if selector is None:
            selector = "**"
        keep = self.object_selector(selector=selector, namespace=namespace, namespaces=namespaces)
        for node in [n for n in data.get("monitor", {}).get("nodes", {})]:
            for path in [p for p in data["monitor"]["nodes"][node].get("services", {}).get("status", {})]:
                if path not in keep:
                    del data["monitor"]["nodes"][node]["services"]["status"][path]
            for path in [p for p in data["monitor"]["nodes"][node].get("services", {}).get("config", {})]:
                if path not in keep:
                    del data["monitor"]["nodes"][node]["services"]["config"][path]
        for path in [p for p in data.get("monitor", {}).get("services", {})]:
            if path not in keep:
                del data["monitor"]["services"][path]
        return data

    def match_object_selector(self, selector=None, namespace=None, namespaces=None, path=None):
        if selector is None:
            selector = "**"
        return path in self.object_selector(selector=selector, namespace=namespace, namespaces=namespaces, paths=[path])

    def object_selector(self, selector=None, namespace=None, namespaces=None, paths=None):
        if not selector:
            return []
        if namespace:
            if namespaces is not None and namespace not in namespaces:
                return []
            # noinspection PySetFunctionToLiteral
            namespaces = set([namespace])
        if "root" in namespaces:
            namespaces.add(None)

        if paths is None:
            # all objects
            paths = [p for p in AGG if split_path(p)[1] in namespaces]
        if selector == "**":
            return paths

        # all services
        if selector == "*":
            return [p for p in paths if split_path(p)[2] == "svc"]

        def or_fragment_selector(s):
            expanded = []
            for _selector in s.split(","):
                for p in and_fragment_selector(_selector):
                    if p in expanded:
                        continue
                    expanded.append(p)
            return expanded

        def and_fragment_selector(s):
            expanded = None
            for _selector in s.split("+"):
                _expanded = fragment_selector(_selector)
                if expanded is None:
                    expanded = _expanded
                else:
                    expanded = [p for p in expanded if p in _expanded]
            return expanded

        def fragment_selector(s):
            # empty
            if not s:
                return []

            # explicit object path
            if s in AGG:
                if s not in paths:
                    return []
                return [s]

            # fnmatch expression
            ops = r"(<=|>=|<|>|=|~|:)"
            negate = s[0] == "!"
            s = s.lstrip("!")
            elts = re.split(ops, s)
            if len(elts) == 1:
                norm_paths = normalize_paths(paths)
                norm_elts = s.split("/")
                norm_elts_count = len(norm_elts)
                if norm_elts_count == 3:
                    _namespace, _kind, _name = norm_elts
                    if not _name:
                        # test/svc/
                        _name = "*"
                elif norm_elts_count == 2:
                    if not norm_elts[1]:
                        # svc/
                        _name = "*"
                        _kind = norm_elts[0]
                        _namespace = "*"
                    elif norm_elts[1] == "**":
                        # prod/**
                        _name = "*"
                        _kind = "*"
                        _namespace = norm_elts[0]
                    elif norm_elts[0] == "**":
                        # **/s*
                        _name = norm_elts[1]
                        _kind = "*"
                        _namespace = "*"
                    else:
                        # svc/s*
                        _name = norm_elts[1]
                        _kind = norm_elts[0]
                        _namespace = "*"
                elif norm_elts_count == 1:
                    if norm_elts[0] == "**":
                        _name = "*"
                        _kind = "*"
                        _namespace = "*"
                    else:
                        _name = norm_elts[0]
                        _kind = "svc"
                        _namespace = namespace if namespace else "root"
                else:
                    return []
                _selector = "/".join((_namespace, _kind, _name))
                filtered_paths = [path for path in norm_paths if negate ^ fnmatch.fnmatch(path, _selector)]
                return [re.sub("^(root/svc/|root/)", "", path) for path in filtered_paths]
            elif len(elts) != 3:
                return []

            param, op, value = elts
            if op in ("<", ">", ">=", "<="):
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    return []

            expanded = []

            if param.startswith("."):
                param = "$"+param
            if param.startswith("$."):
                jsonpath_expr = parse(param)
            else:
                jsonpath_expr = None

            for path in paths:
                ret = svc_matching(path, param, op, value, jsonpath_expr)
                if ret ^ negate:
                    expanded.append(path)

            return expanded

        def matching(current, op, value):
            if op in ("<", ">", ">=", "<="):
                try:
                    current = float(current)
                except (ValueError, TypeError):
                    return False
            if op == "=":
                if current.lower() in ("true", "false"):
                    match = current.lower() == value.lower()
                else:
                    match = current == value
            elif op == "~":
                match = re.search(value, current)
            elif op == ">":
                match = current > value
            elif op == ">=":
                match = current >= value
            elif op == "<":
                match = current < value
            elif op == "<=":
                match = current <= value
            elif op == ":":
                match = True
            else:
                # unknown op value
                match = False
            return match

        def svc_matching(path, param, op, value, jsonpath_expr):
            if param.startswith("$."):
                try:
                    data = self.object_data(path)
                    matches = jsonpath_expr.find(data)
                    for match in matches:
                        current = match.value
                        if matching(current, op, value):
                            return True
                except Exception:
                    pass
            else:
                try:
                    svc = SERVICES[path]
                except KeyError:
                    return False
                try:
                    current = svc._get(param, evaluate=True)
                except (ex.Error, ex.OptNotFound, ex.RequiredOptNotFound):
                    current = None
                if current is None:
                    if "." in param:
                        group, _param = param.split(".", 1)
                    else:
                        group = param
                        _param = None
                    rids = [section for section in svc.conf_sections() if group == "" or section.split('#')[0] == group]
                    if op == ":" and len(rids) > 0 and _param is None:
                        return True
                    elif _param:
                        for rid in rids:
                            try:
                                _current = svc._get(rid+"."+_param, evaluate=True)
                            except (ex.Error, ex.OptNotFound, ex.RequiredOptNotFound):
                                continue
                            if matching(_current, op, value):
                                return True
                    return False
                if current is None:
                    return op == ":"
                if matching(current, op, value):
                    return True
            return False

        expanded = or_fragment_selector(selector)
        return expanded

    def object_data(self, path):
        """
        Extract from the cluster data the structures refering to a
        path.
        """
        try:
            data = AGG[path]
            data["nodes"] = {}
        except KeyError:
            return
        for node in self.cluster_nodes:
            try:
                data["nodes"][node] = {
                    "status": CLUSTER_DATA[node]["services"]["status"][path],
                    "config": CLUSTER_DATA[node]["services"]["config"][path],
                }
            except KeyError:
                pass
        return data
