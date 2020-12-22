"""
A module to share variables used by osvcd threads.
"""
import os
import threading
import time
import fnmatch
import hashlib
import json
import tempfile
import shutil
from copy import deepcopy
from subprocess import Popen, PIPE

import foreign.six as six
# noinspection PyUnresolvedReferences
from foreign.six.moves import queue

import core.exceptions as ex
from foreign.jsonpath_ng.ext import parse
from env import Env
from utilities.journaled_data import JournaledData
from utilities.lazy import lazy, unset_lazy
from utilities.naming import split_path, paths_data, factory, object_path_glob
from utilities.selector import selector_config_match, selector_value_match, selector_parse_fragment, selector_parse_op_fragment
from utilities.storage import Storage
from core.freezer import Freezer
from core.comm import Crypt
from .events import EVENTS


class OsvcJournaledData(JournaledData):
    def __init__(self):
        super(OsvcJournaledData, self).__init__(
            event_q=EVENT_Q,
            journal_head=["monitor", "nodes", Env.nodename],
            journal_exclude=[
                ["gen"],
                ["updated"],
            ],
            # disable journaling if we have no peer, as nothing purges the journal
            journal_condition=lambda: bool(LOCAL_GEN),
        )


# import utilities.dbglock
# RLock = utilities.dbglock.RLock
RLock = threading.RLock

# a global to store the Daemon() instance
DAEMON = None

# the event queue to feed to clients listening for changes
EVENT_Q = queue.Queue()

# daemon_status data
DAEMON_STATUS = OsvcJournaledData()

# disable orchestration if a peer announces a different compat version than
# ours
COMPAT_VERSION = 10

# expose api handlers version
API_VERSION = 6

# node and cluster conf lock to block reading changes during a multi-write
# transaction (ex daemon join)
CONFIG_LOCK = RLock()

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

# The encrypted message all the heartbeat tx threads send.
# It is refreshed in the monitor thread loop.
HB_MSG = None
HB_MSG_LEN = 0
HB_MSG_LOCK = RLock()

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

# The lock to serialize data updates from rx threads
RX = queue.Queue()
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
    CONFIG_LOCK.name = "CONFIG"
    THREADS_LOCK.name = "THREADS"
    NODE_LOCK.name = "NODE"
    SERVICES_LOCK.name = "SERVICES"
    HB_MSG_LOCK.name = "HB_MSG"
    RUN_DONE_LOCK.name = "RUN_DONE"
    LOCKS_LOCK.name = "LOCKS"
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
    if immediate and reason:
        reason += " (immediate)"
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

        self.daemon_status_data = DAEMON_STATUS
        self.node_data = self.daemon_status_data.view(["monitor", "nodes", Env.nodename])
        self.nodes_data = self.daemon_status_data.view(["monitor", "nodes"])
        self.instances_status_data = self.node_data.view(["services", "status"])
        self.thread_data = self.daemon_status_data.view([self.name])

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
                  cmd=None, session_id=None):
        """
        Enqueue a structure including a Popen() result and the success and
        error callbacks.
        """
        self.procs.append(Storage({
            "proc": proc,
            "cmd": cmd,
            "session_id": session_id,
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
        try:
            ProcessLookupError
        except NameError:
            ProcessLookupError = OSError
        for data in self.procs:
            # noinspection PyUnboundLocalVariable
            if hasattr(data.proc, "poll"):
                # subprocess.Popen()
                ret = lambda: data.proc.returncode
                poll = lambda: data.proc.poll()
                comm = lambda: data.proc.communicate()
                kill = lambda: data.proc.kill()
            else:
                # multiprocessing.Process()
                ret = lambda: data.proc.exitcode
                poll = lambda: None
                comm = lambda: None
                kill = lambda: data.proc.terminate()
            try:
                kill()
            except ProcessLookupError:  # pylint: disable=undefined-variable
                continue
            for _ in range(self.stop_tmo):
                if ret() is not None:
                    comm()
                    poll()
                    break
                time.sleep(1)

    def janitor_procs(self):
        done = []
        for idx, data in enumerate(self.procs):
            try:
                # subprocess.Popen()
                data.proc.poll()
                ret = data.proc.returncode
                comm = lambda: data.proc.communicate()
            except AttributeError:
                # multiprocessing.Process()
                ret = data.proc.exitcode
                comm = lambda: None
            if ret is not None:
                comm()
                done.append(idx)
                if ret == 0 and data.on_success:
                    getattr(self, data.on_success)(*data.on_success_args,
                                                   **data.on_success_kwargs)
                elif ret != 0 and data.on_error:
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
        return len(done)

    @lazy
    def freezer(self):
        return Freezer("node")

    def node_busy(self):
        return self.get_node_monitor().status not in (None, "idle")

    def instances_busy(self):
        data = self.node_data.get(["services", "status"])
        for path, sdata in data.items():
            status = sdata.get("monitor", {}).get("status") 
            if status not in (None, "idle") and "failed" not in status:
                return True
        return False
            
    def reload_config(self):
        if not self._node_conf_event.is_set():
            return
        if self.node_busy():
            return
        if self.instances_busy():
            return
        self._node_conf_event.clear()
        self.event("node_config_change")
        unset_lazy(self, "config")
        unset_lazy(self, "quorum")
        unset_lazy(self, "split_action")
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
            self.log.error("reconfigure error: %s, stopping thread", str(exc))
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
                if _patch[0][3] == "labels":
                    return True
            except KeyError:
                continue
            try:
                if _patch[0][3] == "targets":
                    return True
            except KeyError:
                continue
        return False

    def set_nmon(self, status=None, local_expect=None, global_expect=None):
        changed = False
        nmon = self.get_node_monitor()
        if status:
            if status != nmon.status:
                self.log.info(
                    "node monitor status change: %s => %s",
                    nmon.status if
                    nmon.status else "none",
                    status
                )
                changed = True
                nmon.status = status
                nmon.status_updated = time.time()
                nmon.global_expect_updated = time.time()

        if local_expect:
            if local_expect == "unset":
                local_expect = None
            if local_expect != nmon.local_expect:
                self.log.info(
                    "node monitor local expect change: %s => %s",
                    nmon.local_expect if
                    nmon.local_expect else "none",
                    local_expect
                )
                changed = True
                nmon.local_expect = local_expect

        if global_expect:
            if global_expect == "unset":
                global_expect = None
            if global_expect != nmon.global_expect:
                self.log.info(
                    "node monitor global expect change: %s => %s",
                    nmon.global_expect if
                    nmon.global_expect else "none",
                    global_expect
                )
                changed = True
                nmon.global_expect = global_expect
                nmon.global_expect_updated = time.time()

        if changed:
            self.node_data.set(["monitor"], nmon)
            wake_monitor(reason="node mon change")

    def set_smon(self, path, status=None, local_expect=None,
                 global_expect=None, reset_retries=False,
                 stonith=None, expected_status=None):
        instance = self.get_service_instance(path, Env.nodename)
        if not instance:
            self.node_data.set(["services", "status", path], {"resources": {}})
        smon_view = self.node_data.view(["services", "status", path, "monitor"])
        smon = Storage(smon_view.get([], default={"status": "idle"}))
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
        if not smon:
            smon_view.set([], {
                "status": "idle",
                "status_updated": time.time(),
                "global_expect_updated": time.time(),
            })
            changed = True
        if status:
            reset_placement = False
            if status != smon.status \
                    and (not expected_status or expected_status == smon.status):
                self.log.info(
                    "%s monitor status change: %s => %s",
                    path,
                    smon.status if
                    smon.status else "none",
                    status
                )
                if smon.status is not None \
                        and "failed" in smon.status \
                        and "failed" not in status:
                    # the placement might become "leader" after transition
                    # from "failed" to "not-failed". recompute asap so the
                    # orchestrator won't take an undue "stop_instance"
                    # decision.
                    reset_placement = True
                smon.status = status
                smon.status_updated = time.time()
                changed = True
            if reset_placement:
                smon.placement = self.get_service_placement(path)
                smon.status_updated = time.time()
                changed = True

        if local_expect:
            if local_expect == "unset":
                local_expect = None
            if local_expect != smon.local_expect:
                self.log.info(
                    "%s monitor local expect change: %s => %s",
                    path,
                    smon.local_expect if
                    smon.local_expect else "none",
                    local_expect
                )
                smon.local_expect = local_expect
                changed = True

        if global_expect:
            if global_expect == "unset":
                global_expect = None
            if global_expect != smon.global_expect:
                self.log.info(
                    "%s monitor global expect change: %s => %s",
                    path,
                    smon.global_expect if
                    smon.global_expect else "none",
                    global_expect
                )
                smon.global_expect = global_expect
                smon.global_expect_updated = time.time()
                changed = True
        if reset_retries and "restart" in smon:
            self.log.info("%s monitor resources restart count "
                          "reset", path)
            del smon["restart"]
            changed = True

        if stonith:
            if stonith == "unset":
                stonith = None
            if stonith != smon.stonith:
                self.log.info(
                    "%s monitor stonith change: %s => %s",
                    path,
                    smon.stonith if
                    smon.stonith else "none",
                    stonith
                )
                smon.stonith = stonith
                changed = True
        if changed:
            smon_view.set([], smon)
            wake_monitor(reason="%s mon change" % path)

    def get_node_monitor(self, nodename=None):
        """
        Return the Monitor data of the node.
        """
        nodename = nodename or Env.nodename
        return Storage(self.daemon_status_data.get(["monitor", "nodes", nodename, "monitor"], default={}))

    def iter_service_monitors(self, path, nodenames=None):
        for _, nodename, data in self.iter_services_monitors(paths=[path], nodenames=nodenames):
            yield (nodename, data)

    def iter_local_services_monitors(self):
        for path, _, data in self.iter_services_monitors(nodenames=[Env.nodename]):
            yield (path, data)

    def iter_services_monitors(self, paths=None, nodenames=None):
        for path, nodename, data in self.iter_services_instances(paths=paths, nodenames=nodenames):
            try:
                yield (path, nodename, Storage(data.monitor))
            except (TypeError, KeyError):
                continue

    def iter_local_services_instances(self):
        for path, _, data in self.iter_services_instances(nodenames=[Env.nodename]):
            yield (path, data)

    def iter_service_instances(self, path, nodenames=None):
        for _, nodename, data in self.iter_services_instances(paths=[path], nodenames=nodenames):
            yield (nodename, data)

    def iter_services_instances(self, paths=None, nodenames=None):
        nodenames = nodenames or self.cluster_nodes
        for nodename in nodenames:
            for path in self.daemon_status_data.keys_safe(["monitor", "nodes", nodename, "services", "status"]):
                if paths and path not in paths:
                    continue
                try:
                    yield (path, nodename, Storage(self.daemon_status_data.get(["monitor", "nodes", nodename, "services", "status", path])))
                except KeyError:
                    continue

    def iter_services_configs(self, paths=None, nodenames=None):
        nodenames = nodenames or self.cluster_nodes
        for nodename in nodenames:
            for path in self.daemon_status_data.keys_safe(["monitor", "nodes", nodename, "services", "config"]):
                if paths and path not in paths:
                    continue
                try:
                    yield (path, nodename, Storage(self.daemon_status_data.get(["monitor", "nodes", nodename, "services", "config", path])))
                except KeyError:
                    continue

    def iter_nodes(self, nodenames=None):
        nodenames = nodenames or self.cluster_nodes
        for nodename in nodenames:
            try:
                yield (nodename, self.daemon_status_data.get(["monitor", "nodes", nodename]))
            except KeyError:
                continue

    def iter_nodes_monitor(self, nodenames=None):
        nodenames = nodenames or self.cluster_nodes
        for nodename in nodenames:
            try:
                yield (nodename, self.daemon_status_data.get(["monitor", "nodes", nodename, "monitor"]))
            except KeyError:
                continue

    def get_service_monitor(self, path):
        """
        Return the Monitor data of a service.
        """
        try:
            data = Storage(self.node_data.get(["services", "status", path, "monitor"]))
            data.placement = self.get_service_placement(path)
        except KeyError:
            data = Storage({
                "status": "idle",
                "placement": self.get_service_placement(path),
            })
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
        A generic Popen wrapper logging the begining of execution.
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
        A generic node command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        _cmd = [] + Env.om
        cmd = ["node"] + cmd
        self.log.info("execute: om %s", " ".join(cmd))
        proc = Popen(_cmd+cmd, stdout=None, stderr=None, stdin=None,
                     close_fds=True, env=env)
        return proc

    def service_command(self, path, cmd, stdout=None, stderr=None, stdin=None, local=True):
        """
        A generic object command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        _cmd = [] + Env.om
        if path:
            cmd = ["svc", "-s", path] + cmd
        else:
            cmd = ["svc"] + cmd
        if local and "--local" not in cmd:
            cmd += ["--local"]
        self.log.info("execute: om %s", " ".join(cmd))
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
        if not nodename:
            self.log.warning('add_cluster_node called with empty nodename')
            return
        NODE.unset_lazy("cd")
        NODE.unset_lazy("private_cd")
        unset_lazy(self, "cluster_nodes")
        unset_lazy(self, "sorted_cluster_nodes")
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
        if not nodename:
            self.log.warning('remove_cluster_node called with empty nodename')
            return
        NODE.unset_lazy("cd")
        NODE.unset_lazy("private_cd")
        unset_lazy(self, "cluster_nodes")
        unset_lazy(self, "sorted_cluster_nodes")
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
        self.delete_peer_data(nodename)
        del svc

    @lazy
    def quorum(self):
        return NODE.oget("cluster", "quorum")

    @lazy
    def split_action(self):
        return NODE.oget("node", "split_action")

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

    def live_nodes_count(self):
        return len(self.daemon_status_data.keys(["monitor", "nodes"]))

    @staticmethod
    def arbitrators_config_count():
        return len(NODE.arbitrators)

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
        total = len(self.cluster_nodes) + self.arbitrators_config_count()
        live = self.live_nodes_count()
        extra_votes = self.arbitrators_votes()
        n_extra_votes = len(extra_votes)
        if live + n_extra_votes > total / 2:
            self.duplog("info", "cluster is split, we have quorum: "
                        "%(live)d+%(avote)d out of %(total)d votes (%(a)s)",
                        live=live, avote=n_extra_votes, total=total,
                        a=",".join(extra_votes))
            return
        self.event(self.split_action, {
            "reason": "split",
            "node_votes": live,
            "arbitrator_votes": n_extra_votes,
            "voting": total,
            "pro_voters": self.list_nodes() + extra_votes,
        })
        # give a little time for log flush
        NODE.suicide(method=self.split_action, delay=2)

    def forget_peer_data(self, nodename, change=False, origin=None):
        """
        Purge a stale peer data if all rx threads are down.
        """
        if not self.nodes_data.exists([nodename]):
            return
        nmon = self.get_node_monitor(nodename=nodename)
        if not self.peer_down(nodename, exclude=[origin]):
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
        self.delete_peer_data(nodename)
        wake_monitor(reason="forget node %s data" % nodename)
        if nmon_status == "shutting":
            self.log.info("cluster is not split, the lost node %s last known "
                          "monitor state is '%s'", nodename, nmon_status)
        else:
            self.split_handler()

    def delete_peer_data(self, nodename):
        self.log.info('delete node %s from nodes data', nodename)
        with RX_LOCK:
            self.nodes_data.unset_safe([nodename])
            try:
                del LOCAL_GEN[nodename]
            except KeyError:
                pass
            try:
                # will ask for a full when the node comes back again
                del REMOTE_GEN[nodename]
            except KeyError:
                pass

    def peer_down(self, nodename, exclude=None):
        """
        Return True if no rx threads receive data from the specified peer
        node.
        """
        exclude = exclude or []
        for thr_id in list(THREADS):
            if not thr_id.endswith(".rx"):
                continue
            if thr_id in exclude:
                continue
            try:
                peer_status = self.daemon_status_data.get([thr_id, "peers", nodename, "beating"])
            except KeyError:
                continue
            if peer_status:
                return False
        return True

    def get_service_config(self, path, nodename):
        """
        Return the specified object status structure on the specified node.
        """
        try:
            return Storage(
                self.nodes_data.get([nodename, "services", "config", path])
            )
        except (TypeError, KeyError):
            return

    def get_service_instance(self, path, nodename):
        """
        Return the specified object status structure on the specified node.
        """
        data = self.daemon_status_data.get(["monitor", "nodes", nodename, "services", "status", path], None)
        if data is None:
            return
        return Storage(data)

    def get_service_instances(self, path, discard_empty=False):
        """
        Return the specified object status structures on all nodes.
        """
        instances = {}
        for nodename, instance in self.iter_service_instances(path):
            if not instance.updated:
                # foreign
                continue
            if discard_empty and not instance:
                continue
            instances[nodename] = instance
        return instances

    def get_service_nodes(self, path):
        return [n for (n, _) in self.iter_service_instances(path)]

    def list_nodes(self):
        return self.daemon_status_data.keys_safe(["monitor", "nodes"])

    def list_cluster_paths(self):
        paths = set()
        for path, _, _ in self.iter_services_configs():
            paths.add(path)
        return paths

    def get_service_agg(self, path):
        """
        Return the specified object aggregated status structure.
        """
        return Storage(self.daemon_status_data.get(["monitor", "services", path], default={}))

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
                             discard_start_failed=True,
                             discard_affinities=True):
        """
        Return the list of object nodes meeting the following criteria:
        * we have valid instance data (not unknown, has avail)
        * the node is not in maintenance, shutting, init or upgrade (default)
        * the node is not frozen (default)
        * the node is not overloaded (default)
        * the object is not frozen (default)
        * the instance is provisioned (default)
        * the instance smon status is not "start failed" (default)
        * the instance constraints are eval'ed True (default)
        """
        def discard_hard_affinity(nodename):
            if not svc.hard_affinity:
                return False
            for path in svc.hard_affinity:
                try:
                    status = self.nodes_data.get([nodename, "services", "status", path, "avail"])
                except KeyError:
                    continue
                if status != "up":
                    return True
            return False

        def discard_hard_anti_affinity(nodename):
            if not svc.hard_anti_affinity:
                return False
            for path in svc.hard_affinity:
                try:
                    status = self.nodes_data.get([nodename, "services", "status", path, "avail"])
                except KeyError:
                    continue
                if status == "up":
                    return True
            return False

        candidates = []
        if svc is None:
            return []
        for nodename in svc.peers:
            nmon = self.get_node_monitor(nodename)
            if not nmon:
                continue
            if discard_preserved and nmon.status in NMON_STATES_PRESERVED:
                continue
            try:
                frozen = self.daemon_status_data.get(["monitor", "nodes", nodename, "frozen"])
            except KeyError:
                continue
            if discard_frozen and frozen:
                # node frozen
                continue
            instance = self.get_service_instance(svc.path, nodename)
            if instance is None:
                continue
            if discard_na and instance.avail == "n/a":
                continue
            if discard_start_failed and \
               instance.get("monitor", {}).get("status") in (
                   "start failed",
                   "place failed"
               ):
                continue
            if "avail" not in instance:
                # deleting
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
            if discard_affinities:
                if discard_hard_affinity(nodename):
                    continue
                if discard_hard_anti_affinity(nodename):
                    continue
            candidates.append(nodename)

        return candidates

    def placement_ranks(self, svc, candidates=None):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        try:
            return getattr(self, self.rankers[svc.placement])(svc, candidates)
        except AttributeError:
            return [Env.nodename]

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
                            "%(path)s on any node",
                            path=svc.path)
            return False
        if Env.nodename not in candidates:
            if not silent:
                self.duplog("info",
                            "placement constraints prevent us from starting "
                            "%(path)s on this node",
                            path=svc.path)
            return False
        if len(candidates) == 1:
            if not silent:
                self.duplog("info",
                            "we have the greatest placement priority for "
                            "%(path)s (alone)",
                            path=svc.path)
            return True

        ranks = self.placement_ranks(svc, candidates=candidates)
        if ranks == []:
            return False
        elif svc.topology == "failover":
            if Env.nodename == ranks[0]:
                if not silent:
                    self.duplog("info",
                                "we have the highest '%(placement)s' "
                                "placement priority for failover "
                                "%(path)s",
                                placement=svc.placement, path=svc.path)
                return True
            else:
                if not silent:
                    self.duplog("info",
                                "node %(nodename)s is alive and has a higher "
                                "'%(placement)s' placement priority for "
                                "failover %(path)s",
                                nodename=ranks[0], placement=svc.placement,
                                path=svc.path)
                return False
        elif svc.topology == "flex":
            index = ranks.index(Env.nodename) + 1
            if not silent:
                self.duplog("info",
                            "we have the %(idx)d/%(tgt)d '%(placement)s' "
                            "placement priority for flex %(path)s",
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
        for nodename in candidates:
            try:
                load = self.daemon_status_data.get(["monitor", "nodes", nodename, "stats", "score"])
            except KeyError:
                continue
            data.append((nodename, load))
        return [nodename for nodename, _ in sorted(data, key=lambda x: -x[1])]

    def placement_ranks_load_avg(self, svc, candidates, silent=False):
        data = []
        for nodename in candidates:
            try:
                load = self.daemon_status_data.get(["monitor", "nodes", nodename, "stats", "load_15m"])
            except KeyError:
                try:
                    load = self.daemon_status_data.get(["monitor", "nodes", nodename, "load", "15m"])
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
        for nodename, nmon in self.iter_nodes_monitor():
            if nmon:
                return nodename

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
        gen = {Env.nodename: GEN}
        gen.update(REMOTE_GEN)
        return gen

    def node_overloaded(self, nodename=None):
        nodename = nodename or Env.nodename
        for key in ("mem", "swap"):
            limit = self.daemon_status_data.get(["monitor", "nodes", nodename, "min_avail_" + key], default=0)
            total = self.daemon_status_data.get(["monitor", "nodes", nodename, "stats", key + "_total"], default=0)
            val = self.daemon_status_data.get(["monitor", "nodes", nodename, "stats", key + "_avail"], default=0)
            if total > 0 and val < limit:
                return True
        return False

    def nodes_info(self):
        data = {}
        for nodename in self.cluster_nodes:
            data[nodename] = {
                "labels": self.daemon_status_data.get(["monitor", "nodes", nodename, "labels"], default={}),
                "targets": self.daemon_status_data.get(["monitor", "nodes", nodename, "targets"], default={}),
            }
        return data

    def dump_nodes_info(self):
        try:
            with open(Env.paths.nodes_info, "r") as ofile:
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
            return False
        try:
            tmpf = tempfile.NamedTemporaryFile(delete=False, dir=Env.paths.pathtmp)
            fpath = tmpf.name
            tmpf.close()
            with open(fpath, "w") as ofile:
                json.dump(new_data, ofile)
            shutil.move(fpath, Env.paths.nodes_info)
        except Exception as exc:
            self.alert("warning", "failed to refresh %s: %s", Env.paths.nodes_info, exc)
        self.log.info("%s updated", Env.paths.nodes_info)
        return True

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
        changed = self.dump_nodes_info()
        if not changed:
            return
        for path in [p for p in SERVICES]:
            try:
                svc = SERVICES[path]
            except KeyError:
                # deleted
                continue
            svc.unset_conf_lazy()
            if self.get_node_monitor().status != "init":
                smon = self.daemon_status_data.get(["monitor", "nodes", Env.nodename, "services", "status", path, "monitor"], None)
                if not smon:
                    continue
                try:
                    # trigger status.json reload by the mon thread
                    data = svc.print_status_data_eval(refresh=False, write_data=True, clear_rstatus=True)
                    data["monitor"] = smon
                    self.daemon_status_data.set(["monitor", "nodes", Env.nodename, "services", "status", path], data)
                except Exception as exc:
                    self.log.error("on nodes info change, object %s status refresh:", path)
                    self.log.exception(exc)
        wake_monitor(reason="nodes info change")

    def speaker(self):
        return self.first_available_node() == Env.nodename

    def event(self, eid, data=None, log_data=None, level="info"):
        """
        Put an "event"-kind event in the events queue, then log in node.log
        and in the service.log if a path is provided in <data>. If a
        <log_data> is passed, merge it in <data> before formatting the messages
        to log.
        """
        evt = {
            "nodename": Env.nodename,
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
            data["instance"] = self.get_service_instance(path, Env.nodename) or Storage()
            data["instance"].monitor = Storage(data["instance"].get("monitor", {}))
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

    def update_cluster_data(self):
        self.daemon_status_data.set(["cluster"], {
            "name": self.cluster_name,
            "id": self.cluster_id,
            "nodes": self.cluster_nodes,
        })

    def update_cluster_locks_lk(self):
        # this need protection with LOCKS_LOCK
        self.node_data.merge([], {"locks": deepcopy(LOCKS)})

    def update_status(self):
        data = self.status()
        self.thread_data.set([], data)

    def daemon_status(self):
        data = self.daemon_status_data.get_copy()
        return data

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

    def object_selector(self, selector=None, namespace=None, namespaces=None, kind=None, paths=None):
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
            paths = self.list_cluster_paths()
        pds = paths_data(paths)
        pds = [pd for pd in pds if pd["namespace"] in namespaces]
        if kind:
            pds = [pd for pd in pds if pd["kind"] == kind]
        if selector == "**":
            return [pd["display"] for pd in pds]

        # all services
        if selector == "*":
            kind = kind or "svc"
            return [pd["display"] for pd in pds if pd["kind"] == kind]

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

        def selector_parse_jsonpath_expr(param):
            if param.startswith("."):
                param = "$"+param

            if param.startswith("$."):
                jsonpath_expr = parse(param)
            else:
                jsonpath_expr = None
            return jsonpath_expr

        def fragment_selector(s):
            # empty
            if not s:
                return []

            # explicit object path
            if s in self.list_cluster_paths():
                if s not in paths:
                    return []
                return [s]

            # fnmatch expression
            negate, s, elts = selector_parse_fragment(s)

            if len(elts) == 1:
                return object_path_glob(s, pds=pds, namespace=namespace, kind=kind, negate=negate)

            try:
                param, op, value = selector_parse_op_fragment(elts)
            except ValueError:
                return []

            expanded = []
            jsonpath_expr = selector_parse_jsonpath_expr(param)

            for path in paths:
                ret = svc_matching(path, param, op, value, jsonpath_expr)
                if ret ^ negate:
                    expanded.append(path)

            return expanded

        def selector_status_matching(path, jsonpath_expr, op, value):
            try:
                data = self.object_data(path)
                matches = jsonpath_expr.find(data)
                for match in matches:
                    current = match.value
                    if selector_value_match(current, op, value):
                        return True
            except Exception:
                return False
            return False

        def svc_matching(path, param, op, value, jsonpath_expr):
            if jsonpath_expr:
                return selector_status_matching(path, jsonpath_expr, op, value)
            else:
                try:
                    svc = SERVICES[path]
                except KeyError:
                    return False
                return selector_config_match(svc, param, op, value)

        expanded = or_fragment_selector(selector)
        return expanded

    def object_data(self, path):
        """
        Extract from the cluster data the structures refering to a
        path.
        """
        try:
            data = self.get_service_agg(path)
            data["nodes"] = {}
        except KeyError:
            return
        for node in self.cluster_nodes:
            try:
                data["nodes"][node] = {
                    "status": self.daemon_status_data.get(["monitor", "nodes", node, "services", "status", path]),
                    "config": self.daemon_status_data.get(["monitor", "nodes", node, "services", "config", path]),
                }
            except KeyError:
                pass
        return data
