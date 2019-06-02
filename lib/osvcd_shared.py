"""
A module to share variables used by osvcd threads.
"""
import os
import sys
import threading
import time
import codecs
import hashlib
import json
from subprocess import Popen, PIPE

import six
from six.moves import queue

import rcExceptions as ex
from rcUtilities import lazy, unset_lazy, is_string, factory
from rcGlobalEnv import rcEnv
from storage import Storage
from freezer import Freezer
from converters import convert_duration, convert_boolean
from comm import Crypt
from osvcd_events import EVENTS

# a global to store the Daemon() instance
DAEMON = None

# disable orchestration if a peer announces a different compat version than
# ours
COMPAT_VERSION = 8

# node and cluster conf lock to block reading changes during a multi-write
# transaction (ex daemon join)
CONFIG_LOCK = threading.RLock()

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
THREADS_LOCK = threading.RLock()

# A node object instance. Used to access node properties and methods.
NODE = None
NODE_LOCK = threading.RLock()

# CRM services objects. Used to access services properties.
# The monitor thread reloads a new Svc object when the corresponding
# configuration file changes.
SERVICES = {}
SERVICES_LOCK = threading.RLock()

# Per service aggretated data of instances.
# Refresh on monitor status eval, and embedded in the returned data
AGG = {}
AGG_LOCK = threading.RLock()

# The encrypted message all the heartbeat tx threads send.
# It is refreshed in the monitor thread loop.
HB_MSG = None
HB_MSG_LEN = 0
HB_MSG_LOCK = threading.RLock()

# the local service monitor data, where the listener can set expected states
SMON_DATA = {}
SMON_DATA_LOCK = threading.RLock()

# the local node monitor data, where the listener can set expected states
NMON_DATA = Storage({
    "status": "init",
    "status_updated": time.time(),
})
NMON_DATA_LOCK = threading.RLock()

# a boolean flag used to signal the monitor it has to do the long loop asap
MON_CHANGED = []

# cluster wide locks, aquire/release via the listener (usually the unix socket),
# consensus via the heartbeat links.
LOCKS = {}
LOCKS_LOCK = threading.RLock()

# The per-threads configuration, stats and states store
# The monitor thread states include cluster-wide aggregated data
CLUSTER_DATA = {}
CLUSTER_DATA_LOCK = threading.RLock()

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
RUN_DONE_LOCK = threading.RLock()
RUN_DONE = set()

# min interval between thread stats refresh
STATS_INTERVAL = 1

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
        self._stop_event = threading.Event()
        self._node_conf_event = threading.Event()
        self.created = time.time()
        self.threads = []
        self.procs = []
        self.tid = None
        self.stats_data = None
        self.last_stats_refresh = 0

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
        data = Storage({
            "state": state,
            "created": self.created,
        })
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
        except Exception as exc:
            tid_cpu_time = 0.0
        try:
            tid_mem_total = NODE.tid_mem_total(self.tid)
        except Exception as exc:
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
        for data in self.procs:
            data.proc.kill()
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
        while timeout > 0:
            self.janitor_threads()
            if len(self.threads) == 0:
                return
            timeout -= 1
            time.sleep(1)
        self.log.warning("timeout waiting for threads to terminate. %s left alive.", len(self.threads))
        self.log_status()

    def log_status(self):
        from rcColor import format_str_flat_json
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

    @lazy
    def config(self):
        try:
            config = NODE.config
        except Exception as exc:
            self.log.info("error loading config: %s", exc)
            raise ex.excAbortAction()
        return config

    def reload_config(self):
        if not self._node_conf_event.is_set():
            return
        if NMON_DATA.status not in (None, "idle"):
            return
        self._node_conf_event.clear()
        self.event("node_config_change")
        unset_lazy(self, "config")
        unset_lazy(self, "quorum")
        unset_lazy(self, "vip")
        unset_lazy(NODE, "arbitrators")
        unset_lazy(self, "cluster_name")
        unset_lazy(self, "cluster_key")
        unset_lazy(self, "cluster_id")
        unset_lazy(self, "cluster_nodes")
        unset_lazy(self, "sorted_cluster_nodes")
        unset_lazy(self, "maintenance_grace_period")
        unset_lazy(self, "rejoin_grace_period")
        unset_lazy(self, "ready_period")
        self.arbitrators_data = None
        if not hasattr(self, "reconfigure"):
            return
        try:
            getattr(self, "reconfigure")()
        except Exception as exc:
            self.log.error("reconfigure error: %s", str(exc))
            self.stop()

    @staticmethod
    def get_service(svcpath):
        with SERVICES_LOCK:
            if svcpath not in SERVICES:
                return
        return SERVICES[svcpath]

    @staticmethod
    def on_labels_change():
        NODE.unset_lazy("nodes_info")
        for svc in SERVICES.values():
            svc.unset_conf_lazy()

    @staticmethod
    def patch_has_labels_change(patch):
        for _patch in patch:
            try:
                if _patch[0][0] == "labels":
                    return True
            except KeyError:
                continue
        return False

    @staticmethod
    def get_services_nodenames():
        """
        Return the services nodes and drpnodes name, fetching the information
        from the Svc objects.
        """
        nodenames = set()
        with SERVICES_LOCK:
            for svc in SERVICES.values():
                nodenames |= svc.nodes | svc.drpnodes
        return nodenames

    def set_nmon(self, status=None, local_expect=None, global_expect=None):
        global NMON_DATA
        with NMON_DATA_LOCK:
            if status:
                if status != NMON_DATA.status:
                    self.log.info(
                        "node monitor status change: %s => %s",
                        NMON_DATA.status if
                        NMON_DATA.status else "none",
                        status
                    )
                NMON_DATA.status = status
                NMON_DATA.status_updated = time.time()

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
                NMON_DATA.global_expect = global_expect

        wake_monitor(reason="node mon change")

    def set_smon(self, svcpath, status=None, local_expect=None,
                 global_expect=None, reset_retries=False,
                 stonith=None):
        global SMON_DATA
        instance = self.get_service_instance(svcpath, rcEnv.nodename)
        if instance and not instance.get("resources", {}) and \
           not status and \
           (
               global_expect not in (
                   "frozen",
                   "thawed",
                   "aborted",
                   "unset",
                   "deleted",
                   "purged"
               ) or (
                   global_expect is None and local_expect is None and
                   status == "idle"
               )
           ):
            # skip slavers, wrappers, scalers
            return
        with SMON_DATA_LOCK:
            if svcpath not in SMON_DATA:
                SMON_DATA[svcpath] = Storage({
                    "status": "idle",
                    "status_updated": time.time(),
                    "global_expect_updated": time.time(),
                })
            if status:
                reset_placement = False
                if status != SMON_DATA[svcpath].status:
                    self.log.info(
                        "service %s monitor status change: %s => %s",
                        svcpath,
                        SMON_DATA[svcpath].status if
                        SMON_DATA[svcpath].status else "none",
                        status
                    )
                    if SMON_DATA[svcpath].status is not None and \
                       "failed" in SMON_DATA[svcpath].status and \
                       (status is None or "failed" not in status):
                        # the placement might become "leader" after transition
                        # from "failed" to "not-failed". recompute asap so the
                        # orchestrator won't take an undue "stop_instance"
                        # decision.
                        reset_placement = True
                SMON_DATA[svcpath].status = status
                SMON_DATA[svcpath].status_updated = time.time()
                if reset_placement:
                    SMON_DATA[svcpath].placement = \
                        self.get_service_placement(svcpath)

            if local_expect:
                if local_expect == "unset":
                    local_expect = None
                if local_expect != SMON_DATA[svcpath].local_expect:
                    self.log.info(
                        "service %s monitor local expect change: %s => %s",
                        svcpath,
                        SMON_DATA[svcpath].local_expect if
                        SMON_DATA[svcpath].local_expect else "none",
                        local_expect
                    )
                SMON_DATA[svcpath].local_expect = local_expect

            if global_expect:
                if global_expect == "unset":
                    global_expect = None
                if global_expect != SMON_DATA[svcpath].global_expect:
                    self.log.info(
                        "service %s monitor global expect change: %s => %s",
                        svcpath,
                        SMON_DATA[svcpath].global_expect if
                        SMON_DATA[svcpath].global_expect else "none",
                        global_expect
                    )
                SMON_DATA[svcpath].global_expect = global_expect
                SMON_DATA[svcpath].global_expect_updated = time.time()

            if reset_retries and "restart" in SMON_DATA[svcpath]:
                self.log.info("service %s monitor resources restart count "
                              "reset", svcpath)
                del SMON_DATA[svcpath]["restart"]

            if stonith:
                if stonith == "unset":
                    stonith = None
                if stonith != SMON_DATA[svcpath].stonith:
                    self.log.info(
                        "service %s monitor stonith change: %s => %s",
                        svcpath,
                        SMON_DATA[svcpath].stonith if
                        SMON_DATA[svcpath].stonith else "none",
                        stonith
                    )
                SMON_DATA[svcpath].stonith = stonith
        wake_monitor(reason="service %s mon change" % svcpath)

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

    def get_service_monitor(self, svcpath):
        """
        Return the Monitor data of a service.
        """
        with SMON_DATA_LOCK:
            if svcpath not in SMON_DATA:
                self.set_smon(svcpath, "idle")
            data = Storage(SMON_DATA[svcpath])
            data["placement"] = self.get_service_placement(svcpath)
            return data

    def get_service_placement(self, svcpath):
        with SERVICES_LOCK:
            if svcpath not in SERVICES:
                return ""
            svc = SERVICES[svcpath]
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

    def service_command(self, svcpath, cmd, stdin=None, local=True):
        """
        A generic svcmgr command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        _cmd = [] + rcEnv.python_cmd
        _cmd += [os.path.join(rcEnv.paths.pathlib, "svcmgr.py")]
        if svcpath:
            cmd = ["-s", svcpath] + cmd
        if local:
            cmd += ["--local"]
        self.log.info("execute: svcmgr %s", " ".join(cmd))
        if stdin is not None:
            _stdin = PIPE
        else:
            _stdin = None
        proc = Popen(_cmd+cmd, stdout=None, stderr=None, stdin=_stdin,
                     close_fds=True, env=env)
        if stdin:
            proc.stdin.write(stdin.encode())
        return proc

    def add_cluster_node(self, nodename):
        if nodename in self.cluster_nodes:
            return
        nodes = self.cluster_nodes + [nodename]
        config = NODE.get_config(cluster=False)
        if config.has_option("cluster", "nodes"):
            NODE.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
        else:
            from cluster import ClusterSvc
            svc = ClusterSvc()
            svc.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
            del svc

    def remove_cluster_node(self, nodename):
        if nodename not in self.cluster_nodes:
            return
        nodes = [node for node in self.cluster_nodes if node != nodename]
        from cluster import ClusterSvc
        svc = ClusterSvc()
        buff = "cluster.nodes="+" ".join(nodes)
        self.log.info("set %s in cluster config" % buff)
        svc.set_multi(["cluster.nodes="+" ".join(nodes)], validation=False)
        self.log.info("unset cluster.nodes in node config")
        NODE.unset_multi(["cluster.nodes"])
        del svc

    @lazy
    def quorum(self):
        if self.config.has_option("cluster", "quorum"):
            return convert_boolean(self.config.get("cluster", "quorum"))
        else:
            return False

    @lazy
    def maintenance_grace_period(self):
        if self.config.has_option("node", "maintenance_grace_period"):
            return convert_duration(
                self.config.get("node", "maintenance_grace_period")
            )
        else:
            return 60

    @lazy
    def rejoin_grace_period(self):
        if self.config.has_option("node", "rejoin_grace_period"):
            return convert_duration(
                self.config.get("node", "rejoin_grace_period")
            )
        else:
            return 90

    @lazy
    def ready_period(self):
        if self.config.has_option("node", "ready_period"):
            return convert_duration(self.config.get("node", "ready_period"))
        else:
            return 5

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
            for thr_id, thread in THREADS.items():
                if not thr_id.endswith(".rx"):
                    continue
                rx_status = thread.status()
                try:
                    peer_status = rx_status["peers"][nodename]["beating"]
                except KeyError:
                    continue
                if peer_status:
                    return False
        return True

    @staticmethod
    def get_service_instance(svcpath, nodename):
        """
        Return the specified service status structure on the specified node.
        """
        try:
            with CLUSTER_DATA_LOCK:
                return Storage(
                    CLUSTER_DATA[nodename]["services"]["status"][svcpath]
                )
        except (TypeError, KeyError):
            return

    @staticmethod
    def get_service_instances(svcpath, discard_empty=False):
        """
        Return the specified service status structures on all nodes.
        """
        instances = {}
        with CLUSTER_DATA_LOCK:
            for nodename in CLUSTER_DATA:
                try:
                    if svcpath in CLUSTER_DATA[nodename]["services"]["status"]:
                        try:
                            CLUSTER_DATA[nodename]["services"]["status"][svcpath]["updated"]
                        except (TypeError, KeyError):
                            # foreign
                            continue
                        if discard_empty and not CLUSTER_DATA[nodename]["services"]["status"][svcpath]:
                            continue
                        instances[nodename] = CLUSTER_DATA[nodename]["services"]["status"][svcpath]
                except (TypeError, KeyError):
                    continue
        return instances

    @staticmethod
    def get_service_agg(svcpath):
        """
        Return the specified service aggregated status structure.
        """
        try:
            with AGG_LOCK:
                return AGG[svcpath]
        except KeyError:
            return

    #########################################################################
    #
    # Placement policies
    #
    #########################################################################
    def placement_candidates(self, svc, discard_frozen=True,
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
        with CLUSTER_DATA_LOCK:
            for nodename, data in CLUSTER_DATA.items():
                if nodename not in svc.peers:
                    # can happen if the same service is deployed on
                    # differrent cluster segments
                    continue
                if data == "unknown":
                    continue
                if discard_preserved and \
                   data.get("monitor", {}).get("status") in (
                       "maintenance",
                       "upgrade",
                       "init",
                       "shutting",
                   ):
                    continue
                if discard_frozen and data.get("frozen"):
                    # node frozen
                    continue
                instance = self.get_service_instance(svc.svcpath, nodename)
                if instance is None:
                    continue
                if discard_start_failed and \
                   instance["monitor"]["status"] in (
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
            return ranks[0:svc.flex_min_nodes]
        else:
            return []

    def placement_leader(self, svc, candidates=None, silent=False):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        if len(candidates) == 0:
            if not silent:
                self.duplog("info",
                            "placement constraints prevent us from starting "
                            "service %(svcpath)s on any node",
                            svcpath=svc.svcpath)
            return False
        if rcEnv.nodename not in candidates:
            if not silent:
                self.duplog("info",
                            "placement constraints prevent us from starting "
                            "service %(svcpath)s on this node",
                            svcpath=svc.svcpath)
            return False
        if len(candidates) == 1:
            if not silent:
                self.duplog("info",
                            "we have the greatest placement priority for "
                            "service %(svcpath)s (alone)",
                            svcpath=svc.svcpath)
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
                                "%(svcpath)s",
                                placement=svc.placement, svcpath=svc.svcpath)
                return True
            else:
                if not silent:
                    self.duplog("info",
                                "node %(nodename)s is alive and has a higher "
                                "'%(placement)s' placement priority for "
                                "failover service %(svcpath)s",
                                nodename=ranks[0], placement=svc.placement,
                                svcpath=svc.svcpath)
                return False
        elif svc.topology == "flex":
            index = ranks.index(rcEnv.nodename) + 1
            if not silent:
                self.duplog("info",
                            "we have the %(idx)d/%(mini)d '%(placement)s' "
                            "placement priority for flex service %(svcpath)s",
                            idx=index, mini=svc.flex_min_nodes,
                            placement=svc.placement, svcpath=svc.svcpath)
            if index <= svc.flex_min_nodes:
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
        hash together each candidate nodename+svcpath, and sort the resulting
        list.
        """
        def fn(s):
            h = hashlib.md5()
            h.update(s.encode())
            return h.digest()
        return [nodename for nodename in
                sorted(candidates, key=lambda x: fn(svc.svcpath+x))]

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

    def speaker(self):
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
        and in the service.log if a svcpath is provided in <data>. If a
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
        svcpath = data.get("svcpath")
        data["monitor"] = Storage(self.get_node_monitor())
        if svcpath:
            try:
                data["service"] = Storage(self.get_service_agg(svcpath))
            except TypeError:
                data["service"] = Storage()
            try:
                data["instance"] = Storage(
                    self.get_service_instance(svcpath, rcEnv.nodename)
                )
            except TypeError:
                data["instance"] = Storage()
            try:
                data["instance"]["monitor"] = Storage(
                    self.get_service_monitor(svcpath)
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

        svcpath = fmt_data.get("svcpath")
        if svcpath:
            # log to node.log with a "service <svcpath> " prefix
            node_fmt = "service {svcpath} "+fmt
            getattr(self.log, level)(node_fmt.format(**fmt_data))

            # log to <svcname.log>
            with SERVICES_LOCK:
                svc = SERVICES.get(svcpath)
                if svc:
                    getattr(svc.log, level)(fmt.format(**fmt_data))
        else:
            # log to node.log with no prefix
            getattr(self.log, level)(fmt.format(**fmt_data))

    @lazy
    def vip(self):
        template = [
            ("sync#i0", "disable", "true"),
            ("DEFAULT", "orchestrate", "ha"),
            ("DEFAULT", "nodes", "*"),
        ]
        default_cidr = NODE.oget("cluster", "vip")
        for node in self.cluster_nodes:
            priv_cidr = NODE.oget("cluster", "vip", impersonate=node)
            if priv_cidr is None and cidr is None:
                if cidr is None:
                    self.log.info("cluster vip not set")
                else:
                    self.log.info("cluster vip not set for node %s", node)
            if priv_cidr != default_cidr:
                cidr = default_cidr
            else:
                cidr = priv_cidr
            try:
                addr, netmask = cidr.split("/", 1)
                netmask, ipdev = netmask.split("@", 1)
            except Exception as exc:
                self.log.info("cluster vip not set or malformed: %s", exc)
                return
            t = ("ip#0", "ipname", addr)
            if t not in template:
                template.append(t)
            t = ("ip#0", "netmask", netmask)
            if t not in template:
                template.append(t)
            if priv_cidr == default_cidr:
                t = ("ip#0", "ipdev", ipdev)
                if t not in template:
                    template.append(t)
            else:
                template += [
                    ("ip#0", "ipdev@"+node, ipdev),
                ]
        self.log.info("cluster vip %s" % default_cidr)
        svc = factory("svc")("vip", namespace="system", node=NODE)
        kws = []
        changes = []
        current = svc.print_config_data()
        for section, keyword, value in template:
            kws.append("%s.%s" % (section, keyword))
            try:
                val = svc._get("%s.%s" %(section, keyword))
            except (ex.OptNotFound, ex.RequiredOptNotFound):
                val = None
            if val != value:
                changes.append("%s.%s=%s" % (section, keyword, value))
        extraneous = []
        for kw in svc.print_config_data().get("ip#0", {}):
            _kw = "ip#0."+kw
            if _kw not in kws:
                extraneous.append(_kw)
        if changes:
            for k in changes:
                self.log.info("set %s: %s", svc.svcpath, k)
            svc.set_multi(changes, validation=False)
        if extraneous:
            for k in extraneous:
                self.log.info("unset %s: %s (undue)", svc.svcpath, k)
            svc.unset_multi(extraneous)
        return svc

