"""
A module to share variables used by osvcd threads.
"""
import os
import sys
import threading
import datetime
import time
import codecs
import hashlib
from subprocess import Popen

import rcExceptions as ex
from rcConfigParser import RawConfigParser
from rcUtilities import lazy, unset_lazy, is_string
from rcGlobalEnv import rcEnv, Storage
from freezer import Freezer
from converters import convert_duration, convert_boolean

# disable orchestration if a peer announces a different compat version than ours
COMPAT_VERSION = 5

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
    "status": "idle",
    "status_updated": datetime.datetime.utcnow(),
})
NMON_DATA_LOCK = threading.RLock()

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

# a queue of xmlrpc calls to do, fed by the lsnr, purged by the collector thread
COLLECTOR_XMLRPC_QUEUE = []

def wake_heartbeat_tx():
    """
    Notify the heartbeat tx thread to do they periodic job immediatly
    """
    with HB_TX_TICKER:
        HB_TX_TICKER.notify_all()

def wake_monitor():
    """
    Notify the monitor thread to do they periodic job immediatly
    """
    with MON_TICKER:
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
class OsvcThread(threading.Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """
    stop_tmo = 60

    def __init__(self):
        super(OsvcThread, self).__init__()
        self._stop_event = threading.Event()
        self._node_conf_event = threading.Event()
        self.created = time.time()
        self.threads = []
        self.procs = []

        # hash for log dups avoiding
        self.duplog_data = {}

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
            "created": datetime.datetime.utcfromtimestamp(self.created)\
                               .strftime(JSON_DATEFMT),
        })
        return data

    def push_proc(self, proc,
                  on_success=None, on_success_args=None, on_success_kwargs=None,
                  on_error=None, on_error_args=None, on_error_kwargs=None):
        """
        Enqueue a structure including a Popen() result and the success and
        error callbacks.
        """
        self.procs.append(Storage({
            "proc": proc,
            "on_success": on_success,
            "on_success_args": on_success_args if on_success_args else [],
            "on_success_kwargs": on_success_kwargs if on_success_kwargs else {},
            "on_error": on_error,
            "on_error_args": on_error_args if on_error_args else [],
            "on_error_kwargs": on_error_kwargs if on_error_kwargs else {},
        }))

    def terminate_procs(self):
        """
        Send a terminate() to all procs in the queue and wait for their
        completion.
        """
        for data in self.procs:
            data.proc.terminate()
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

    def join_threads(self):
        for thr in self.threads:
            thr.join()

    def janitor_threads(self):
        done = []
        for idx, thr in enumerate(self.threads):
            thr.join(0)
            if not thr.is_alive():
                done.append(idx)
        for idx in sorted(done, reverse=True):
            del self.threads[idx]
        if len(self.threads) > 2:
            self.log.info("threads queue length %d", len(self.threads))

    @lazy
    def freezer(self):
        return Freezer("node")

    @lazy
    def config(self):
        try:
            config = RawConfigParser()
            with codecs.open(rcEnv.paths.nodeconf, "r", "utf8") as filep:
                if sys.version_info[0] >= 3:
                    config.read_file(filep)
                else:
                    config.readfp(filep)
        except Exception as exc:
            self.log.info("error loading config: %s", exc)
            raise ex.excAbortAction()
        return config

    def reload_config(self):
        if not self._node_conf_event.is_set():
            return
        self._node_conf_event.clear()
        self.log.info("config change event received")
        unset_lazy(self, "config")
        unset_lazy(self, "quorum")
        unset_lazy(self, "arbitrators")
        unset_lazy(self, "cluster_name")
        unset_lazy(self, "cluster_key")
        unset_lazy(self, "cluster_id")
        unset_lazy(self, "cluster_nodes")
        unset_lazy(self, "sorted_cluster_nodes")
        unset_lazy(self, "maintenance_grace_period")
        unset_lazy(self, "rejoin_grace_period")
        unset_lazy(self, "ready_period")
        if not hasattr(self, "reconfigure"):
            return
        try:
            getattr(self, "reconfigure")()
        except Exception as exc:
            self.log.error("reconfigure error: %s", str(exc))
            self.stop()

    @staticmethod
    def get_service(svcname):
        with SERVICES_LOCK:
            if svcname not in SERVICES:
                return
        return SERVICES[svcname]

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
                        NMON_DATA.status if \
                            NMON_DATA.status else "none",
                        status
                    )
                NMON_DATA.status = status
                NMON_DATA.status_updated = datetime.datetime.utcnow()

            if local_expect:
                if local_expect == "unset":
                    local_expect = None
                if local_expect != NMON_DATA.local_expect:
                    self.log.info(
                        "node monitor local expect change: %s => %s",
                        NMON_DATA.local_expect if \
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
                        NMON_DATA.global_expect if \
                            NMON_DATA.global_expect else "none",
                        global_expect
                    )
                NMON_DATA.global_expect = global_expect

        wake_monitor()

    def set_smon(self, svcname, status=None, local_expect=None,
                 global_expect=None, reset_retries=False,
                 stonith=None):
        global SMON_DATA
        with SMON_DATA_LOCK:
            if svcname not in SMON_DATA:
                SMON_DATA[svcname] = Storage({
                    "status": "idle",
                    "status_updated": datetime.datetime.utcnow(),
                })
            if status:
                if status != SMON_DATA[svcname].status:
                    self.log.info(
                        "service %s monitor status change: %s => %s",
                        svcname,
                        SMON_DATA[svcname].status if \
                            SMON_DATA[svcname].status else "none",
                        status
                    )
                SMON_DATA[svcname].status = status
                SMON_DATA[svcname].status_updated = datetime.datetime.utcnow()

            if local_expect:
                if local_expect == "unset":
                    local_expect = None
                if local_expect != SMON_DATA[svcname].local_expect:
                    self.log.info(
                        "service %s monitor local expect change: %s => %s",
                        svcname,
                        SMON_DATA[svcname].local_expect if \
                            SMON_DATA[svcname].local_expect else "none",
                        local_expect
                    )
                SMON_DATA[svcname].local_expect = local_expect

            if global_expect:
                if global_expect == "unset":
                    global_expect = None
                if global_expect != SMON_DATA[svcname].global_expect:
                    self.log.info(
                        "service %s monitor global expect change: %s => %s",
                        svcname,
                        SMON_DATA[svcname].global_expect if \
                            SMON_DATA[svcname].global_expect else "none",
                        global_expect
                    )
                SMON_DATA[svcname].global_expect = global_expect

            if reset_retries and "restart" in SMON_DATA[svcname]:
                self.log.info("service %s monitor resources restart count reset",
                              svcname)
                del SMON_DATA[svcname]["restart"]

            if stonith:
                if stonith == "unset":
                    stonith = None
                if stonith != SMON_DATA[svcname].stonith:
                    self.log.info(
                        "service %s monitor stonith change: %s => %s",
                        svcname,
                        SMON_DATA[svcname].stonith if \
                            SMON_DATA[svcname].stonith else "none",
                        stonith
                    )
                SMON_DATA[svcname].stonith = stonith
        wake_monitor()

    def get_node_monitor(self, datestr=False, nodename=None):
        """
        Return the Monitor data of the node.
        If datestr is set, convert datetimes to a json compatible string.
        """
        if nodename is None:
            with NMON_DATA_LOCK:
                data = Storage(NMON_DATA)
        else:
            with CLUSTER_DATA_LOCK:
                if nodename not in CLUSTER_DATA:
                    return
                data = Storage(CLUSTER_DATA[nodename].get("monitor", {}))
        if datestr and isinstance(data.status_updated, datetime.datetime):
            data.status_updated = data.status_updated.strftime(DATEFMT)
        elif not datestr and is_string(data.status_updated):
            data.status_updated = datetime.datetime.strptime(data.status_updated, DATEFMT)
        return data

    def get_service_monitor(self, svcname, datestr=False):
        """
        Return the Monitor data of a service.
        If datestr is set, convert datetimes to a json compatible string.
        """
        with SMON_DATA_LOCK:
            if svcname not in SMON_DATA:
                self.set_smon(svcname, "idle")
            data = Storage(SMON_DATA[svcname])
            data["placement"] = self.get_service_placement(svcname)
            if datestr and isinstance(data.status_updated, datetime.datetime):
                data.status_updated = data.status_updated.strftime(DATEFMT)
            return data

    def get_service_placement(self, svcname):
        with SERVICES_LOCK:
            if svcname not in SERVICES:
                return ""
            svc = SERVICES[svcname]
            if self.placement_leader(svc, silent=True):
                return "leader"
        return ""

    def node_command(self, cmd):
        """
        A generic nodemgr command Popen wrapper.
        """
        cmd = [rcEnv.paths.nodemgr] + cmd
        self.log.info("execute: %s", " ".join(cmd))
        proc = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
        return proc

    def service_command(self, svcname, cmd):
        """
        A generic svcmgr command Popen wrapper.
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        cmd = [rcEnv.paths.svcmgr, '-s', svcname, "--local"] + cmd
        self.log.info("execute: %s", " ".join(cmd))
        proc = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True, env=env)
        return proc

    def add_cluster_node(self, nodename):
        nodes = " ".join(sorted(list(set(self.cluster_nodes + [nodename]))))
        cmd = ["set", "--param", "cluster.nodes", "--value", nodes]
        proc = self.node_command(cmd)
        ret = proc.wait()
        return ret

    def remove_cluster_node(self, nodename):
        cmd = ["set", "--param", "cluster.nodes", "--remove", nodename]
        proc = self.node_command(cmd)
        return proc.wait()

    @lazy
    def quorum(self):
        if self.config.has_option("cluster", "quorum"):
            return convert_boolean(self.config.get("cluster", "quorum"))
        else:
            return False

    @lazy
    def maintenance_grace_period(self):
        if self.config.has_option("node", "maintenance_grace_period"):
            return convert_duration(self.config.get("node", "maintenance_grace_period"))
        else:
            return 60

    @lazy
    def rejoin_grace_period(self):
        if self.config.has_option("node", "rejoin_grace_period"):
            return convert_duration(self.config.get("node", "rejoin_grace_period"))
        else:
            return 90

    @lazy
    def ready_period(self):
        if self.config.has_option("node", "ready_period"):
            seconds = convert_duration(self.config.get("node", "ready_period"))
        else:
            seconds = 16
        return datetime.timedelta(seconds=seconds)

    def in_maintenance_grace_period(self, nmon):
        if nmon.status == "upgrade":
            return True
        if nmon.status == "maintenance" and \
           nmon.status_updated > datetime.datetime.utcnow() - datetime.timedelta(seconds=self.maintenance_grace_period):
            return True
        return False

    def split_handler(self):
        if not self.quorum:
            self.duplog("info", "cluster is split, ignore as cluster.quorum is "
                        "false", msgid="quorum disabled")
            return
        if self.freezer.node_frozen():
            self.duplog("info", "cluster is split, ignore as the node is frozen",
                        msgid="quorum disabled")
            return
        live = len(CLUSTER_DATA)
        total = len(self.cluster_nodes)
        if live > total / 2:
            self.duplog("info", "cluster is split, we have 1st ring quorum: "
                        "%(live)d/%(total)d nodes", live=live, total=total)
            return
        arbitrator_vote = 0
        for arbitrator in self.arbitrators:
            ret = NODE._ping(arbitrator["name"])
            if ret == 0:
                arbitrator_vote = 1
                break
        if live + arbitrator_vote > total / 2:
            self.duplog("info", "cluster is split, we have 2nd ring quorum: "
                        "%(live)d+%(avote)d/%(total)d nodes (%(a)s)",
                        live=live, avote=arbitrator_vote, total=total,
                        a=arbitrator["name"])
            return
        self.duplog("info", "cluster is split, we don't have 1st nor 2nd ring "
                    "quorum: %(live)d+%(avote)d/%(total)d nodes (%(a)s)",
                    live=live, avote=arbitrator_vote, total=total,
                    a=arbitrator["name"])
        self.log.info("toc")
        NODE.system.crash()

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
                self.log.info("preserve node %s data in %s since %s (grace %s)",
                              nodename, nmon.status, nmon.status_updated, self.maintenance_grace_period)
            return
        self.log.info("no rx thread still receive from node %s and maintenance "
                      "grace period expired. flush its data",
                      nodename)
        with CLUSTER_DATA_LOCK:
            try:
                del CLUSTER_DATA[nodename]
            except KeyError:
                pass
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
    def get_service_instance(svcname, nodename):
        """
        Return the specified service status structure on the specified node.
        """
        try:
            with CLUSTER_DATA_LOCK:
                return Storage(CLUSTER_DATA[nodename]["services"]["status"][svcname])
        except KeyError:
            return

    #########################################################################
    #
    # Placement policies
    #
    #########################################################################
    def placement_candidates(self, svc, discard_frozen=True,
                             discard_unprovisioned=True,
                             discard_constraints_violation=True,
                             discard_start_failed=True):
        """
        Return the list of service nodes meeting the following criteria:
        * we have valid service instance data (not unknown, has avail)
        * the node is not in maintenance or upgrade
        * the node is not frozen (default)
        * the service is not frozen (default)
        * the service instance is provisioned (default)
        * the service instance smon status is not "start failed" (default)
        * the service instance constraints are eval'ed True (default)
        """
        candidates = []
        with CLUSTER_DATA_LOCK:
            for nodename, data in CLUSTER_DATA.items():
                if data == "unknown":
                    continue
                if data.get("monitor", {}).get("status") in ("maintenance", "upgrade"):
                    continue
                if discard_frozen and data.get("frozen", False):
                    # node frozen
                    continue
                instance = self.get_service_instance(svc.svcname, nodename)
                if instance is None:
                    continue
                if discard_start_failed and instance["monitor"]["status"] == "start failed":
                    continue
                if "avail" not in instance:
                    # deleting
                    continue
                if discard_frozen and instance.frozen:
                    continue
                if discard_unprovisioned and instance.provisioned is False:
                    continue
                if discard_constraints_violation and not instance.get("constraints", True):
                    continue
                candidates.append(nodename)
        return candidates

    def placement_ranks(self, svc, candidates=None):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        if svc.placement == "load avg":
            return self.placement_ranks_load_avg(svc, candidates)
        elif svc.placement == "nodes order":
            return self.placement_ranks_nodes_order(svc, candidates)
        elif svc.placement == "spread":
            return self.placement_ranks_spread(svc, candidates)
        else:
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

    def placement_leader(self, svc, candidates=None, silent=False):
        if candidates is None:
            candidates = self.placement_candidates(svc)
        if len(candidates) == 0:
            if not silent:
                self.duplog("info", "placement constraints prevent us from starting "
                            "service %(svcname)s on any node", svcname=svc.svcname)
            return False
        if rcEnv.nodename not in candidates:
            if not silent:
                self.duplog("info", "placement constraints prevent us from starting "
                            "service %(svcname)s on this node", svcname=svc.svcname)
            return False
        if len(candidates) == 1:
            if not silent:
                self.duplog("info", "we have the greatest placement priority for "
                            "service %(svcname)s (alone)", svcname=svc.svcname)
            return True

        ranks = self.placement_ranks(svc, candidates=candidates)
        if ranks == []:
            return False
        elif svc.topology == "failover":
            if rcEnv.nodename == ranks[0]:
                if not silent:
                    self.duplog("info", "we have the highest '%(placement)s' "
                                "placement priority for failover service %(svcname)s",
                                placement=svc.placement, svcname=svc.svcname)
                return True
            else:
                if not silent:
                    self.duplog("info", "node %(nodename)s is alive and has a higher "
                                  "'%(placement)s' placement priority for "
                                  "failover service %(svcname)s",
                                  nodename=ranks[0], placement=svc.placement,
                                  svcname=svc.svcname)
                return False
        elif svc.topology == "flex":
            index = ranks.index(rcEnv.nodename) + 1
            if not silent:
                self.duplog("info", "we have the %(idx)d/%(mini)d '%(placement)s'"
                            " placement priority for flex service %(svcname)s",
                            idx=index, mini=svc.flex_min_nodes,
                            placement=svc.placement, svcname=svc.svcname)
            if index <= svc.flex_min_nodes:
                return True
            else:
                return False

    def placement_ranks_spread(self, svc, candidates, silent=False):
        """
        hash together each candidate nodename+svcname, and sort the resulting
        list.
        """
        def fn(s):
            h = hashlib.md5()
            h.update(s.encode())
            return h.digest()
        return [nodename for nodename in sorted(candidates, key=lambda x: fn(svc.svcname+x))]

    def placement_ranks_load_avg(self, svc, candidates, silent=False):
        data = []
        with CLUSTER_DATA_LOCK:
            for nodename in CLUSTER_DATA:
                if nodename not in candidates:
                    continue
                try:
                    load = CLUSTER_DATA[nodename]["load"]["15m"]
                except KeyError:
                    continue
                data.append((nodename, load))
        return [nodename for nodename, _ in sorted(data, key=lambda x: x[1])]

    def placement_ranks_nodes_order(self, svc, candidates, silent=False):
        return [nodename for nodename in svc.ordered_peers if nodename in candidates]

