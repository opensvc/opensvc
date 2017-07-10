"""
A module to share variables used by osvcd threads.
"""
import sys
import threading
import datetime
import time
import codecs
from subprocess import Popen, PIPE

import rcExceptions as ex
from rcConfigParser import RawConfigParser
from rcUtilities import lazy, unset_lazy
from rcGlobalEnv import rcEnv, Storage
from freezer import Freezer

DEFAULT_HB_PERIOD = 5
DATEFMT = "%Y-%m-%dT%H:%M:%S.%fZ"
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
SCHED_TICKER = threading.Condition()
HB_TX_TICKER = threading.Condition()

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

    def status(self):
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
                               .strftime('%Y-%m-%dT%H:%M:%SZ'),
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
            "on_success_kwargs": on_success_args if on_success_kwargs else {},
            "on_error": on_error,
            "on_error_args": on_error_args if on_error_args else [],
            "on_error_kwargs": on_error_args if on_error_kwargs else {},
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
                    break
                time.sleep(1)

    def janitor_procs(self):
        done = []
        for idx, data in enumerate(self.procs):
            data.proc.poll()
            if data.proc.returncode is not None:
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
        unset_lazy(self, "cluster_name")
        unset_lazy(self, "cluster_key")
        unset_lazy(self, "cluster_nodes")

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
                 global_expect=None, reset_retries=False):
        global SMON_DATA
        with SMON_DATA_LOCK:
            if svcname not in SMON_DATA:
                SMON_DATA[svcname] = Storage({})
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
        wake_monitor()

    def get_node_monitor(self, datestr=False):
        """
        Return the Monitor data of the node.
        If datestr is set, convert datetimes to a json compatible string.
        """
        with NMON_DATA_LOCK:
            data = Storage(NMON_DATA)
            if datestr:
                data.status_updated = data.status_updated.strftime(DATEFMT)
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
            if datestr:
                data.status_updated = data.status_updated.strftime(DATEFMT)
            return data

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
        cmd = [rcEnv.paths.svcmgr, '-s', svcname, "--crm"] + cmd
        self.log.info("execute: %s", " ".join(cmd))
        proc = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
        return proc

    def add_cluster_node(self, nodename):
        new_nodes = self.cluster_nodes + [nodename]
        new_nodes_str = " ".join(new_nodes)
        cmd = ["set", "--param", "cluster.nodes", "--value", new_nodes_str]
        proc = self.node_command(cmd)
        return proc.wait()

    def forget_peer_data(self, nodename):
        """
        Purge a stale peer data if all rx threads are down.
        """
        if not self.peer_down(nodename):
            self.log.info("other rx threads still receive from node %s",
                          nodename)
            return
        self.log.info("no rx thread still receive from node %s. flush its data",
                      nodename)
        with CLUSTER_DATA_LOCK:
            try:
                del CLUSTER_DATA[nodename]
            except KeyError:
                pass

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

