"""
The opensvc daemon.
"""
from __future__ import print_function

import logging
import os
import sys
import threading
import time
import multiprocessing
from optparse import OptionParser

import daemon.shared as shared
import core.exceptions as ex
import core.logger
from core.capabilities import capabilities
from core.comm import CRYPTO_MODULE
from utilities.lock import LockTimeout, cmlock
from env import Env
from utilities.proc import daemon_process_running, process_args
from .hb.disk import HbDiskRx, HbDiskTx
from .hb.mcast import HbMcastRx, HbMcastTx
from .hb.relay import HbRelayRx, HbRelayTx
from .hb.ucast import HbUcastRx, HbUcastTx
from .collector import Collector
from .dns import Dns
from .listener import Listener
from .monitor import Monitor
from .scheduler import Scheduler
from core.node import Node
from utilities.lazy import lazy, unset_lazy

# node monitor status where start_threads is allowed
START_THREADS_ALLOWED_NMON_STATUS = (None, "idle", "init", "rejoin", "draining")

try:
    # with python3, select the forkserver method beacuse the
    # default fork method is unsafe from the daemon.
    multiprocessing.set_start_method("forkserver")
    multiprocessing.set_forkserver_preload([
        "opensvc.core.comm",
        "opensvc.core.contexts",
        "opensvc.foreign.h2",
        "opensvc.foreign.hyper",
        "opensvc.foreign.jsonpath_ng.ext",
        "opensvc.utilities.forkserver",
        "opensvc.utilities.converters",
        "opensvc.utilities.naming",
        "opensvc.utilities.optparser",
        "opensvc.utilities.render",
        "opensvc.utilities.cache",
        "opensvc.utilities.lock",
        "opensvc.utilities.files",
        "opensvc.utilities.proc",
        "opensvc.utilities.string",
    ])

except (ImportError, AttributeError):
    # on python2, the only method is spawn, which is slow but
    # safe.
    pass

try:
    from utilities.os.linux import set_tname
except (ImportError, OSError):
    if hasattr(threading.Thread, '_bootstrap'):
        def _bootstrap_named_thread(self):
            set_tname(self.name)
            threading.Thread._bootstrap_original(self)

        threading.Thread._bootstrap_original = threading.Thread._bootstrap
        threading.Thread._bootstrap = _bootstrap_named_thread

DAEMON_TICKER = threading.Condition()
DAEMON_INTERVAL = 2
STATS_INTERVAL = 1

HEARTBEATS = (
    ("multicast", HbMcastTx, HbMcastRx),
    ("unicast", HbUcastTx, HbUcastRx),
    ("disk", HbDiskTx, HbDiskRx),
    ("relay", HbRelayTx, HbRelayRx),
)

def printstack(sig, frame):
    try:
        import faulthandler
    except ImportError:
        return
    try:
        faulthandler.dump_traceback()
        with open(os.path.join(Env.paths.pathvar, "daemon.stack"), "w") as f:
            faulthandler.dump_traceback(file=f)
    except Exception:
        pass

try:
    import signal
    signal.signal(signal.SIGUSR1, printstack)
except ImportError:
    pass

def fork(func, args=None, kwargs=None):
    """
    A fork daemonizing function.
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    if os.fork() > 0:
        # return to parent execution
        return

    # separate the son from the father
    os.chdir('/')
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except Exception:
        os._exit(1)

    # Redirect standard file descriptors.
    if hasattr(os, "devnull"):
        devnull = os.devnull
    else:
        devnull = "/dev/null"

    for fileno in range(0, 3):
        try:
            os.close(fileno)
        except OSError:
            pass

    # Popen(close_fds=True) does not close 0, 1, 2. Make sure we have those
    # initialized to /dev/null
    os.open(devnull, os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)

    try:
        func(*args, **kwargs)
    except Exception:
        sys.exit(1)

    sys.exit(0)


def forked(func):
    """
    A decorator that runs the decorated function in a detached subprocess
    immediately. A lock is held to avoid running the same function twice.
    """
    def _func(*args, **kwargs):
        fork(func, args, kwargs)
    return _func


#############################################################################
#
# Daemon
#
#############################################################################
class Daemon(object):
    """
    The OpenSVC daemon process.
    Can run forked or foreground.
    Janitors all the listener, the monitor and all heartbeat threads.
    Monitors the node configuration file and notify its changes to threads.
    """
    def __init__(self):
        self.handlers = None
        self.threads = {}
        self.last_config_mtime = None
        log_file = os.path.join(Env.paths.pathlog, "node.log")
        core.logger.initLogger(Env.nodename, log_file, handlers=self.handlers, sid=False)
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd"), {"node": Env.nodename, "component": "main"})
        self.pid = os.getpid()
        self.stats_data = None
        self.last_stats_refresh = 0
        self.init_data()

    def init_data(self):
        initial_data = {
            "monitor": {
                "nodes": {},
                "services": {},
            }
        }
        shared.DAEMON_STATUS.set([], initial_data)

    def stop(self):
        """
        The global stop method. Signal all threads to shutdown.
        """
        self.log.info("daemon stop")
        self.stop_threads()

    def run(self, daemon=True):
        """
        Switch between the forked/foreground execution mode.
        Drop the stream handler for the forked mode.
        """
        if daemon:
            self.handlers = ["file", "syslog"]
            self._run_daemon()
        else:
            self._run()

    @forked
    def _run_daemon(self):
        """
        The method used as fork-point in the daemon execution mode.
        """
        self.pid = os.getpid()
        self._run()

    def set_last_shutdown(self):
        with open(Env.paths.last_shutdown, "w") as filep:
            filep.write("")

    def _run(self):
        """
        Acquire the osvcd lock, write the pid in a system-compatible pidfile,
        and start the daemon loop.
        """

        try:
            with cmlock(lockfile=Env.paths.daemon_lock, timeout=1, delay=1):
                if self._already_running():
                    self.log.error("abort start: a daemon process is already running")
                    sys.exit(1)
                self.write_pid()
        except LockTimeout:
            self.log.error("abort start: a daemon is already running, and holding the daemon lock")
            sys.exit(1)
        self.loop_forever()

    def _already_running(self):
        if daemon_process_running():
            try:
                with open(Env.paths.daemon_pid, "r") as pid_file:
                    last_pid_trace = pid_file.read()
            except:
                self.log.error("another daemon process detected, but file error on %s" % Env.paths.daemon_pid)
                return True
            if last_pid_trace == str(self.pid):
                return False
            else:
                self.log.error("another daemon process is already running with pid %s" % last_pid_trace)
                return True
        else:
            return False

    def write_pid(self):
        pid = str(self.pid)
        with open(Env.paths.daemon_pid, "w") as ofile:
            ofile.write(pid)
        _, pid_args = process_args(self.pid)
        with open(Env.paths.daemon_pid_args, "w") as ofile:
            ofile.write(pid_args)

    def init(self):
        shared.NODE = Node(log_handlers=self.handlers)
        self.log.info("daemon started")
        self.log.info("versions:")
        self.log.info(" opensvc agent: %s", shared.NODE.agent_version)
        self.log.info(" opensvc api:   %s", shared.API_VERSION)
        self.log.info(" python:        %s", sys.version.split()[0])
        self.log.info(" crypto module: %s", CRYPTO_MODULE)
        caps = capabilities.scan(node=shared.NODE)
        self.log.info("%d capabilities:", len(caps))
        for cap in caps:
            self.log.info(" %s", cap)

    def loop_forever(self):
        """
        Loop over the daemon tasks until notified to stop.
        """
        self.init()
        while self.loop():
            with DAEMON_TICKER:
                DAEMON_TICKER.wait(DAEMON_INTERVAL)
        self.log.info("daemon graceful stop")

    def loop(self):
        if shared.DAEMON_STOP.is_set():
            self.stop_threads()
            return False
        self.start_threads()
        return True

    def stats(self):
        now = time.time()
        if self.stats_data and now - self.last_stats_refresh < STATS_INTERVAL:
            return self.stats_data
        self.stats_data = {
            "cpu": self.cpu_stats(),
            "mem": self.mem_stats(),
        }
        self.last_stats_refresh = now
        return self.stats_data

    def cpu_stats(self):
        data = {}
        try:
            pid_cpu_time = shared.NODE.pid_cpu_time(self.pid)
        except Exception as exc:
            pid_cpu_time = 0.0
        return {
            "time": pid_cpu_time,
        }

    def mem_stats(self):
        data = {}
        try:
            mem_total = shared.NODE.pid_mem_total(self.pid)
        except Exception as exc:
            mem_total = 0.0
        return {
            "total": shared.NODE.pid_mem_total(self.pid),
        }

    def stop_threads(self):
        """
        Send a stop notification to all threads, and wait for them to
        complete their shutdown.
        Stop dns last, so the service is available as long as possible.
        """
        self.log.info("signal stop to all threads")
        for thr_id, thr in self.threads.items():
            if thr_id == "dns":
                continue
            thr.stop()
        shared.wake_collector()
        shared.wake_scheduler()
        shared.wake_monitor(reason="stop threads", immediate=True)
        shared.wake_heartbeat_tx()
        for thr_id, thr in self.threads.items():
            if thr_id == "dns":
                continue
            self.log.info("waiting for %s to stop", thr_id)
            thr.join()
        if "dns" in self.threads:
            self.threads["dns"].stop()
            self.log.info("waiting for dns to stop")
            self.threads["dns"].join()

    def need_start(self, thr_id):
        """
        Return True if a thread need restarting, ie not signalled to stop
        and not alive.
        """
        try:
            thr = self.threads[thr_id]
        except KeyError:
            return True
        if thr.stopped():
            return False
        if thr.is_alive():
            return False
        return True

    def start_thread(self, key, obj, name=None):
        try:
            if name is None:
                self.threads[key] = obj()
            else:
                self.threads[key] = obj(name)
            self.threads[key].start()
            return True
        except RuntimeError as exc:
            self.log.warning("failed to start a %s thread: %s", key, exc)
            return False

    def start_threads(self):
        """
        Reload the node configuration if needed.
        Start threads or restart threads dead of an unexpected cause.
        Stop and delete heartbeat threads whose configuration was deleted.
        """
        try:
            nmon_status = shared.DAEMON_STATUS.get(["monitor", "nodes", Env.nodename, "monitor", "status"])
        except (KeyError, TypeError):
            nmon_status = None
        if nmon_status not in START_THREADS_ALLOWED_NMON_STATUS:
            return

        config_changed = self.read_config()

        # a thread can only be started once, allocate a new one if not alive.
        changed = False
        if shared.NODE.dns and self.need_start("dns"):
            changed |= self.start_thread("dns", Dns)
        if self.need_start("listener"):
            changed |= self.start_thread("listener", Listener)
        if shared.NODE and shared.NODE.collector_env.dbopensvc and self.need_start("collector"):
            changed |= self.start_thread("collector", Collector)
        if self.need_start("monitor"):
            changed |= self.start_thread("monitor", Monitor)
        if self.need_start("scheduler"):
            changed |= self.start_thread("scheduler", Scheduler)

        for hb_type, txc, rxc in HEARTBEATS:
            for name in self.get_config_hb(hb_type):
                hb_id = name + ".rx"
                if self.need_start(hb_id):
                    changed |= self.start_thread(hb_id, rxc, name)
                hb_id = name + ".tx"
                if self.need_start(hb_id):
                    changed |= self.start_thread(hb_id, txc, name)

        if config_changed:
            # clean up deleted heartbeats
            thr_ids = list(self.threads.keys())
            for thr_id in thr_ids:
                if not thr_id.startswith("hb#"):
                    continue
                name = thr_id.replace(".tx", "").replace(".rx", "")
                if self.hb_enabled(name):
                    continue
                self.log.info("heartbeat %s removed from configuration. stop "
                              "thread %s", name, thr_id)
                self.threads[thr_id].stop()
                self.threads[thr_id].join()
                del self.threads[thr_id]
                shared.DAEMON_STATUS.unset_safe([thr_id])
                changed = True

            # clean up collector thread no longer needed
            if shared.NODE and not shared.NODE.collector_env.dbopensvc and "collector" in self.threads:
                self.log.info("stopping collector thread, no longer needed")
                self.threads["collector"].stop()
                self.threads["collector"].join()
                del self.threads["collector"]
                shared.DAEMON_STATUS.unset_safe(["collector"])
                changed = True

        if changed:
            with shared.THREADS_LOCK:
                shared.THREADS = self.threads

    def init_nodeconf(self):
        if not os.path.exists(Env.paths.pathetc):
            self.log.info("create dir %s", Env.paths.pathetc)
            os.makedirs(Env.paths.pathetc)
        if not os.path.exists(Env.paths.nodeconf):
            self.log.info("create %s", Env.paths.nodeconf)
            with open(Env.paths.nodeconf, "a") as ofile:
                ofile.write("")
            os.chmod(Env.paths.nodeconf, 0o0600)

    def get_config_mtime(self):
        try:
            mtime = os.path.getmtime(Env.paths.nodeconf)
        except (OSError, IOError):
            self.init_nodeconf()
            mtime = os.path.getmtime(Env.paths.nodeconf)
        except Exception as exc:
            self.log.warning("failed to get node config mtime: %s", exc)
            mtime = 0
        try:
            cmtime = os.path.getmtime(Env.paths.clusterconf)
        except Exception as exc:
            cmtime = 0
        return mtime if mtime > cmtime else cmtime

    def read_config(self):
        locked = shared.CONFIG_LOCK.acquire(blocking=False)
        if not locked:
            return
        try:
            return self._read_config()
        finally:
            shared.CONFIG_LOCK.release()

    def _read_config(self):
        """
        Reload the node configuration file and notify the threads to do the
        same, if the file's mtime has changed since the last load.
        """
        mtime = self.get_config_mtime()
        if mtime is None:
            return
        if self.last_config_mtime is not None and \
                self.last_config_mtime >= mtime:
            return
        try:
            with shared.NODE_LOCK:
                if shared.NODE:
                    shared.NODE.close()
                shared.NODE = Node()
                shared.NODE.set_rlimit()
                shared.NODE.network_setup()
            unset_lazy(self, "config_hbs")
            if self.last_config_mtime:
                self.log.info("node config reloaded (changed)")
            else:
                self.log.info("node config loaded")
            self.last_config_mtime = mtime

            # signal the node config change to threads
            for thr in self.threads.values():
                if thr.stopped():
                    thr.unstop()
                else:
                    thr.notify_config_change()
            shared.wake_monitor(reason="config change", immediate=True)

            # signal the caller the config has changed
            return True
        except Exception as exc:
            self.log.warning("failed to load config: %s", str(exc))

    def get_config_hb(self, hb_type=None):
        """
        Parse the node configuration and return the list of heartbeat
        section names matching the specified type.
        """
        return self.config_hbs.get(hb_type, [])

    @lazy
    def config_hbs(self):
        """
        Parse the node configuration and return a dictionary of heartbeat
        section names indexed by heartbeat type.
        """
        hbs = {}
        for section in shared.NODE.conf_sections("hb", cd=shared.NODE.cd):
            try:
                section_type = shared.NODE.oget(section, "type")
            except Exception:
                continue
            try:
                hb_nodes = shared.NODE.conf_get(section, "nodes")
                if Env.nodename not in hb_nodes:
                    continue
            except ex.OptNotFound as exc:
                pass
            if section_type not in hbs:
                hbs[section_type] = [section]
            else:
                hbs[section_type].append(section)
        return hbs

    def hb_enabled(self, name):
        for names in self.config_hbs.values():
            if name in names:
                return True
        return False

#############################################################################
#
# Main
#
#############################################################################
def optparse(args=None):
    """
    Parse command line options for main().
    """
    parser = OptionParser()
    parser.add_option(
        "--debug", action="store_true",
        dest="debug"
    )
    parser.add_option(
        "-f", "--foreground", action="store_false",
        default=True, dest="daemon"
    )
    return parser.parse_args(args=args)


def main(args=None):
    """
    Start the daemon and catch Exceptions to reap it down cleanly.
    """
    options, _ = optparse(args=args)
    try:
        shared.DAEMON = Daemon()
        shared.DAEMON.run(daemon=options.daemon)
    except (KeyboardInterrupt, ex.Signal):
        shared.DAEMON.log.info("interrupted")
        shared.DAEMON.stop()
    except Exception as exc:
        shared.DAEMON.log.exception(exc)
        shared.DAEMON.stop()
