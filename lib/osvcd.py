"""
The opensvc daemon.
"""
from __future__ import print_function

import sys
import os
import threading
import logging
import codecs
import time
from optparse import OptionParser

import six
import rcExceptions as ex
import rcLogger
import osvcd_shared as shared
from rcConfigParser import RawConfigParser
from rcGlobalEnv import rcEnv
from rcUtilities import lazy, unset_lazy, ximport
from lock import lock, unlock

from osvcd_mon import Monitor
from osvcd_lsnr import Listener
from osvcd_scheduler import Scheduler
from osvcd_collector import Collector
from osvcd_dns import Dns
from hb_ucast import HbUcastRx, HbUcastTx
from hb_mcast import HbMcastRx, HbMcastTx
from hb_disk import HbDiskRx, HbDiskTx
from hb_relay import HbRelayRx, HbRelayTx
from comm import CRYPTO_MODULE

node_mod = ximport('node')

DAEMON_TICKER = threading.Condition()
DAEMON_INTERVAL = 2
STATS_INTERVAL = 1

HEARTBEATS = (
    ("multicast", HbMcastTx, HbMcastRx),
    ("unicast", HbUcastTx, HbUcastRx),
    ("disk", HbDiskTx, HbDiskRx),
    ("relay", HbRelayTx, HbRelayRx),
)


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
        rcLogger.initLogger(rcEnv.nodename, handlers=self.handlers)
        rcLogger.set_namelen(force=30)
        self.log = logging.getLogger(rcEnv.nodename+".osvcd")
        self.pid = os.getpid()
        self.stats_data = None
        self.last_stats_refresh = 0

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

    def init_nodeconf(self):
        if not os.path.exists(rcEnv.paths.pathetc):
            self.log.info("create dir %s", rcEnv.paths.pathetc)
            os.makedirs(rcEnv.paths.pathetc)
        if not os.path.exists(rcEnv.paths.nodeconf):
            self.log.info("create %s", rcEnv.paths.nodeconf)
            with open(rcEnv.paths.nodeconf, "a") as ofile:
                ofile.write("")
            os.chmod(rcEnv.paths.nodeconf, 0o0600)

    @lazy
    def config(self):
        """
        Allocate a config parser object and load the node configuration.
        Abstracting python2/3 differences in the parser modules and utf8
        handling.
        """
        self.init_nodeconf()
        try:
            config = RawConfigParser()
            with codecs.open(rcEnv.paths.nodeconf, "r", "utf8") as filep:
                try:
                    if six.PY3:
                        config.read_file(filep)
                    else:
                        config.readfp(filep)
                except AttributeError:
                    raise
        except Exception as exc:
            self.log.info("error loading config: %s", exc)
            raise ex.excAbortAction()
        return config

    def lock(self):
        try:
            self.lockfd = lock(lockfile=rcEnv.paths.daemon_lock, timeout=0,
                               delay=0)
        except Exception:
            self.log.error("a daemon is already running, and holding the "
                           "daemon lock")
            sys.exit(1)

    def set_last_shutdown(self):
        with open(rcEnv.paths.last_shutdown, "w") as filep:
            filep.write("")

    def unlock(self):
        if self.lockfd:
            unlock(self.lockfd)
        self.set_last_shutdown()

    def _run(self):
        """
        Acquire the osvcd lock, write the pid in a system-compatible pidfile,
        and start the daemon loop.
        """
        self.lock()
        try:
            self.write_pid()
            self.loop_forever()
        finally:
            self.unlock()

    def write_pid(self):
        pid = str(self.pid)+"\n"
        with open(rcEnv.paths.daemon_pid, "w") as ofile:
            ofile.write(pid)

    def init(self):
        shared.NODE = node_mod.Node()
        self.log.info("daemon started, version %s, crypto mod %s",
                      shared.NODE.agent_version, CRYPTO_MODULE)

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
        self.start_threads()
        if shared.DAEMON_STOP.is_set():
            self.stop_threads()
            return False
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
        if thr_id not in self.threads:
            return True
        thr = self.threads[thr_id]
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
        config_changed = self.read_config()

        # a thread can only be started once, allocate a new one if not alive.
        changed = False
        if shared.NODE.dns and self.need_start("dns"):
            changed |= self.start_thread("dns", Dns)
        if self.need_start("listener"):
            changed |= self.start_thread("listener", Listener)
        if shared.NODE and rcEnv.dbopensvc and self.need_start("collector"):
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
                changed = True

        if changed:
            with shared.THREADS_LOCK:
                shared.THREADS = self.threads

    def get_config_mtime(self, first=True):
        try:
            mtime = os.path.getmtime(rcEnv.paths.nodeconf)
        except Exception as exc:
            if first:
                self.init_nodeconf()
                return self.get_config_mtime(first=False)
            else:
                self.log.warning("failed to get node config mtime: %s",
                                 str(exc))
                return
        return mtime

    def read_config(self):
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
                shared.NODE = node_mod.Node()
                shared.NODE.set_rlimit()
            unset_lazy(self, "config")
            unset_lazy(self, "config_hbs")
            if self.last_config_mtime:
                self.log.info("node config reloaded (changed)")
            else:
                self.log.info("node config loaded")
            self.last_config_mtime = mtime

            # signal the node config change to threads
            for thr_id in self.threads:
                self.threads[thr_id].notify_config_change()
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
        for section in self.config.sections():
            if not section.startswith("hb#"):
                continue
            try:
                section_type = self.config.get(section, "type")
            except Exception:
                continue
            try:
                hb_nodes = shared.NODE.conf_get(section, "nodes")
                if rcEnv.nodename not in hb_nodes:
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
def optparse():
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
    return parser.parse_args()


def main():
    """
    Start the daemon and catch Exceptions to reap it down cleanly.
    """
    options, _ = optparse()
    try:
        shared.DAEMON = Daemon()
        shared.DAEMON.run(daemon=options.daemon)
    except (KeyboardInterrupt, ex.excSignal):
        shared.DAEMON.log.info("interrupted")
        shared.DAEMON.stop()
    except Exception as exc:
        shared.DAEMON.log.exception(exc)
        shared.DAEMON.stop()


if __name__ == "__main__":
    main()
