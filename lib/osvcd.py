"""
The opensvc daemon.
"""
from __future__ import print_function

import sys
import os
import threading
import logging
import codecs
from optparse import OptionParser

import rcExceptions as ex
import rcLogger
import osvcd_shared as shared
from rcConfigParser import RawConfigParser
from rcGlobalEnv import rcEnv
from rcUtilities import lazy, unset_lazy
from node import Node

from osvcd_mon import Monitor
from osvcd_lsnr import Listener
from osvcd_scheduler import Scheduler
from hb_ucast import HbUcastRx, HbUcastTx
from hb_mcast import HbMcastRx, HbMcastTx
from hb_disk import HbDiskRx, HbDiskTx

DAEMON_TICKER = threading.Condition()

HEARTBEATS = (
    ("multicast", HbMcastTx, HbMcastRx),
    ("unicast", HbUcastTx, HbUcastRx),
    ("dsk", HbDiskTx, HbDiskRx),
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
        rcLogger.initLogger(rcEnv.nodename, self.handlers)
        rcLogger.set_namelen(force=30)
        self.log = logging.getLogger(rcEnv.nodename+".osvcd")

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
        self.log.info("daemon started")
        self._run()

    @lazy
    def config(self):
        """
        Allocate a config parser object and load the node configuration.
        Abstracting python2/3 differences in the parser modules and utf8
        handling.
        """
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

    def _run(self):
        """
        Acquire the osvcd lock, write the pid in a system-compatible pidfile,
        and start the daemon loop.
        """
        from lock import lock, unlock
        try:
            lockfd = lock(lockfile=rcEnv.paths.daemon_lock, timeout=0, delay=0)
        except Exception:
            self.log.error("a daemon is already running, and holding the daemon lock")
            sys.exit(1)
        try:
            pid = str(os.getpid())+"\n"
            with open(rcEnv.paths.daemon_pid, "w") as ofile:
                ofile.write(pid)
            self.__run()
        finally:
            unlock(lockfd)

    def __run(self):
        """
        Loop over the daemon tasks until notified to stop.
        """
        while True:
            self.start_threads()
            if shared.DAEMON_STOP.is_set():
                self.stop_threads()
                break
            with DAEMON_TICKER:
                DAEMON_TICKER.wait(1)
        self.log.info("daemon graceful stop")

    def stop_threads(self):
        """
        Send a stop notification to all threads, and wait for them to
        complete their shutdown.
        """
        self.log.info("signal stop to all threads")
        for thr in self.threads.values():
            thr.stop()
        shared.wake_scheduler()
        shared.wake_monitor()
        shared.wake_heartbeat_tx()
        for thr_id, thr in self.threads.items():
            self.log.info("waiting for %s to stop", thr_id)
            thr.join()

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

    def start_threads(self):
        """
        Reload the node configuration if needed.
        Start threads or restart threads dead of an unexpected cause.
        Stop and delete heartbeat threads whose configuration was deleted.
        """
        # a thread can only be started once, allocate a new one if not alive.
        changed = False
        if self.need_start("listener"):
            self.threads["listener"] = Listener()
            self.threads["listener"].start()
            changed = True
        if self.need_start("scheduler"):
            self.threads["scheduler"] = Scheduler()
            self.threads["scheduler"].start()
            changed = True

        self.read_config()

        for hb_type, txc, rxc in HEARTBEATS:
            for name in self.get_config_hb(hb_type):
                hb_id = name + ".rx"
                if self.need_start(hb_id):
                    self.threads[hb_id] = rxc(name)
                    self.threads[hb_id].start()
                    changed = True
                hb_id = name + ".tx"
                if self.need_start(hb_id):
                    self.threads[hb_id] = txc(name)
                    self.threads[hb_id].start()
                    changed = True

        if self.need_start("monitor"):
            self.threads["monitor"] = Monitor()
            self.threads["monitor"].start()
            changed = True

        # clean up deleted heartbeats
        thr_ids = self.threads.keys()
        for thr_id in thr_ids:
            if not thr_id.startswith("hb#"):
                continue
            name = thr_id.replace(".tx", "").replace(".rx", "")
            if not self.config.has_section(name):
                self.log.info("heartbeat %s removed from configuration. stop "
                              "thread %s", name, thr_id)
                self.threads[thr_id].stop()
                self.threads[thr_id].join()
                del self.threads[thr_id]

        if changed:
            with shared.THREADS_LOCK:
                shared.THREADS = self.threads

    def read_config(self):
        """
        Reload the node configuration file and notify the threads to do the
        same, if the file's mtime has changed since the last load.
        """
        if not os.path.exists(rcEnv.paths.nodeconf):
            return
        try:
            mtime = os.path.getmtime(rcEnv.paths.nodeconf)
        except Exception as exc:
            self.log.warning("failed to get node config mtime: %s", str(exc))
            return
        if self.last_config_mtime is not None and \
           self.last_config_mtime >= mtime:
            return
        try:
            with shared.NODE_LOCK:
                if shared.NODE:
                    shared.NODE.close()
                shared.NODE = Node()
            unset_lazy(self, "config")
            if self.last_config_mtime:
                self.log.info("node config reloaded (changed)")
            else:
                self.log.info("node config loaded")
            self.last_config_mtime = mtime

            # signal the node config change to threads
            for thr_id in self.threads:
                self.threads[thr_id].notify_config_change()
        except Exception as exc:
            self.log.warning("failed to load config: %s", str(exc))

    def get_config_hb(self, hb_type=None):
        """
        Parse the node configuration and return the list of heartbeat
        section names matching the specified type.
        """
        hbs = []
        for section in self.config.sections():
            if not section.startswith("hb#"):
                continue
            try:
                section_type = self.config.get(section, "type")
            except Exception:
                section_type = None
            if hb_type and section_type != hb_type:
                continue
            hbs.append(section)
        return hbs

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
        daemon = Daemon()
        daemon.run(daemon=options.daemon)
    except (KeyboardInterrupt, ex.excSignal):
        daemon.log.info("interrupted")
        daemon.stop()
    except Exception as exc:
        daemon.log.exception(exc)
        daemon.stop()

if __name__ == "__main__":
    main()
