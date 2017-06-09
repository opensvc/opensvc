"""
This module implements the Node class.
The node
* handles communications with the collector
* holds the list of services
* has a scheduler
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import datetime
import sys
import json
import socket

if sys.version_info[0] < 3:
    from urllib2 import Request, urlopen
    from urllib2 import HTTPError
    from urllib import urlencode
else:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError
    from urllib.parse import urlencode

import svcBuilder
import xmlrpcClient
from rcGlobalEnv import rcEnv, Storage
import rcCommandWorker
import rcLogger
import rcExceptions as ex
from rcScheduler import scheduler_fork, Scheduler, SchedOpts
from rcConfigParser import RawConfigParser
from rcColor import formatter
from rcUtilities import justcall, lazy, lazy_initialized, vcall, check_privs, \
                        call, which, purge_cache
from osvcd import Crypt

if sys.version_info[0] < 3:
    BrokenPipeError = IOError

os.environ['LANG'] = 'C'

DEPRECATED_KEYWORDS = {
    "node.host_mode": "env",
    "node.environment": "asset_env",
    "node.environnement": "asset_env",
}

REVERSE_DEPRECATED_KEYWORDS = {
    "node.asset_env": ["environnement", "environment"],
    "node.env": ["host_mode"],
}

ACTIONS_NO_PARALLEL = [
    "edit_config",
    "get",
    "print_config",
    "print_resource_status",
    "print_schedule",
    "print_status",
]

ACTIONS_NO_MULTIPLE_SERVICES = [
    "print_resource_status",
]

CONFIG_DEFAULTS = {
    "clusters": "",
    "node_env": "TST",
    "push_schedule": "00:00-06:00@361 mon-sun",
    "sync_schedule": "04:00-06:00@121 mon-sun",
    "comp_schedule": "02:00-06:00@241 sun",
    "collect_stats_schedule": "@10",
    "no_schedule": "",
}

UNPRIVILEGED_ACTIONS = [
    "collector_cli",
]

class Node(Crypt):
    """
    Defines a cluster node.  It contain list of Svc.
    Implements node-level actions and checks.
    """
    def __str__(self):
        return self.nodename

    def __init__(self):
        self.ex_monitor_action_exit_code = 251
        self.config = None
        self.auth_config = None
        self.clusters = None
        self.clouds = None
        self.paths = Storage(
            reboot_flag=os.path.join(rcEnv.paths.pathvar, "REBOOT_FLAG"),
        )
        self.services = None
        self.load_config()
        self.options = Storage(
            cron=False,
            syncrpc=False,
            force=False,
            debug=False,
            stats_dir=None,
            begin=None,
            end=None,
            moduleset="",
            module="",
            ruleset_date="",
            objects=[],
            format=None,
            user=None,
            api=None,
            resource=[],
            mac=None,
            broadcast=None,
            param=None,
            value=None,
            extra_argv=[],
        )
        self.set_collector_env()
        self.log = rcLogger.initLogger(rcEnv.nodename)

    @property
    def svcs(self):
        if self.services is None:
            return None
        return list(self.services.values())

    @lazy
    def sched(self):
        """
        Lazy initialization of the node Scheduler object.
        """
        return Scheduler(
            config_defaults=CONFIG_DEFAULTS,
            options=self.options,
            config=self.config,
            log=self.log,
            scheduler_actions={
                "checks": SchedOpts(
                    "checks"
                ),
                "dequeue_actions": SchedOpts(
                    "dequeue_actions",
                    schedule_option="no_schedule"
                ),
                "pushstats": SchedOpts(
                    "stats"
                ),
                "collect_stats": SchedOpts(
                    "stats_collection",
                    schedule_option="collect_stats_schedule"
                ),
                "pushpkg": SchedOpts(
                    "packages"
                ),
                "pushpatch": SchedOpts(
                    "patches"
                ),
                "pushasset": SchedOpts(
                    "asset"
                ),
                "pushnsr": SchedOpts(
                    "nsr",
                    schedule_option="no_schedule"
                ),
                "pushhp3par": SchedOpts(
                    "hp3par",
                    schedule_option="no_schedule"
                ),
                "pushemcvnx": SchedOpts(
                    "emcvnx",
                    schedule_option="no_schedule"
                ),
                "pushcentera": SchedOpts(
                    "centera",
                    schedule_option="no_schedule"
                ),
                "pushnetapp": SchedOpts(
                    "netapp",
                    schedule_option="no_schedule"
                ),
                "pushibmds": SchedOpts(
                    "ibmds",
                    schedule_option="no_schedule"
                ),
                "pushdcs": SchedOpts(
                    "dcs",
                    schedule_option="no_schedule"
                ),
                "pushfreenas": SchedOpts(
                    "freenas",
                    schedule_option="no_schedule"
                ),
                "pushxtremio": SchedOpts(
                    "xtremio",
                    schedule_option="no_schedule"
                ),
                "pushgcedisks": SchedOpts(
                    "gcedisks",
                    schedule_option="no_schedule"
                ),
                "pushhds": SchedOpts(
                    "hds",
                    schedule_option="no_schedule"
                ),
                "pushnecism": SchedOpts(
                    "necism",
                    schedule_option="no_schedule"
                ),
                "pusheva": SchedOpts(
                    "eva",
                    schedule_option="no_schedule"
                ),
                "pushibmsvc": SchedOpts(
                    "ibmsvc",
                    schedule_option="no_schedule"
                ),
                "pushvioserver": SchedOpts(
                    "vioserver",
                    schedule_option="no_schedule"
                ),
                "pushsym": SchedOpts(
                    "sym",
                    schedule_option="no_schedule"
                ),
                "pushbrocade": SchedOpts(
                    "brocade", schedule_option="no_schedule"
                ),
                "pushdisks": SchedOpts(
                    "disks"
                ),
                "sysreport": SchedOpts(
                    "sysreport"
                ),
                "compliance_auto": SchedOpts(
                    "compliance",
                    fname="node"+os.sep+"last_comp_check",
                    schedule_option="comp_schedule"
                ),
                "auto_rotate_root_pw": SchedOpts(
                    "rotate_root_pw",
                    fname="node"+os.sep+"last_rotate_root_pw",
                    schedule_option="no_schedule"
                ),
                "auto_reboot": SchedOpts(
                    "reboot",
                    fname="node"+os.sep+"last_auto_reboot",
                    schedule_option="no_schedule"
                )
            },
        )

    @lazy
    def collector(self):
        """
        Lazy initialization of the node Collector object.
        """
        self.log.debug("initiatize node::collector")
        return xmlrpcClient.Collector(node=self)

    @lazy
    def cmdworker(self):
        """
        Lazy initialization of the node asynchronous command execution queue.
        """
        self.log.debug("initiatize node::cmdworker")
        return rcCommandWorker.CommandWorker()

    @lazy
    def nodename(self):
        """
        Lazy initialization of the node name.
        """
        self.log.debug("initiatize node::nodename")
        return socket.gethostname().lower()

    @lazy
    def system(self):
        """
        Lazy initialization of the operating system object, which implements
        specific methods like crash or fast-reboot.
        """
        try:
            rcos = __import__('rcOs'+rcEnv.sysname)
        except ImportError:
            rcos = __import__('rcOs')
        return rcos.Os()

    @lazy
    def compliance(self):
        from compliance import Compliance
        comp = Compliance(self)
        return comp

    @staticmethod
    def check_privs(action):
        """
        Raise if the action requires root privileges but the current
        running user is not root.
        """
        if action in UNPRIVILEGED_ACTIONS:
            return
        check_privs()

    @staticmethod
    def split_url(url, default_app=None):
        """
        Split a node.conf node.dbopensvc style url into a
        (protocol, host, port, app) tuple.
        """
        if url == 'None':
            return 'https', '127.0.0.1', '443', '/'

        # transport
        if url.startswith('https'):
            transport = 'https'
            url = url.replace('https://', '')
        elif url.startswith('http'):
            transport = 'http'
            url = url.replace('http://', '')
        else:
            transport = 'https'

        elements = url.split('/')
        if len(elements) < 1:
            raise ex.excError("url %s should have at least one slash")

        # app
        if len(elements) > 1:
            app = elements[1]
        else:
            app = default_app

        # host/port
        subelements = elements[0].split(':')
        if len(subelements) == 1:
            host = subelements[0]
            if transport == 'http':
                port = '80'
            else:
                port = '443'
        elif len(subelements) == 2:
            host = subelements[0]
            port = subelements[1]
        else:
            raise ex.excError("too many columns in %s" % ":".join(subelements))

        return transport, host, port, app

    def set_collector_env(self):
        """
        Store the collector connection elements parsed from the node.conf
        node.uuid, node.dbopensvc and node.dbcompliance as properties of the
        rcEnv class.
        """
        if self.config is None:
            self.load_config()
        if self.config is None:
            return
        if self.config.has_option('node', 'dbopensvc'):
            url = self.config.get('node', 'dbopensvc')
            try:
                (
                    rcEnv.dbopensvc_transport,
                    rcEnv.dbopensvc_host,
                    rcEnv.dbopensvc_port,
                    rcEnv.dbopensvc_app
                ) = self.split_url(url, default_app="feed")
                rcEnv.dbopensvc = "%s://%s:%s/%s/default/call/xmlrpc" % (
                    rcEnv.dbopensvc_transport,
                    rcEnv.dbopensvc_host,
                    rcEnv.dbopensvc_port,
                    rcEnv.dbopensvc_app
                )
            except ex.excError as exc:
                self.log.error("malformed dbopensvc url: %s (%s)",
                               rcEnv.dbopensvc, str(exc))
        else:
            rcEnv.dbopensvc_transport = None
            rcEnv.dbopensvc_host = None
            rcEnv.dbopensvc_port = None
            rcEnv.dbopensvc_app = None
            rcEnv.dbopensvc = None

        if self.config.has_option('node', 'dbcompliance'):
            url = self.config.get('node', 'dbcompliance')
            try:
                (
                    rcEnv.dbcompliance_transport,
                    rcEnv.dbcompliance_host,
                    rcEnv.dbcompliance_port,
                    rcEnv.dbcompliance_app
                ) = self.split_url(url, default_app="init")
                rcEnv.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (
                    rcEnv.dbcompliance_transport,
                    rcEnv.dbcompliance_host,
                    rcEnv.dbcompliance_port,
                    rcEnv.dbcompliance_app
                )
            except ex.excError as exc:
                self.log.error("malformed dbcompliance url: %s (%s)",
                               rcEnv.dbcompliance, str(exc))
        else:
            rcEnv.dbcompliance_transport = rcEnv.dbopensvc_transport
            rcEnv.dbcompliance_host = rcEnv.dbopensvc_host
            rcEnv.dbcompliance_port = rcEnv.dbopensvc_port
            rcEnv.dbcompliance_app = "init"
            rcEnv.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (
                rcEnv.dbcompliance_transport,
                rcEnv.dbcompliance_host,
                rcEnv.dbcompliance_port,
                rcEnv.dbcompliance_app
            )

        if self.config.has_option('node', 'uuid'):
            rcEnv.uuid = self.config.get('node', 'uuid')
        else:
            rcEnv.uuid = ""

    def call(self, *args, **kwargs):
        """
        Wrap rcUtilities call function, setting the node logger.
        """
        kwargs["log"] = self.log
        return call(*args, **kwargs)

    def vcall(self, *args, **kwargs):
        """
        Wrap rcUtilities vcall function, setting the node logger.
        """
        kwargs["log"] = self.log
        return vcall(*args, **kwargs)

    def build_services(self, *args, **kwargs):
        """
        Instanciate a Svc objects for each requested services and add it to
        the node.
        If a service configuration file has changed since the last time we
        sent it to the collector, resend. This behaviour can be blocked by
        the caller, using the autopush=False keyword argument.
        """
        if self.svcs is not None and \
           ('svcnames' not in kwargs or \
           (isinstance(kwargs['svcnames'], list) and len(kwargs['svcnames']) == 0)):
            return

        if 'svcnames' in kwargs and \
           isinstance(kwargs['svcnames'], list) and \
           len(kwargs['svcnames']) > 0 and \
           self.svcs is not None:
            svcnames_request = set(kwargs['svcnames'])
            svcnames_actual = set([s.svcname for s in self.svcs])
            if len(svcnames_request-svcnames_actual) == 0:
                return

        self.services = {}
        autopush = True
        if 'autopush' in kwargs:
            if not kwargs['autopush']:
                autopush = False
            del kwargs['autopush']

        svcs, errors = svcBuilder.build_services(*args, **kwargs)
        if 'svcnames' in kwargs:
            self.check_build_errors(kwargs['svcnames'], svcs, errors)

        for svc in svcs:
            self += svc

        if autopush:
            for svc in self.svcs:
                try:
                    svc.autopush()
                except ex.excError as exc:
                    self.log.error(str(exc))

        rcLogger.set_namelen(self.svcs)

    @staticmethod
    def check_build_errors(svcnames, svcs, errors):
        """
        Raise error if the service builder did not return a Svc object for
        each service we requested.
        """
        if isinstance(svcnames, list):
            n_args = len(svcnames)
        else:
            n_args = 1
        n_svcs = len(svcs)
        if n_svcs == n_args:
            return 0
        msg = ""
        if n_args > 1:
            msg += "%d services validated out of %d\n" % (n_svcs, n_args)
        if len(errors) == 1:
            msg += errors[0]
        else:
            msg += "\n".join(["- "+err for err in errors])
        raise ex.excError(msg)

    def rebuild_services(self, svcnames, minimal):
        """
        Delete the list of Svc objects in the Node object and create a new one.

        Args:
          svcnames: add only Svc objects for services specified
          minimal: include a minimal set of properties in the new Svc objects
        """
        del self.services
        self.services = None
        self.build_services(svcnames=svcnames, autopush=False, minimal=minimal)

    def close(self):
        """
        Stop the node class workers
        """
        if lazy_initialized(self, "collector"):
            self.collector.stop_worker()
        if lazy_initialized(self, "cmdworker"):
            self.cmdworker.stop_worker()
        import gc
        import threading
        gc.collect()
        for thr in threading.enumerate():
            if thr.name == 'QueueFeederThread' and thr.ident is not None:
                thr.join(1)


    def edit_config(self):
        """
        edit_config node action entrypoint
        """
        fpath = os.path.join(rcEnv.paths.pathetc, "node.conf")
        return self.edit_cf(fpath)

    def edit_authconfig(self):
        """
        edit_authconfig node action entrypoint
        """
        fpath = os.path.join(rcEnv.paths.pathetc, "auth.conf")
        return self.edit_cf(fpath)

    @staticmethod
    def edit_cf(fpath):
        """
        Choose an editor, setup the LANG, and exec the editor on the
        file passed as argument.
        """
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        if not which(editor):
            print("%s not found" % editor, file=sys.stderr)
            return 1
        os.environ["LANG"] = "en_US.UTF-8"
        return os.system(' '.join((editor, fpath)))

    def write_config(self):
        """
        Rewrite node.conf using the in-memory ConfigParser object as reference.
        """
        for option in CONFIG_DEFAULTS:
            if self.config.has_option('DEFAULT', option):
                self.config.remove_option('DEFAULT', option)
        for section in self.config.sections():
            if '#sync#' in section:
                self.config.remove_section(section)
        import tempfile
        import shutil
        try:
            tmpf = tempfile.NamedTemporaryFile()
            fpath = tmpf.name
            tmpf.close()
            with open(fpath, "w") as tmpf:
                self.config.write(tmpf)
            shutil.move(fpath, rcEnv.paths.nodeconf)
        except (OSError, IOError) as exc:
            print("failed to write new %s (%s)" % (rcEnv.paths.nodeconf, str(exc)),
                  file=sys.stderr)
            raise ex.excError
        try:
            os.chmod(rcEnv.paths.nodeconf, 0o0600)
        except OSError:
            pass
        self.load_config()

    def purge_status_last(self):
        """
        Purge the cached status of each and every services and resources.
        """
        for svc in self.svcs:
            svc.purge_status_last()

    @staticmethod
    def read_cf(fpath, defaults=None):
        """
        Read and parse an arbitrary ini-formatted config file, and return
        the RawConfigParser object.
        """
        import codecs
        if defaults is None:
            defaults = {}
        config = RawConfigParser(defaults)
        if not os.path.exists(fpath):
            return config
        with codecs.open(fpath, "r", "utf8") as ofile:
            if sys.version_info[0] >= 3:
                config.read_file(ofile)
            else:
                config.readfp(ofile)
        return config

    def load_config(self):
        """
        Parse the node.conf configuration file and store the RawConfigParser
        object as self.config.
        IOError is catched and self.config is left to its current value, which
        initially is None. So users of self.config should check for this None
        value before use.
        """
        try:
            self.config = self.read_cf(rcEnv.paths.nodeconf, CONFIG_DEFAULTS)
        except IOError:
            # some action don't need self.config
            pass

    def load_auth_config(self):
        """
        Parse the auth.conf configuration file and store the RawConfigParser
        object as self.auth_config.
        The actual parsing is done only on first call. This method is a noop
        on subsequent calls.
        """
        if self.auth_config is not None:
            return
        self.auth_config = self.read_cf(rcEnv.paths.authconf)

    @staticmethod
    def setup_sync_outdated():
        """
        Return True if any configuration file has changed in the last 10'
        else return False
        """
        import glob
        setup_sync_flag = os.path.join(rcEnv.paths.pathvar, 'last_setup_sync')
        fpaths = glob.glob(os.path.join(rcEnv.paths.pathetc, '*.conf'))
        if not os.path.exists(setup_sync_flag):
            return True
        for fpath in fpaths:
            try:
                mtime = os.stat(fpath).st_mtime
                with open(setup_sync_flag) as ofile:
                    last = float(ofile.read())
            except (OSError, IOError):
                return True
            if mtime > last:
                return True
        return False

    def get_clusters(self):
        """
        Set the self.clusters list from the configuration file node.cluster
        option value.
        """
        if self.clusters is not None:
            return
        if self.config and self.config.has_option("node", "cluster"):
            self.clusters = list(set(self.config.get('node', 'clusters').split()))
        else:
            self.clusters = []

    def __iadd__(self, svc):
        """
        Implement the Node() += Svc() operation, setting the node backpointer
        in the added service, storing the service in a list and setting a
        clustername if not already set in the service explicitely.
        """
        if not hasattr(svc, "svcname"):
            return self
        if self.services is None:
            self.services = {}
        svc.node = self
        self.get_clusters()
        if not hasattr(svc, "clustername") and len(self.clusters) == 1:
            svc.clustername = self.clusters[0]
        self.services[svc.svcname] = svc
        return self

    def action(self, action):
        """
        The node action wrapper.
        Looks up which method to handle the action (some are not implemented
        in the Node class), and call the handling method.
        """
        if "_json_" in action:
            self.options.format = "json"
            action = action.replace("_json_", "_")
        if action.startswith("json_"):
            self.options.format = "json"
            action = "print" + action[4:]

        if action.startswith("compliance_"):
            if self.options.cron and action == "compliance_auto" and \
               self.config.has_option('compliance', 'auto_update') and \
               self.config.getboolean('compliance', 'auto_update'):
                self.compliance.updatecomp = True
                self.compliance.node = self
            return getattr(self.compliance, action)()
        elif action.startswith("collector_") and action != "collector_cli":
            from collector import Collector
            coll = Collector(self.options, self)
            data = getattr(coll, action)()
            self.print_data(data)
            return 0
        elif action.startswith("print"):
            getattr(self, action)()
            return 0
        else:
            ret = getattr(self, action)()
            if ret is None:
                return 0
            return ret

    @formatter
    def print_data(self, data):
        """
        A dummy method decorated by the formatter function.
        The formatter needs self to access the formatting options, so this
        can't be a staticmethod.
        """
        fmt = self.options.format if self.options.format else "default"
        self.log.debug("format data using the %s formatter", fmt)
        return data

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        return self.sched.print_schedule()

    def scheduler(self):
        """
        The node scheduler entrypoint.
        Evaluates execution constraints for all scheduled tasks and executes
        the tasks if required.
        """
        self.options.cron = True
        for action in self.sched.scheduler_actions:
            try:
                if action == "compliance_auto":
                    self.compliance_auto()
                else:
                    self.action(action)
            except:
                self.log.exception("")

    def schedulers(self):
        """
        schedulers node action entrypoint.
        Run the node scheduler and each configured service scheduler.
        """
        purge_cache()
        self.scheduler()

        self.build_services()
        for svc in self.svcs:
            try:
                svc.scheduler()
            except ex.excError as exc:
                svc.log.error(exc)

    def get_push_objects(self, section):
        """
        Returns the object names to do inventory on.
        Object names passed as nodemgr argument take precedence.
        If not specified by argument, objet names found in the
        configuration file section are returned.
        """
        if len(self.options.objects) > 0:
            return self.options.objects
        if self.config and self.config.has_option(section, "objects"):
            return self.config.get(section, "objects").split(",")
        return []

    def collect_stats(self):
        """
        Do the stats collection if the scheduler constraints permit.
        """
        if self.sched.skip_action("collect_stats"):
            return
        self.task_collect_stats()

    @scheduler_fork
    def task_collect_stats(self):
        """
        Choose the os specific stats collection module and call its collect
        method.
        """
        try:
            mod = __import__("rcStatsCollect"+rcEnv.sysname)
        except ImportError:
            return
        mod.collect(self)

    def pushstats(self):
        """
        Set stats range to push to "last pushstat => now"
        """
        fpath = self.sched.get_timestamp_f(self.sched.scheduler_actions["pushstats"].fname)
        try:
            with open(fpath, "r") as ofile:
                buff = ofile.read()
            start = datetime.datetime.strptime(buff, "%Y-%m-%d %H:%M:%S.%f\n")
            now = datetime.datetime.now()
            delta = now - start
            interval = delta.days * 1440 + delta.seconds // 60 + 10
        except:
            interval = 1450
        if interval < 21:
            interval = 21

        if self.sched.skip_action("pushstats"):
            return
        self.task_pushstats(interval)

    @scheduler_fork
    def task_pushstats(self, interval):
        """
        The scheduled task that collects system statistics from system tools
        like sar, and sends the data to the collector.
        A list of metrics can be disabled from the task configuration section,
        using the 'disable' option.
        """
        def get_disable_stats():
            """
            Returns the list of stats metrics collection disabled through the
            configuration file stats.disable option.
            """
            if not self.config.has_option("stats", "disable"):
                return []
            disable = self.config.get("stats", "disable")
            try:
                return json.loads(disable)
            except ValueError:
                pass
            if ',' in disable:
                return disable.replace(' ', '').split(',')
            return disable.split()

        disable = get_disable_stats()
        return self.collector.call('push_stats',
                                   stats_dir=self.options.stats_dir,
                                   stats_start=self.options.begin,
                                   stats_end=self.options.end,
                                   interval=interval,
                                   disable=disable)

    def pushpkg(self):
        """
        The pushpkg action entrypoint.
        Inventories the installed packages.
        """
        if self.sched.skip_action("pushpkg"):
            return
        self.task_pushpkg()

    @scheduler_fork
    def task_pushpkg(self):
        """
        The pushpkg scheduler task.
        """
        self.collector.call('push_pkg')

    def pushpatch(self):
        """
        The pushpatch action entrypoint.
        Inventories the installed patches.
        """
        if self.sched.skip_action("pushpatch"):
            return
        self.task_pushpatch()

    @scheduler_fork
    def task_pushpatch(self):
        """
        The pushpatch scheduler task.
        """
        self.collector.call('push_patch')

    def pushasset(self):
        """
        The pushasset action entrypoint.
        Inventories the server properties.
        """
        if self.sched.skip_action("pushasset"):
            return
        self.task_pushasset()

    @scheduler_fork
    def task_pushasset(self):
        """
        The pushasset scheduler task.
        """
        self.collector.call('push_asset', self)

    def pushnsr(self):
        """
        The pushnsr action entrypoint.
        Inventories Networker Backup Server index databases.
        """
        if self.sched.skip_action("pushnsr"):
            return
        self.task_pushnsr()

    @scheduler_fork
    def task_pushnsr(self):
        """
        The pushnsr scheduler task.
        """
        self.collector.call('push_nsr')

    def pushhp3par(self):
        """
        The push3par action entrypoint.
        Inventories HP 3par storage arrays.
        """
        if self.sched.skip_action("pushhp3par"):
            return
        self.task_pushhp3par()

    @scheduler_fork
    def task_pushhp3par(self):
        """
        The push3par scheduler task.
        """
        self.collector.call('push_hp3par', self.options.objects)

    def pushnetapp(self):
        """
        The pushnetapp action entrypoint.
        Inventories NetApp storage arrays.
        """
        if self.sched.skip_action("pushnetapp"):
            return
        self.task_pushnetapp()

    @scheduler_fork
    def task_pushnetapp(self):
        """
        The pushnetapp scheduler task.
        """
        self.collector.call('push_netapp', self.options.objects)

    def pushcentera(self):
        """
        The pushcentera action entrypoint.
        Inventories Centera storage arrays.
        """
        if self.sched.skip_action("pushcentera"):
            return
        self.task_pushcentera()

    @scheduler_fork
    def task_pushcentera(self):
        """
        The pushcentera scheduler task.
        """
        self.collector.call('push_centera', self.options.objects)

    def pushemcvnx(self):
        """
        The pushemcvnx action entrypoint.
        Inventories EMC VNX storage arrays.
        """
        if self.sched.skip_action("pushemcvnx"):
            return
        self.task_pushemcvnx()

    @scheduler_fork
    def task_pushemcvnx(self):
        """
        The pushemcvnx scheduler task.
        """
        self.collector.call('push_emcvnx', self.options.objects)

    def pushibmds(self):
        """
        The pushibmds action entrypoint.
        Inventories IBM DS storage arrays.
        """
        if self.sched.skip_action("pushibmds"):
            return
        self.task_pushibmds()

    @scheduler_fork
    def task_pushibmds(self):
        """
        The pushibmds scheduler task.
        """
        self.collector.call('push_ibmds', self.options.objects)

    def pushgcedisks(self):
        """
        The pushgcedisks action entrypoint.
        Inventories Google Compute Engine disks.
        """
        if self.sched.skip_action("pushgcedisks"):
            return
        self.task_pushgcedisks()

    @scheduler_fork
    def task_pushgcedisks(self):
        """
        The pushgce scheduler task.
        """
        self.collector.call('push_gcedisks', self.options.objects)

    def pushfreenas(self):
        """
        The pushfreenas action entrypoint.
        Inventories FreeNas storage arrays.
        """
        if self.sched.skip_action("pushfreenas"):
            return
        self.task_pushfreenas()

    @scheduler_fork
    def task_pushfreenas(self):
        """
        The pushfreenas scheduler task.
        """
        self.collector.call('push_freenas', self.options.objects)

    def pushxtremio(self):
        """
        The pushxtremio action entrypoint.
        Inventories XtremIO storage arrays.
        """
        if self.sched.skip_action("pushxtremio"):
            return
        self.task_pushxtremio()

    @scheduler_fork
    def task_pushxtremio(self):
        """
        The pushxtremio scheduler task.
        """
        self.collector.call('push_xtremio', self.options.objects)

    def pushdcs(self):
        """
        The pushdcs action entrypoint.
        Inventories DataCore SAN Symphony storage arrays.
        """
        if self.sched.skip_action("pushdcs"):
            return
        self.task_pushdcs()

    @scheduler_fork
    def task_pushdcs(self):
        """
        The pushdcs scheduler task.
        """
        self.collector.call('push_dcs', self.options.objects)

    def pushhds(self):
        """
        The pushhds action entrypoint.
        Inventories Hitachi storage arrays.
        """
        if self.sched.skip_action("pushhds"):
            return
        self.task_pushhds()

    @scheduler_fork
    def task_pushhds(self):
        """
        The pushhds scheduler task.
        """
        self.collector.call('push_hds', self.options.objects)

    def pushnecism(self):
        """
        The pushnecism action entrypoint.
        Inventories NEC iSM storage arrays.
        """
        if self.sched.skip_action("pushnecism"):
            return
        self.task_pushnecism()

    @scheduler_fork
    def task_pushnecism(self):
        """
        The pushnecism scheduler task.
        """
        self.collector.call('push_necism', self.options.objects)

    def pusheva(self):
        """
        The pusheva action entrypoint.
        Inventories HP EVA storage arrays.
        """
        if self.sched.skip_action("pusheva"):
            return
        self.task_pusheva()

    @scheduler_fork
    def task_pusheva(self):
        """
        The pusheva scheduler task.
        """
        self.collector.call('push_eva', self.options.objects)

    def pushibmsvc(self):
        """
        The pushibmsvc action entrypoint.
        Inventories IBM SVC storage arrays.
        """
        if self.sched.skip_action("pushibmsvc"):
            return
        self.task_pushibmsvc()

    @scheduler_fork
    def task_pushibmsvc(self):
        """
        The pushibmsvc scheduler task.
        """
        self.collector.call('push_ibmsvc', self.options.objects)

    def pushvioserver(self):
        """
        The pushvioserver action entrypoint.
        Inventories IBM vio server storage arrays.
        """
        if self.sched.skip_action("pushvioserver"):
            return
        self.task_pushvioserver()

    @scheduler_fork
    def task_pushvioserver(self):
        """
        The pushvioserver scheduler task.
        """
        self.collector.call('push_vioserver', self.options.objects)

    def pushsym(self):
        """
        The pushsym action entrypoint.
        Inventories EMC Symmetrix server storage arrays.
        """
        if self.sched.skip_action("pushsym"):
            return
        self.task_pushsym()

    @scheduler_fork
    def task_pushsym(self):
        """
        The pushsym scheduler task.
        """
        objects = self.get_push_objects("sym")
        self.collector.call('push_sym', objects)

    def pushbrocade(self):
        """
        The pushsym action entrypoint.
        Inventories Brocade SAN switches.
        """
        if self.sched.skip_action("pushbrocade"):
            return
        self.task_pushbrocade()

    @scheduler_fork
    def task_pushbrocade(self):
        """
        The pushbrocade scheduler task.
        """
        self.collector.call('push_brocade', self.options.objects)

    def auto_rotate_root_pw(self):
        """
        The rotate_root_pw node action entrypoint.
        """
        if self.sched.skip_action("auto_rotate_root_pw"):
            return
        self.task_auto_rotate_root_pw()

    @scheduler_fork
    def task_auto_rotate_root_pw(self):
        """
        The rotate root password scheduler task.
        """
        self.rotate_root_pw()

    def unschedule_reboot(self):
        """
        Unflag the node for reboot during the next allowed period.
        """
        if not os.path.exists(self.paths.reboot_flag):
            print("reboot already not scheduled")
            return
        os.unlink(self.paths.reboot_flag)
        print("reboot unscheduled")

    def schedule_reboot(self):
        """
        Flag the node for reboot during the next allowed period.
        """
        if not os.path.exists(self.paths.reboot_flag):
            with open(self.paths.reboot_flag, "w") as ofile:
                ofile.write("")
        import stat
        statinfo = os.stat(self.paths.reboot_flag)
        if statinfo.st_uid != 0:
            os.chown(self.paths.reboot_flag, 0, -1)
            print("set %s root ownership"%self.paths.reboot_flag)
        if statinfo.st_mode & stat.S_IWOTH:
            mode = statinfo.st_mode ^ stat.S_IWOTH
            os.chmod(self.paths.reboot_flag, mode)
            print("set %s not world-writable"%self.paths.reboot_flag)
        print("reboot scheduled")

    def schedule_reboot_status(self):
        """
        Display information about the next scheduled reboot
        """
        import stat
        if os.path.exists(self.paths.reboot_flag):
            statinfo = os.stat(self.paths.reboot_flag)
        else:
            statinfo = None

        if statinfo is None or \
           statinfo.st_uid != 0 or statinfo.st_mode & stat.S_IWOTH:
            print("reboot is not scheduled")
        else:
            sch = self.sched.scheduler_actions["auto_reboot"]
            schedule = self.sched.sched_get_schedule_raw(sch.section, sch.schedule_option)
            print("reboot is scheduled")
            print("reboot schedule: %s" % schedule)

        result = self.sched.get_next_schedule("auto_reboot")
        if result["next_sched"]:
            print("next reboot slot:",
                  result["next_sched"].strftime("%a %Y-%m-%d %H:%M"))
        else:
            print("next reboot slot: none in the next %d days" % (result["minutes"]/144))

    def auto_reboot(self):
        """
        The scheduler task executing the node reboot if the scheduler
        constraints are satisfied and the reboot flag is set.
        """
        if self.sched.skip_action("auto_reboot"):
            return
        self.task_auto_reboot()

    @scheduler_fork
    def task_auto_reboot(self):
        """
        Reboot the node if the reboot flag is set.
        """
        if not os.path.exists(self.paths.reboot_flag):
            print("%s is not present. no reboot scheduled" % self.paths.reboot_flag)
            return
        import stat
        statinfo = os.stat(self.paths.reboot_flag)
        if statinfo.st_uid != 0:
            print("%s does not belong to root. abort scheduled reboot" % self.paths.reboot_flag)
            return
        if statinfo.st_mode & stat.S_IWOTH:
            print("%s is world writable. abort scheduled reboot" % self.paths.reboot_flag)
            return
        print("remove %s and reboot" % self.paths.reboot_flag)
        os.unlink(self.paths.reboot_flag)
        self.reboot()

    def pushdisks(self):
        """
        The pushdisks node action entrypoint.
        """
        if self.sched.skip_action("pushdisks"):
            return
        self.task_pushdisks()

    @scheduler_fork
    def task_pushdisks(self):
        """
        Send to the collector the list of disks visible on this node, and
        their attributes.
        """
        if self.svcs is None:
            self.build_services()
        self.collector.call('push_disks', self)

    def shutdown(self):
        """
        The shutdown node action entrypoint.
        To be overloaded by child classes.
        """
        self.log.warning("to be implemented")

    def reboot(self):
        """
        The reboot node action entrypoint.
        """
        self.do_triggers("reboot", "pre")
        self.log.info("reboot")
        self._reboot()

    def do_triggers(self, action, when):
        """
        Determine which triggers need to be executed for the action and
        executes them when appropriate.
        """
        trigger = None
        blocking_trigger = None
        try:
            trigger = self.config.get(action, when)
        except:
            pass
        try:
            blocking_trigger = self.config.get(action, "blocking_"+when)
        except:
            pass
        if trigger:
            self.log.info("execute trigger %s", trigger)
            try:
                self.do_trigger(trigger)
            except ex.excError:
                pass
        if blocking_trigger:
            self.log.info("execute blocking trigger %s", trigger)
            try:
                self.do_trigger(blocking_trigger)
            except ex.excError:
                if when == "pre":
                    self.log.error("blocking pre trigger error: abort %s", action)
                raise

    def do_trigger(self, cmd, err_to_warn=False):
        """
        The trigger execution wrapper.
        """
        import shlex
        _cmd = shlex.split(cmd)
        ret, out, err = self.vcall(_cmd, err_to_warn)
        if ret != 0:
            raise ex.excError((ret, out, err))

    def _reboot(self):
        """
        A system reboot method to be implemented by child classes.
        """
        self.log.warning("to be implemented")

    def sysreport(self):
        """
        The sysreport node action entrypoint.
        """
        if self.sched.skip_action("sysreport"):
            return
        try:
            self.task_sysreport()
        except (OSError, ex.excError) as exc:
            print(exc)
            return 1

    @scheduler_fork
    def task_sysreport(self):
        """
        Send to the collector a tarball of the files the user wants to track
        that changed since the last call.
        If the force option is set, send all files the user wants to track.
        """
        try:
            mod = __import__('rcSysReport'+rcEnv.sysname)
        except ImportError:
            print("sysreport is not supported on this os")
            return
        mod.SysReport(node=self).sysreport(force=self.options.force)

    def get_prkey(self):
        """
        Returns the persistent reservation key.
        Once generated from the algorithm, the prkey is written to the config
        file to ensure its stability.
        """
        if self.config.has_option("node", "prkey"):
            hostid = self.config.get("node", "prkey")
            if len(hostid) > 18 or not hostid.startswith("0x") or \
               len(set(hostid[2:]) - set("0123456789abcdefABCDEF")) > 0:
                raise ex.excError("prkey in node.conf must have 16 significant"
                                  " hex digits max (ex: 0x90520a45138e85)")
            return hostid
        self.log.info("can't find a prkey forced in node.conf. generate one.")
        hostid = "0x"+self.hostid()
        self.config.set('node', 'prkey', hostid)
        self.write_config()
        return hostid

    def prkey(self):
        """
        Print the persistent reservation key.
        """
        print(self.get_prkey())

    @staticmethod
    def hostid():
        """
        Return a stable host unique id
        """
        mod = __import__('hostid'+rcEnv.sysname)
        return mod.hostid()

    def checks(self):
        """
        The checks node action entrypoint.
        """
        if self.sched.skip_action("checks"):
            return
        self.task_checks()

    @scheduler_fork
    def task_checks(self):
        """
        Runs health checks.
        """
        import checks
        if self.svcs is None:
            self.build_services()
        checkers = checks.checks(self.svcs)
        checkers.node = self
        checkers.do_checks()

    def wol(self):
        """
        Send a Wake-On-LAN packet to a mac address on the broadcast address.
        """
        import rcWakeOnLan
        if self.options.mac is None:
            print("missing parameter. set --mac argument. multiple mac "
                  "addresses must be separated by comma", file=sys.stderr)
            print("example 1 : --mac 00:11:22:33:44:55", file=sys.stderr)
            print("example 2 : --mac 00:11:22:33:44:55,66:77:88:99:AA:BB",
                  file=sys.stderr)
            return 1
        if self.options.broadcast is None:
            print("missing parameter. set --broadcast argument. needed to "
                  "identify accurate network to use", file=sys.stderr)
            print("example 1 : --broadcast 10.25.107.255", file=sys.stderr)
            print("example 2 : --broadcast 192.168.1.5,10.25.107.255",
                  file=sys.stderr)
            return 1
        macs = self.options.mac.split(',')
        broadcasts = self.options.broadcast.split(',')
        for brdcast in broadcasts:
            for mac in macs:
                req = rcWakeOnLan.wolrequest(macaddress=mac, broadcast=brdcast)
                if not req.check_broadcast():
                    print("Error : skipping broadcast address <%s>, not in "
                          "the expected format 123.123.123.123" % req.broadcast,
                          file=sys.stderr)
                    break
                if not req.check_mac():
                    print("Error : skipping mac address <%s>, not in the "
                          "expected format 00:11:22:33:44:55" % req.mac,
                          file=sys.stderr)
                    continue
                if req.send():
                    print("Sent Wake On Lan packet to mac address <%s>"%req.mac)
                else:
                    print("Error while trying to send Wake On Lan packet to "
                          "mac address <%s>" % req.mac, file=sys.stderr)

    def unset(self):
        """
        Unset an option in the node configuration file.
        """
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) != 2:
            print("malformed parameter. format as 'section.key'",
                  file=sys.stderr)
            return 1
        section, option = elements
        if not self.config.has_section(section):
            print("section '%s' not found" % section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section '%s'" % (option, section),
                  file=sys.stderr)
            return 1
        self.config.remove_option(section, option)
        self.write_config()
        return 0

    def get(self):
        """
        Print the raw value of any option of any section of the node
        configuration file.
        """
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) != 2:
            print("malformed parameter. format as 'section.key'",
                  file=sys.stderr)
            return 1
        section, option = elements

        if not self.config.has_section(section):
            self.config.add_section(section)

        if self.config.has_option(section, option):
            print(self.config.get(section, option))
            return 0
        else:
            if self.options.param in DEPRECATED_KEYWORDS:
                newkw = DEPRECATED_KEYWORDS[self.options.param]
                if self.config.has_option(section, newkw):
                    print("deprecated keyword %s translated to %s" % \
                          (self.options.param, newkw), file=sys.stderr)
                    print(self.config.get(section, newkw))
                    return 0
            if self.options.param in REVERSE_DEPRECATED_KEYWORDS:
                for oldkw in REVERSE_DEPRECATED_KEYWORDS[self.options.param]:
                    if self.config.has_option(section, oldkw):
                        print("keyword %s not found, translated to deprecated %s" % \
                              (self.options.param, oldkw), file=sys.stderr)
                        print(self.config.get(section, oldkw))
                        return 0
            print("option '%s' not found in section '%s'"%(option, section),
                  file=sys.stderr)
            return 1

    def set(self):
        """
        Set any option in any section of the node configuration file
        """
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        if self.options.value is None:
            print("no value. set --value", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) != 2:
            print("malformed parameter. format as 'section.key'",
                  file=sys.stderr)
            return 1
        section, option = elements
        if not self.config.has_section(section):
            try:
                self.config.add_section(section)
            except ValueError as exc:
                print(exc)
                return 1
        self.config.set(section, option, self.options.value)
        self.write_config()
        return 0

    def register_as_user(self):
        """
        Returns a node registration unique id, authenticating to the
        collector as a user.
        """
        data = self.collector.call('register_node')
        if data is None:
            raise ex.excError("failed to obtain a registration number")
        elif isinstance(data, dict) and "ret" in data and data["ret"] != 0:
            msg = "failed to obtain a registration number"
            if "msg" in data and len(data["msg"]) > 0:
                msg += "\n" + data["msg"]
            raise ex.excError(msg)
        elif isinstance(data, list):
            raise ex.excError(data[0])
        return data

    def register_as_node(self):
        """
        Returns a node registration unique id, authenticating to the
        collector as a node.
        """
        try:
            data = self.collector_rest_post("/register", {
                "nodename": rcEnv.nodename,
                "app": self.options.app
            })
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])
        return data["data"]["uuid"]

    def register(self):
        """
        Do anonymous or indentified node register to obtain a node uuid
        that will be used as a password valid for the current hostname used
        as a username in the application code context.
        """
        if self.options.user is None:
            register_fn = "register_as_user"
        else:
            register_fn = "register_as_node"
        try:
            rcEnv.uuid = getattr(self, register_fn)()
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            return 1

        if not self.config.has_section('node'):
            self.config.add_section('node')
        self.config.set('node', 'uuid', rcEnv.uuid)

        try:
            self.write_config()
        except ex.excError:
            print("failed to write registration number: %s" % rcEnv.uuid,
                  file=sys.stderr)
            return 1

        print("registered")
        self.pushasset()
        self.pushdisks()
        self.pushpkg()
        self.pushpatch()
        self.sysreport()
        self.checks()
        return 0

    def service_action_worker(self, svc, action, options):
        """
        The method the per-service subprocesses execute
        """
        try:
            ret = svc.action(action, options)
        except ex.MonitorAction:
            self.close()
            sys.exit(self.ex_monitor_action_exit_code)
        finally:
            self.close()
            sys.exit(1)
        self.close()
        sys.exit(ret)

    @staticmethod
    def devlist(tree=None):
        """
        Return the node's top-level device paths
        """
        if tree is None:
            try:
                mod = __import__("rcDevTree"+rcEnv.sysname)
            except ImportError:
                return
            tree = mod.DevTree()
            tree.load()
        devpaths = []
        for dev in tree.get_top_devs():
            if len(dev.devpath) > 0:
                devpaths.append(dev.devpath[0])
        return devpaths

    def updatecomp(self):
        """
        Downloads and installs the compliance module archive from the url
        specified as node.repocomp or node.repo in node.conf.
        """
        if self.config.has_option('node', 'repocomp'):
            pkg_name = self.config.get('node', 'repocomp').strip('/') + "/current"
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + "/compliance/current"
        else:
            if self.options.cron:
                return 0
            print("node.repo or node.repocomp must be set in node.conf",
                  file=sys.stderr)
            return 1
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        fpath = tmpf.name
        tmpf.close()
        try:
            ret = self._updatecomp(pkg_name, fpath)
        finally:
            if os.path.exists(fpath):
                os.unlink(fpath)
        return ret

    def _updatecomp(self, pkg_name, fpath):
        """
        Downloads and installs the compliance module archive from the url
        specified by the pkg_name argument. The download destination file
        is specified by fpath. The caller is responsible for its deletion.
        """
        print("get %s (%s)"%(pkg_name, fpath))
        try:
            self.urlretrieve(pkg_name, fpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            if self.options.cron:
                return 0
            return 1
        tmpp = os.path.join(rcEnv.paths.pathtmp, 'compliance')
        backp = os.path.join(rcEnv.paths.pathtmp, 'compliance.bck')
        compp = os.path.join(rcEnv.paths.pathvar, 'compliance')
        if not os.path.exists(compp):
            os.makedirs(compp, 0o755)
        import shutil
        try:
            shutil.rmtree(backp)
        except (OSError, IOError):
            pass
        print("extract compliance in", rcEnv.paths.pathtmp)
        import tarfile
        tar = tarfile.open(fpath)
        os.chdir(rcEnv.paths.pathtmp)
        try:
            tar.extractall()
            tar.close()
        except (OSError, IOError):
            print("failed to unpack", file=sys.stderr)
            return 1
        print("install new compliance")
        for root, dirs, files in os.walk(tmpp):
            for fpath in dirs:
                os.chown(os.path.join(root, fpath), 0, 0)
                for fpath in files:
                    os.chown(os.path.join(root, fpath), 0, 0)
        shutil.move(compp, backp)
        shutil.move(tmpp, compp)
        return 0

    def updatepkg(self):
        """
        Downloads and upgrades the OpenSVC agent, using the system-specific
        packaging tools.
        """
        modname = 'rcUpdatePkg'+rcEnv.sysname
        if not os.path.exists(os.path.join(rcEnv.paths.pathlib, modname+'.py')):
            print("updatepkg not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        mod = __import__(modname)
        if self.config.has_option('node', 'repopkg'):
            pkg_name = self.config.get('node', 'repopkg').strip('/') + \
                       "/" + mod.repo_subdir + '/current'
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + \
                       "/packages/" + mod.repo_subdir + '/current'
        else:
            print("node.repo or node.repopkg must be set in node.conf",
                  file=sys.stderr)
            return 1
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        fpath = tmpf.name
        tmpf.close()
        print("get %s (%s)"%(pkg_name, fpath))
        try:
            self.urlretrieve(pkg_name, fpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            try:
                os.unlink(fpath)
            except OSError:
                pass
            return 1
        print("updating opensvc")
        mod.update(fpath)
        print("clean up")
        try:
            os.unlink(fpath)
        except OSError:
            pass
        self.action("pushasset")
        self.build_services()
        for svc in self.svcs:
            svc.set_run_flag()
        return 0

    def array(self):
        """
        Execute a array command, passing extra_argv to the array driver.
        """
        array_name = None
        for idx, arg in enumerate(self.options.extra_argv):
            if arg.startswith("--array="):
                array_name = arg[arg.index("=")+1:]
                break
            if (arg == "-a" or arg == "--array") and idx+1 < len(self.options.extra_argv):
                array_name = self.options.extra_argv[idx+1]
                break

        if array_name is None:
            raise ex.excError("can not determine array driver (no --array)")

        self.load_auth_config()

        section = None

        for _section in self.auth_config.sections():
            if _section == array_name:
                section = _section
                break
            if self.auth_config.has_option(_section, "array") and \
               array_name in self.auth_config.get(_section, "array").split():
                section = _section
                break

        if section is None:
            raise ex.excError("array '%s' not found in %s" % (array_name, rcEnv.paths.authconf))

        if not self.auth_config.has_option(section, "type"):
            raise ex.excError("%s must have a '%s.type' option" % (rcEnv.paths.authconf, section))

        driver = self.auth_config.get(section, "type")
        rtype = driver[0].upper() + driver[1:].lower()
        modname = "rc" + rtype
        try:
            mod = __import__(modname)
        except ImportError as exc:
            raise ex.excError("driver %s load error: %s" % (modname, str(exc)))
        return mod.main(self.options.extra_argv, node=self)

    def get_ruser(self, node):
        """
        Returns the remote user to use for remote commands on the node
        specified as argument.
        If not specified as node.ruser in the node configuration file,
        the root user is returned.
        """
        default = "root"
        if not self.config.has_option('node', "ruser"):
            return default
        node_ruser = {}
        elements = self.config.get('node', 'ruser').split()
        for element in elements:
            subelements = element.split("@")
            if len(subelements) == 1:
                default = element
            elif len(subelements) == 2:
                _ruser, _node = subelements
                node_ruser[_node] = _ruser
            else:
                continue
        if node in node_ruser:
            return node_ruser[node]
        return default

    def dequeue_actions(self):
        """
        The dequeue_actions node action entrypoint.
        """
        if self.sched.skip_action("dequeue_actions"):
            return
        self.task_dequeue_actions()

    @scheduler_fork
    def task_dequeue_actions(self):
        """
        Poll the collector action queue until emptied.
        """
        actions = self.collector.call('collector_get_action_queue')
        if actions is None:
            return "unable to fetch actions scheduled by the collector"
        import re
        regex = re.compile(r"\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[m|K|G]", re.UNICODE)
        data = []
        for action in actions:
            ret, out, err = self.dequeue_action(action)
            out = regex.sub('', out)
            err = regex.sub('', err)
            data.append((action.get('id'), ret, out, err))
        if len(actions) > 0:
            self.collector.call('collector_update_action_queue', data)

    @staticmethod
    def dequeue_action(action):
        """
        Execute the nodemgr or svcmgr action described in payload element
        received from the collector's action queue.
        """
        if action.get("svcname") is None or action.get("svcname") == "":
            cmd = [rcEnv.paths.nodemgr]
        else:
            cmd = [rcEnv.paths.svcmgr, "-s", action.get("svcname")]
        import shlex
        cmd += shlex.split(action.get("command", ""))
        print("dequeue action %s" % " ".join(cmd))
        out, err, ret = justcall(cmd)
        return ret, out, err

    def rotate_root_pw(self):
        """
        Generate a random password, send it to the collector and set it as
        the root user password.
        """
        passwd = self.genpw()

        from collector import Collector
        coll = Collector(self.options, self)
        try:
            getattr(coll, 'rotate_root_pw')(passwd)
        except Exception as exc:
            print("unexpected error sending the new password to the collector "
                  "(%s). Abording password change." % str(exc), file=sys.stderr)
            return 1

        try:
            mod = __import__('rcPasswd'+rcEnv.sysname)
        except ImportError:
            print("not implemented")
            return 1
        ret = mod.change_root_pw(passwd)
        if ret == 0:
            print("root password changed")
        else:
            print("failed to change root password")
        return ret

    @staticmethod
    def genpw():
        """
        Returns a random password.
        """
        import string
        chars = string.letters + string.digits + r'+/'
        assert 256 % len(chars) == 0
        pwd_len = 16
        return ''.join(chars[ord(c) % len(chars)] for c in os.urandom(pwd_len))

    def scanscsi(self):
        """
        Rescans the scsi host buses for new logical units discovery.
        """
        try:
            mod = __import__("rcDiskInfo"+rcEnv.sysname)
        except ImportError:
            print("scanscsi is not supported on", rcEnv.sysname, file=sys.stderr)
            return 1
        diskinfo = mod.diskInfo()
        if not hasattr(diskinfo, 'scanscsi'):
            print("scanscsi is not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        return diskinfo.scanscsi(
            hba=self.options.hba,
            target=self.options.target,
            lun=self.options.lun,
        )

    def discover(self):
        """
        Auto configures services wrapping cloud compute instances
        """
        self.cloud_init()

    def cloud_init(self):
        """
        Initializes a cloud object for each cloud seaction in the configuration
        file.
        """
        ret = 0
        for section in self.config.sections():
            try:
                self.cloud_init_section(section)
            except ex.excInitError as exc:
                print(str(exc), file=sys.stderr)
                ret |= 1
        return ret

    def cloud_get(self, section):
        """
        Get the cloud object instance handling the config file section passed
        as argument. If not already instanciated, create the instance and
        store it in a dict hashed by section name.
        """
        if not section.startswith("cloud"):
            return

        if not section.startswith("cloud#"):
            raise ex.excInitError("cloud sections must have a unique name in "
                                  "the form '[cloud#n] in %s" % rcEnv.paths.nodeconf)

        if self.clouds and section in self.clouds:
            return self.clouds[section]

        if not self.config.has_option(section, "type"):
            raise ex.excInitError("type option is mandatory in cloud section "
                                  "in %s" % rcEnv.paths.nodeconf)
        cloud_type = self.config.get(section, 'type')

        if len(cloud_type) == 0:
            raise ex.excInitError("invalid cloud type in %s"%rcEnv.paths.nodeconf)

        self.load_auth_config()
        if not self.auth_config.has_section(section):
            raise ex.excInitError("%s must have a '%s' section" % (rcEnv.paths.authconf, section))

        auth_dict = {}
        for key, val in self.auth_config.items(section):
            auth_dict[key] = val

        mod_name = "rcCloud" + cloud_type[0].upper() + cloud_type[1:].lower()

        try:
            mod = __import__(mod_name)
        except ImportError:
            raise ex.excInitError("cloud type '%s' is not supported"%cloud_type)

        if self.clouds is None:
            self.clouds = {}
        cloud = mod.Cloud(section, auth_dict)
        self.clouds[section] = cloud
        return cloud

    def cloud_init_section(self, section):
        """
        Detects all cloud instances in the section, and init a service for each
        """
        cloud = self.cloud_get(section)

        if cloud is None:
            return

        cloud_id = cloud.cloud_id()
        svcnames = cloud.list_svcnames()

        self.cloud_purge_services(cloud_id, [x[1] for x in svcnames])

        for vmname, svcname in svcnames:
            self.cloud_init_service(cloud, vmname, svcname)

    @staticmethod
    def cloud_purge_services(suffix, svcnames):
        """
        Purge a lingering service no longer detected in the cloud.
        """
        import glob
        fpaths = glob.glob(os.path.join(rcEnv.paths.pathetc, '*.conf'))
        for fpath in fpaths:
            svcname = os.path.basename(fpath)[:-5]
            if svcname.endswith(suffix) and svcname not in svcnames:
                print("purge_service(svcname)", svcname)

    @staticmethod
    def cloud_init_service(cloud, vmname, svcname):
        """
        Init a service for a detected cloud instance.
        """
        import glob
        fpaths = glob.glob(os.path.join(rcEnv.paths.pathetc, '*.conf'))
        fpath = os.path.join(rcEnv.paths.pathetc, svcname+'.conf')
        if fpath in fpaths:
            print(svcname, "is already defined")
            return
        print("initialize", svcname)

        defaults = {
            "app": cloud.app_id(svcname),
            "mode": cloud.mode,
            "nodes": rcEnv.nodename,
            "service_type": "TST",
            "vm_name": vmname,
            "cloud_id": cloud.cid,
        }
        config = RawConfigParser(defaults)

        try:
            ofile = open(fpath, 'w')
            config.write(ofile)
            ofile.close()
        except:
            print("failed to write %s"%fpath, file=sys.stderr)
            raise Exception()

        basename = fpath[:-5]
        launchers_d = basename + '.dir'
        launchers_l = basename + '.d'
        try:
            os.makedirs(launchers_d)
        except OSError:
            pass
        try:
            os.symlink(launchers_d, launchers_l)
        except OSError:
            pass
        try:
            os.symlink(rcEnv.paths.svcmgr, basename)
        except OSError:
            pass

    def can_parallel(self, action, options):
        """
        Returns True if the action can be run in a subprocess per service
        """
        if options.parallel and action not in ACTIONS_NO_PARALLEL:
            return True
        return False

    @staticmethod
    def action_need_aggregate(action):
        """
        Returns True if the action returns data from multiple sources (nodes
        or services) to arrange for display.
        """
        if action.startswith("print_"):
            return True
        if action.startswith("json_"):
            return True
        if action.startswith("collector_"):
            return True
        if "_json_" in action:
            return True
        return False

    def do_svcs_action(self, action, options):
        """
        The services action wrapper.
        Takes care of
        * parallelization of the action in per-service subprocesses
        * collection and aggregation of returned data and errors
        """
        if action == "ls":
            for svc in self.svcs:
                print(svc.svcname)
            return

        err = 0
        data = Storage()
        data.outs = {}
        need_aggregate = self.action_need_aggregate(action)

        # generic cache janitoring
        purge_cache()
        self.log.debug("session uuid: %s", rcEnv.session_uuid)

        if action in ACTIONS_NO_MULTIPLE_SERVICES and len(self.svcs) > 1:
            print("action '%s' is not allowed on multiple services" % action, file=sys.stderr)
            return 1

        if self.can_parallel(action, options):
            from multiprocessing import Process
            if rcEnv.sysname == "Windows":
                from multiprocessing import set_executable
                set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
            data.procs = {}
            data.svcs = {}

        for svc in self.svcs:
            if self.can_parallel(action, options):
                data.svcs[svc.svcname] = svc
                data.procs[svc.svcname] = Process(
                    target=self.service_action_worker,
                    name='worker_'+svc.svcname,
                    args=[svc, action, options],
                )
                data.procs[svc.svcname].start()
            else:
                try:
                    ret = svc.action(action, options)
                    if need_aggregate:
                        if ret is not None:
                            data.outs[svc.svcname] = ret
                    else:
                        if ret is None:
                            ret = 0
                        err += ret
                except ex.MonitorAction:
                    svc.action('toc')
                except ex.excSignal:
                    break

        if self.can_parallel(action, options):
            for svcname in data.procs:
                data.procs[svcname].join()
                ret = data.procs[svcname].exitcode
                if ret == self.ex_monitor_action_exit_code:
                    data.svcs[svcname].action('toc')
                elif ret > 0:
                    # r is negative when data.procs[svcname] is killed by signal.
                    # in this case, we don't want to decrement the err counter.
                    err += ret

        if need_aggregate:
            if self.options.single_service:
                svcname = self.svcs[0].svcname
                if svcname not in data.outs:
                    return 1
                self.print_data(data.outs[svcname])
            else:
                self.print_data(data.outs)

        return err

    def collector_cli(self):
        """
        The collector cli entrypoint.
        """
        data = {}

        if os.getuid() == 0:
            if self.options.user is None:
                user, password = self.collector_auth_node()
                data["user"] = user
                data["password"] = password
            if self.options.api is None:
                if rcEnv.dbopensvc is None:
                    raise ex.excError("node.dbopensvc is not set in node.conf")
                data["api"] = rcEnv.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        from rcCollectorCli import Cli
        cli = Cli(**data)
        return cli.run()

    def collector_api(self, svcname=None):
        """
        Prepare the authentication info, either as node or as user.
        Fetch and cache the collector's exposed rest api metadata.
        """
        if rcEnv.dbopensvc is None:
            raise ex.excError("node.dbopensvc is not set in node.conf")
        data = {}
        if self.options.user is None:
            username, password = self.collector_auth_node()
            if svcname:
                username = svcname+"@"+username
        else:
            username, password = self.collector_auth_user()
        data["username"] = username
        data["password"] = password
        data["url"] = rcEnv.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        return data

    def collector_auth_node(self):
        """
        Returns the authentcation info for login as node
        """
        username = rcEnv.nodename
        if not self.config.has_option("node", "uuid"):
            raise ex.excError("the node is not registered yet. use 'nodemgr register [--user <user>]'")
        password = self.config.get("node", "uuid")
        return username, password

    def collector_auth_user(self):
        """
        Returns the authentcation info for login as user
        """
        username = self.options.user

        if self.options.password and self.options.password != "?":
            return username, self.options.password

        import getpass
        try:
            password = getpass.getpass()
        except EOFError:
            raise KeyboardInterrupt()
        return username, password

    def collector_request(self, path, svcname=None):
        """
        Make a request to the collector's rest api
        """
        import base64
        api = self.collector_api(svcname=svcname)
        url = api["url"]
        if not url.startswith("https"):
            raise ex.excError("refuse to submit auth tokens through a "
                              "non-encrypted transport")
        request = Request(url+path)
        auth_string = '%s:%s' % (api["username"], api["password"])
        if sys.version_info[0] >= 3:
            base64string = base64.encodestring(auth_string.encode()).decode()
        else:
            base64string = base64.encodestring(auth_string)
        base64string = base64string.replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        return request

    def collector_rest_get(self, path, data=None, svcname=None):
        """
        Make a GET request to the collector's rest api
        """
        return self.collector_rest_request(path, data=data, svcname=svcname)

    def collector_rest_post(self, path, data=None, svcname=None):
        """
        Make a POST request to the collector's rest api
        """
        return self.collector_rest_request(path, data, svcname=svcname, get_method="POST")

    def collector_rest_put(self, path, data=None, svcname=None):
        """
        Make a PUT request to the collector's rest api
        """
        return self.collector_rest_request(path, data, svcname=svcname, get_method="PUT")

    def collector_rest_delete(self, path, data=None, svcname=None):
        """
        Make a DELETE request to the collector's rest api
        """
        return self.collector_rest_request(path, data, svcname=svcname, get_method="DELETE")

    @staticmethod
    def set_ssl_context(kwargs):
        """
        Python 2.7.9+ verifies certs by default and support the creationn
        of an unverified context through ssl._create_unverified_context().
        This method add an unverified context to a kwargs dict, when
        necessary.
        """
        try:
            import ssl
            kwargs["context"] = ssl._create_unverified_context()
        except (ImportError, AttributeError):
            pass
        return kwargs

    def urlretrieve(self, url, fpath):
        """
        A chunked download method
        """
        request = Request(url)
        kwargs = {}
        kwargs = self.set_ssl_context(kwargs)
        ufile = urlopen(request, **kwargs)
        with open(fpath, 'wb') as ofile:
            for chunk in iter(lambda: ufile.read(4096), b""):
                ofile.write(chunk)
        ufile.close()

    def collector_rest_request(self, path, data=None, svcname=None, get_method="GET"):
        """
        Make a request to the collector's rest api
        """
        if data is not None and get_method == "GET":
            if len(data) == 0 or not isinstance(data, dict):
                data = None
            else:
                path += "?" + urlencode(data)
                data = None

        request = self.collector_request(path, svcname=svcname)
        if get_method:
            request.get_method = lambda: get_method
        if data is not None:
            try:
                request.add_data(urlencode(data))
            except AttributeError:
                request.data = urlencode(data).encode('utf-8')
        kwargs = {}
        kwargs = self.set_ssl_context(kwargs)
        try:
            ufile = urlopen(request, **kwargs)
        except HTTPError as exc:
            try:
                err = json.loads(exc.read())["error"]
                exc = ex.excError(err)
            except (ValueError, TypeError):
                pass
            raise exc
        except IOError as exc:
            if hasattr(exc, "reason"):
                raise ex.excError(exc.reason)
            raise ex.excError(str(exc))
        data = json.loads(ufile.read().decode("utf-8"))
        ufile.close()
        return data

    def collector_rest_get_to_file(self, path, fpath):
        """
        Download bulk chunked data from the collector's rest api
        """
        request = self.collector_request(path)
        kwargs = {}
        kwargs = self.set_ssl_context(kwargs)
        try:
            ufile = urlopen(request, **kwargs)
        except HTTPError as exc:
            try:
                err = json.loads(exc.read())["error"]
                exc = ex.excError(err)
            except ValueError:
                pass
            raise exc
        with open(fpath, 'wb') as ofile:
            for chunk in iter(lambda: ufile.read(4096), b""):
                ofile.write(chunk)
        ufile.close()

    def install_svc_conf_from_templ(self, svcname, template):
        """
        Download a provisioning template from the collector's rest api,
        and installs it as the service configuration file.
        """
        fpath = os.path.join(rcEnv.paths.pathetc, svcname+'.conf')
        try:
            int(template)
            url = "/provisioning_templates/"+str(template)+"?props=tpl_definition&meta=0"
        except ValueError:
            url = "/provisioning_templates?filters=tpl_name="+template+"&props=tpl_definition&meta=0"
        data = self.collector_rest_get(url)
        if "error" in data:
            raise ex.excError(data["error"])
        if len(data["data"]) == 0:
            raise ex.excError("service not found on the collector")
        if len(data["data"][0]["tpl_definition"]) == 0:
            raise ex.excError("service has an empty configuration")
        with open(fpath, "w") as ofile:
            ofile.write(data["data"][0]["tpl_definition"].replace("\\n", "\n").replace("\\t", "\t"))
        self.install_svc_conf_from_file(svcname, fpath)

    def install_svc_conf_from_uri(self, svcname, fpath):
        """
        Download a provisioning template from an arbitrary uri,
        and installs it as the service configuration file.
        """
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        tmpfpath = tmpf.name
        tmpf.close()
        print("get %s (%s)" % (fpath, tmpfpath))
        try:
            self.urlretrieve(fpath, tmpfpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            try:
                os.unlink(tmpfpath)
            except OSError:
                pass
        self.install_svc_conf_from_file(svcname, tmpfpath)

    @staticmethod
    def install_svc_conf_from_file(svcname, fpath):
        """
        Installs a local template as the service configuration file.
        """
        if not os.path.exists(fpath):
            raise ex.excError("%s does not exists" % fpath)

        import shutil

        # install the configuration file in etc/
        src_cf = os.path.realpath(fpath)
        dst_cf = os.path.join(rcEnv.paths.pathetc, svcname+'.conf')
        if dst_cf != src_cf:
            shutil.copy2(src_cf, dst_cf)

    def install_service(self, svcname, fpath=None, template=None):
        """
        Pick a collector's template, arbitrary uri, or local file service
        configuration file fetching method. Run it, and create the
        service symlinks and launchers directory.
        """
        if isinstance(svcname, list):
            if len(svcname) != 1:
                raise ex.excError("only one service must be specified")
            svcname = svcname[0]

        if fpath is None and template is None:
            return

        if fpath is not None and template is not None:
            raise ex.excError("--config and --template can't both be specified")

        if template is not None:
            if "://" in template:
                self.install_svc_conf_from_uri(svcname, template)
            elif os.path.exists(template):
                self.install_svc_conf_from_file(svcname, template)
            else:
                self.install_svc_conf_from_templ(svcname, template)
        else:
            if "://" in fpath:
                self.install_svc_conf_from_uri(svcname, fpath)
            else:
                self.install_svc_conf_from_file(svcname, fpath)

        self.install_service_files(svcname)

    @staticmethod
    def install_service_files(svcname):
        """
        Given a service name, install the symlink to svcmgr.
        """
        if rcEnv.sysname == 'Windows':
            return

        # install svcmgr link
        svcmgr_l = os.path.join(rcEnv.paths.pathetc, svcname)
        if not os.path.exists(svcmgr_l):
            os.symlink(rcEnv.paths.svcmgr, svcmgr_l)
        elif os.path.realpath(rcEnv.paths.svcmgr) != os.path.realpath(svcmgr_l):
            os.unlink(svcmgr_l)
            os.symlink(rcEnv.paths.svcmgr, svcmgr_l)

    def set_rlimit(self):
        """
        Set the operating system nofile rlimit to a sensible value for the
        number of services configured.
        """
        nofile = 4096
        if self.svcs and isinstance(self.svcs, list):
            proportional_nofile = 64 * len(self.svcs)
            if proportional_nofile > nofile:
                nofile = proportional_nofile

        try:
            import resource
            _vs, _vg = resource.getrlimit(resource.RLIMIT_NOFILE)
            if _vs < nofile:
                self.log.debug("raise nofile resource from %d limit to %d", _vs, nofile)
                resource.setrlimit(resource.RLIMIT_NOFILE, (nofile, _vg))
            else:
                self.log.debug("current nofile %d already over minimum %d", _vs, nofile)
        except Exception as exc:
            self.log.debug(str(exc))

    def logs(self):
        """
        logs node action entrypoint.
        Read the node.log file, colorize its content and print.
        """
        if self.options.debug:
            logfile = os.path.join(rcEnv.paths.pathlog, "node.debug.log")
        else:
            logfile = os.path.join(rcEnv.paths.pathlog, "node.log")
        if not os.path.exists(logfile):
            return
        from rcColor import color, colorize

        def highlighter(line):
            """
            Colorize interesting parts to help readability
            """
            line = line.rstrip("\n")
            elements = line.split(" - ")

            if len(elements) < 3 or elements[2] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
                # this is a log line continuation (command output for ex.)
                return line

            if len(elements[1]) > rcLogger.namelen:
                elements[1] = "*"+elements[1][-(rcLogger.namelen-1):]
            elements[1] = rcLogger.namefmt % elements[1]
            elements[1] = colorize(elements[1], color.BOLD)
            elements[2] = "%-7s" % elements[2]
            elements[2] = elements[2].replace("ERROR", colorize("ERROR", color.RED))
            elements[2] = elements[2].replace("WARNING", colorize("WARNING", color.BROWN))
            elements[2] = elements[2].replace("INFO", colorize("INFO", color.LIGHTBLUE))
            return " ".join(elements)

        try:
            pipe = os.popen('TERM=xterm less -R', 'w')
        except:
            pipe = sys.stdout
        try:
            for _logfile in [logfile+".1", logfile]:
                if not os.path.exists(_logfile):
                    continue
                with open(_logfile, "r") as ofile:
                    for line in ofile.readlines():
                        line = highlighter(line)
                        if line:
                            pipe.write(line+'\n')
        except BrokenPipeError:
            try:
                sys.stdout = os.fdopen(1)
            except (AttributeError, OSError, IOError):
                pass
        finally:
            if pipe != sys.stdout:
                pipe.close()

    @formatter
    def print_config_data(self, src_config):
        """
        Return a simple dict (OrderedDict if possible), fed with the
        service configuration sections and keys
        """
        try:
            from collections import OrderedDict
            best_dict = OrderedDict
        except ImportError:
            best_dict = dict

        config = best_dict()

        for section in src_config.sections():
            options = src_config.options(section)
            tmpsection = best_dict()
            for option in options:
                tmpsection[option] = src_config.get(section, option)
            config[section] = tmpsection
        return config

    def print_config(self):
        """
        print_config node action entrypoint
        """
        if self.options.format is not None:
            return self.print_config_data(self.config)
        from rcColor import print_color_config
        print_color_config(rcEnv.paths.nodeconf)

    def print_authconfig(self):
        """
        print_authconfig node action entrypoint
        """
        if self.options.format is not None:
            self.load_auth_config()
            return self.print_config_data(self.auth_config)
        from rcColor import print_color_config
        print_color_config(rcEnv.paths.authconf)

    def agent_version(self):
        try:
            import version
        except ImportError:
            return "dev"

        try:
            reload(version)
            return version.version
        except:
            pass

        try:
            import imp
            imp.reload(version)
            return version.version
        except:
            pass

        try:
            import importlib
            importlib.reload(version)
            return version.version
        except:
            pass

        return "dev"

    def compliance_auto(self):
        if self.sched.skip_action("compliance_auto"):
            return
        self.action("compliance_auto")

    #
    # daemon actions
    #
    def daemon_status(self):
        data = self.daemon_send(
            {"action": "daemon_status"},
            nodename=self.options.node,
        )
        if data is None:
            return

        from rcColor import format_json, colorize, color, unicons
        if self.options.format == "json":
            format_json(data)
            return

        from rcStatus import Status, colorize_status
        out = []

        def get_nodes():
            nodenames = set()
            for svc in self.svcs:
                nodenames |= svc.nodes
                nodenames |= svc.drpnodes
            return sorted(nodenames)

        self.build_services(minimal=True)
        nodenames = get_nodes()
        services = {}
        if self.options.node:
            daemon_node = self.options.node
        else:
            daemon_node = rcEnv.nodename

        def load_svc_header():
            line = [
                colorize("Services", color.GRAY),
                "",
                "",
                "",
            ]
            for nodename in nodenames:
                line.append(colorize("@" + nodename, color.GRAY))
            out.append(line)
            return True

        def load_svc(svcname, data):
            if svcname not in self.services:
                # svc deleted and monitor not yet aware
                return
            line = [
                " "+colorize(svcname, color.BOLD),
                colorize_status(data["avail"], lpad=0),
                self.services[svcname].clustertype,
                "|",
            ]
            for nodename in nodenames:
                if nodename in services[svcname]["nodes"]:
                    val = []
                    # frozen unicon
                    if services[svcname]["nodes"][nodename]["frozen"]:
                        frozen_icon = colorize(unicons.FROZEN, color.BLUE)
                    else:
                        frozen_icon = ""
                    # avail status unicon
                    avail = services[svcname]["nodes"][nodename]["avail"]
                    if avail == "unknown":
                        avail_icon = colorize("?", color.RED)
                    elif "stdby" in avail:
                        avail_icon = colorize_status(avail, lpad=0).replace(avail, unicons.STDBY)
                    else:
                        avail_icon = colorize_status(avail, lpad=0).replace(avail, unicons.STATUS)
                    # mon status
                    smon = services[svcname]["nodes"][nodename]["mon"]

                    val.append(smon)
                    val.append(avail_icon)
                    val.append(frozen_icon)
                    line.append(" ".join(val))
                elif nodename not in self.services[svcname].nodes | self.services[svcname].drpnodes:
                    line.append("")
                else:
                    line.append("unknown")
            out.append(line)

        def load_threads_header():
            line = [
                colorize("Threads", color.GRAY),
                "",
                "",
                "",
            ]
            for nodename in nodenames:
                if nodename == daemon_node:
                    line.append("")
                    continue
                line.append(colorize("@" + nodename, color.GRAY))
            out.append(line)

        def load_hb(key, _data):
            if _data["state"] == "running":
                state = colorize(_data["state"], color.GREEN)
            else:
                state = colorize(_data["state"], color.RED)
            line = [
                " "+colorize(key, color.BOLD),
                state,
                _data["config"]["addr"]+":"+str(_data["config"]["port"]),
                "|",
            ]
            for nodename in nodenames:
                if nodename == daemon_node:
                    line.append("")
                    continue
                if "*" in _data["peers"]:
                    status = _data["peers"]["*"]["beating"]
                elif nodename not in _data["peers"]:
                    status = " "
                else:
                    status = _data["peers"][nodename]["beating"]
                if status != " ":
                    if status:
                        status = colorize(unicons.STATUS, color.GREEN)
                    else:
                        status = colorize(unicons.STATUS, color.RED)
                line.append(status)
            out.append(line)

        def load_listener(key, _data):
            if _data["state"] == "running":
                state = colorize(_data["state"], color.GREEN)
            else:
                state = colorize(_data["state"], color.RED)
            out.append((
                " "+colorize(key, color.BOLD),
                state,
                _data["config"]["addr"]+":"+str(_data["config"]["port"]),
            ))

        def load_thread(key, _data):
            if _data["state"] == "running":
                state = colorize(_data["state"], color.GREEN)
            else:
                state = colorize(_data["state"], color.RED)
            out.append((
                " "+colorize(key, color.BOLD),
                state,
            ))

        if sys.version_info[0] < 3:
            pad = " "
            def bare_len(val):
                import re
                ansi_escape = re.compile(r'\x1b[^m]*m')
                val = ansi_escape.sub('', val)
                val = bytes(val).decode("utf-8")
                return len(val)
        else:
            pad = b" "
            def bare_len(val):
                import re
                ansi_escape = re.compile(b'\x1b[^m]*m')
                val = ansi_escape.sub(b'', val)
                val = bytes(val).decode("utf-8")
                return len(val)

        def list_print(data):
            if len(data) == 0:
                return
            widths = [0] * len(data[0])
            _data = []
            for line in data:
                _data.append(tuple(map(lambda x: x.encode("utf-8"), line)))
            for line in _data:
                for i, val in enumerate(line):
                    strlen = bare_len(val)
                    if strlen > widths[i]:
                        widths[i] = strlen
            for line in _data:
                _line = []
                for i, val in enumerate(line):
                    val = val + pad*(widths[i]-bare_len(val))
                    _line.append(val)
                print(pad.join(_line).decode("utf-8"))

        def print_section(data):
            if len(data) == 0:
                return
            list_print(data)

        def load_threads():
            for key in sorted(list(data.keys())):
                if key.startswith("hb#"):
                    load_hb(key, data[key])
                elif key == "listener":
                    load_listener(key, data[key])
                else:
                    load_thread(key, data[key])

        # init the services hash
        for node in data.get("monitor", {}).get("nodes", []):
            for svcname, _data in data["monitor"]["nodes"][node]["services"]["status"].items():
                if svcname not in services:
                    services[svcname] = Storage({
                        "avail": Status(),
                        "nodes": {}
                    })
                services[svcname].avail += Status(_data["avail"])
                services[svcname].nodes[node] = {
                    "avail": _data["avail"],
                    "frozen": _data["frozen"],
                    "mon": _data["monitor"]["status"],
                }

        # load data in lists
        load_threads_header()
        load_threads()
        out.append([])
        load_svc_header()
        for svcname in sorted(list(services.keys())):
            load_svc(svcname, services[svcname])

        # print tabulated lists
        print_section(out)

    def daemon_stop(self):
        options = {}
        if self.options.thr_id:
            options["thr_id"] = self.options.thr_id
        data = self.daemon_send(
            {"action": "daemon_stop", "options": options},
            nodename=self.options.node,
        )
        print(json.dumps(data, indent=4, sort_keys=True))

    def daemon_start(self):
        options = {}
        if self.options.thr_id:
            options["thr_id"] = self.options.thr_id
        else:
            os.system(sys.executable+" "+os.path.join(rcEnv.paths.pathlib, "osvcd.py"))
            return
        data = self.daemon_send(
            {"action": "daemon_start", "options": options},
            nodename=self.options.node,
        )
        print(json.dumps(data, indent=4, sort_keys=True))

