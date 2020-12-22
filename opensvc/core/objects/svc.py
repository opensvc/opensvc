"""
The module defining the Svc class.
"""
from __future__ import print_function, unicode_literals

import hashlib
import itertools
import logging
import os
import signal
import shutil
import sys
import tempfile
import time
from errno import ECONNREFUSED

import core.exceptions as ex
import core.logger
import core.status
import utilities.lock
from core.comm import Crypt, DEFAULT_DAEMON_TIMEOUT
from core.contexts import want_context
from core.extconfig import ExtConfigMixin
from core.freezer import Freezer
from core.node import Node
from core.objects.pg import PgMixin
from core.resource import Resource
from core.resourceset import ResourceSet
from core.scheduler import SchedOpts, Scheduler, sched_action
from env import Env, Paths
from utilities.converters import *
from utilities.drivers import driver_import
from utilities.fcache import fcache
from utilities.files import makedirs
from utilities.lazy import lazy, set_lazy, unset_all_lazy, unset_lazy
from utilities.naming import (fmt_path, resolve_path, svc_pathcf, svc_pathetc,
                              svc_pathlog, svc_pathtmp, svc_pathvar, new_id, factory)
from utilities.proc import (action_triggers, drop_option, find_editor,
                            init_locale, justcall, lcall, vcall)
from utilities.storage import Storage
from utilities.string import is_string

if six.PY2:
    BrokenPipeError = IOError


def signal_handler(*args):
    """
    A signal handler raising the Signal exception.
    Args can be signum and frame, but we don't use them.
    """
    raise ex.Signal


# Actions with a special handling of remote/peer relaying
ACTION_NO_ASYNC = [
    "add",
    "clear",
    "edit_config",
    "logs",
    "print_config",
    "print_status",
]

ACTION_ANY_NODE = (
    "decode",
    "delete",
    "eval",
    "gen_cert",
    "get",
    "keys",
    "validate_config",
    "set",
    "unset",
)

ACTION_ASYNC = {
    "abort": {
        "target": "aborted",
        "progress": "aborting",
    },
    "delete": {
        "target": "deleted",
        "progress": "deleting",
        "local": True,
    },
    "freeze": {
        "target": "frozen",
        "progress": "freezing",
        "local": True,
    },
    "giveback": {
        "target": "placed",
        "progress": "placing",
    },
    "move": {
        "target": "placed@",
        "progress": "placing@",
    },
    "provision": {
        "target": "provisioned",
        "progress": "provisioning",
        "local": True,
    },
    "purge": {
        "target": "purged",
        "progress": "purging",
        "local": True,
    },
    "shutdown": {
        "target": "shutdown",
        "progress": "shutting",
        "local": True,
    },
    "start": {
        "target": "started",
        "progress": "starting",
        "local": True,
    },
    "stop": {
        "target": "stopped",
        "progress": "stopping",
        "local": True,
    },
    "switch": {
        "target": "placed@",
        "progress": "placing@",
    },
    "takeover": {
        "target": "placed@",
        "progress": "placing@",
    },
    "toc": {
        "progress": "tocing",
        "local": True,
    },
    "thaw": {
        "target": "thawed",
        "progress": "thawing",
        "local": True,
    },
    "unprovision": {
        "target": "unprovisioned",
        "progress": "unprovisioning",
        "local": True,
    },
}

TOP_STATUS_GROUPS = [
    "overall",
    "avail",
    "optional",
]

DEFAULT_STATUS_GROUPS = [
    "ip",
    "volume",
    "disk",
    "fs",
    "share",
    "container",
    "app",
    "sync",
    "task",
]

CONFIG_DEFAULTS = {
    'sync#i0_schedule': '@60',
    'sync_schedule': '04:00-06:00',
    'comp_schedule': '00:00-06:00',
    'status_schedule': '@9',
    'monitor_schedule': '@1',
    'resinfo_schedule': '@60',
    'no_schedule': '',
}

ACTIONS_NO_STATUS_CHANGE = [
    "abort",
    "clear",
    "decode",
    "docker",
    "eval",
    "frozen",
    "get",
    "giveback",
    "json_config",
    "json_status",
    "json_devs",
    "json_exposed_devs",
    "json_sub_devs",
    "json_base_devs",
    "logs",
    "oci",
    "podman",
    "pg_pids",
    "print_config",
    "print_devs",
    "print_exposed_devs",
    "print_sub_devs",
    "print_base_devs",
    "print_config_mtime",
    "print_resource_status",
    "print_resinfo",
    "print_schedule",
    "print_status",
    "push_resinfo",
    "push_status",
    "push_config",
    "push_encap_config",
    "prstatus",
    "resource_monitor",
    "status",
    "validate_config",
]

#
# don't refresh the status at the end of these actions because
# we need a Svc rebuild to produce an accurate status dump.
# osvcd will refresh the status due to cf_mtime>status_mtime.
#
ACTIONS_CF_CHANGE = [
    "edit_config",
    "scale",
    "set",
    "unset",
]

ACTIONS_ALLOW_ON_INVALID_NODE = [
    "abort",
    "clear",
    "delete",
    "disable",
    "edit_config",
    "eval",
    "frozen",
    "freeze",
    "get",
    "logs",
    "print_config",
    "print_status",
    "set",
    "status",
    "thaw",
    "unset",
    "update",
    "validate_config",
]

ACTIONS_NO_LOG = [
    "delete",
    "edit_config",
    "eval",
    "get",
    "group_status",
    "install_secrets",
    "logs",
    "push_resinfo",
    "push_status",
    "push_config",
    "push_encap_config",
    "service_status",
    "resource_monitor",
    "set",
    "status",
    "unset",
    "validate_config",
]

ACTIONS_NO_TRIGGER = [
    "abort",
    "clear",
    "delete",
    "disable",
    "dns_update",
    "edit_config",
    "enable",
    "group_status",
    "install_secrets",
    "logs",
    "pg_freeze",
    "pg_thaw",
    "pg_kill",
    "postsync",
    "push_encap_config",
    "push_config",
    "push_resinfo",
    "push_status",
    "presync",
    "resource_monitor",
    "set_provisioned",
    "set_unprovisioned",
    "status",
]

ACTIONS_LOCK_COMPAT = {
    "postsync": ["sync_all", "sync_nodes", "sync_drp", "sync_update", "sync_resync"],
}

ACTIONS_NO_LOCK = [
    "abort",
    "clear",
    "docker",
    "edit_config",
    "enter",
    "freeze",
    "frozen",
    "eval",
    "get",
    "logs",
    "oci",
    "podman",
    "push_resinfo",
    "push_status",
    "push_config",
    "push_encap_config",
    "run",
    "status",
    "set_provisioned",
    "set_unprovisioned",
    "thaw",
    "validate_config",
]

START_GROUPS = [
    "ip",
    "sync.netapp",
    "sync.nexenta",
    "sync.symclone",
    "sync.symsnap",
    "sync.symsrdfs",
    "sync.hp3par",
    "sync.ibmdssnap",
    "volume",
    "disk",
    "fs",
    "share",
    "container",
    "app",
    "task",
]

STOP_GROUPS = [
    "task",
    "app",
    "container",
    "share",
    "fs",
    "sync.btrfssnap",
    "disk",
    "volume",
    "ip",
]

ACTIONS_DO_MASTER = [
    "clear",
    "freeze",
    "install_secrets",
    "set_provisioned",
    "set_unprovisioned",
    "run",
    "thaw",
    "toc",
]

ACTIONS_DO_MASTER_AND_SLAVE = [
    "boot",
    "migrate",
    "pg_update",
    "provision",
    "prstart",
    "prstop",
    "restart",
    "shutdown",
    "start",
    "startstandby",
    "stop",
    "toc",
    "unprovision"
]

ACTIONS_NEED_SNAP_TRIGGER = [
    "sync_drp",
    "sync_nodes",
    "sync_resync",
    "sync_update",
]

TOPOLOGIES = [
    "failover",
    "flex",
    "span",
]

DRV_GRP_XLATE = {
    "drbd": ["disk", "drbd"],
    "vdisk": ["disk", "vdisk"],
    "vmdg": ["disk", "ldom"],
    "pool": ["disk", "zpool"],
    "zpool": ["disk", "zpool"],
    "loop": ["disk", "loop"],
    "md": ["disk", "md"],
    "zvol": ["disk", "zvol"],
    "lv": ["disk", "lv"],
    "raw": ["disk", "raw"],
    "vxdg": ["disk", "vxdg"],
    "vxvol": ["disk", "vxvol"],
}


init_locale()


def _slave_action(func):
    def need_specifier(self):
        """
        Raise an exception if --master or --slave(s) need to be set
        """
        if self.command_is_scoped():
            return
        if self.running_action in ACTIONS_DO_MASTER_AND_SLAVE + ACTIONS_DO_MASTER:
            return
        if self.options.master or self.options.slaves or self.options.slave is not None:
            return
        raise ex.Error("specify either --master, --slave(s) or both (%s)" % func.__name__)

    def _func(self):
        if self.encap or not self.has_encap_resources:
            return
        if self.command_is_scoped() and \
           len(set(self.action_rid) & set(self.encap_resources.keys())) == 0:
            self.log.info("skip action on slaves: no encap resources are selected")
            return
        need_specifier(self)
        if self.options.slaves or \
                self.options.slave is not None or \
                (not self.options.master and
                 not self.options.slaves and
                 self.options.slave is None and
                 self.running_action in ACTIONS_DO_MASTER_AND_SLAVE):
            try:
                func(self)
            except Exception as exc:
                raise ex.Error(str(exc))
    return _func


def _master_action(func):
    def need_specifier(self):
        """
        Raise an exception if --master or --slave(s) need to be set
        """
        if self.encap:
            return
        if not self.has_encap_resources:
            return
        if self.command_is_scoped():
            return
        if self.running_action in ACTIONS_DO_MASTER_AND_SLAVE + ACTIONS_DO_MASTER:
            return
        if self.options.master or self.options.slaves or self.options.slave is not None:
            return
        raise ex.Error("specify either --master, --slave(s) or both (%s)" % func.__name__)

    def _func(self):
        need_specifier(self)
        if self.options.master or \
                (not self.options.master and
                 not self.options.slaves and
                 self.options.slave is None
                 and self.running_action in ACTIONS_DO_MASTER_AND_SLAVE + ACTIONS_DO_MASTER):
            func(self)
    return _func


class ObjPaths(object):
    def __init__(self, path, name, cf):
        self.name = name
        self.path = path
        nsetc = svc_pathetc(path)
        if cf:
            self.cf = cf
        else:
            self.cf = svc_pathcf(path)
        self.initd = os.path.join(nsetc, name+'.d')
        self.alt_initd = os.path.join(nsetc, name+'.dir')

    @property
    def tmp_cf(self):
        nstmp = svc_pathtmp(self.path)
        return os.path.join(nstmp, self.name+".conf.tmp")


class BaseSvc(Crypt, ExtConfigMixin):
    kind = "base"

    def __init__(self, name=None, namespace=None, node=None, cf=None, cd=None, volatile=False, log=None, log_handlers=None):
        self.log_handlers = log_handlers
        self.raw_cd = cd
        ExtConfigMixin.__init__(self, default_status_groups=DEFAULT_STATUS_GROUPS)
        self.name = name
        self.namespace = namespace.strip("/") if namespace else None
        self.node = node
        self.hostid = Env.nodename
        self.volatile = volatile
        self.path = fmt_path(self.name, self.namespace, self.kind)

        if log:
            self.set_lazy("log", log)

        self.paths = ObjPaths(self.path, name, cf)
        self.reset_resources()

        self.encap_json_status_cache = {}
        self.rset_status_cache = None
        self.lockfd = None
        self.abort_start_done = False
        self.action_start_date = datetime.datetime.now()
        self.action_rid = []
        self.action_rid_before_depends = []
        self.action_rid_depends = []
        self.dependencies = {}
        self.running_action = None
        self.presync_done = False
        self.stats_data = {}
        self.stats_updated = 0

        # needed for kw scoping
        self.nodes = set([Env.nodename])
        self.drpnodes = set()
        self.encapnodes = set()
        self.flex_primary = ""
        self.drp_flex_primary = ""

        # real values for kw needed by scoping
        self.init_nodes()

        # merged by the cmdline parser
        self.options = Storage(
            color="auto",
            local=False,
            slaves=False,
            slave=None,
            master=False,
            cron=False,
            follow=False,
            force=False,
            remote=False,
            debug=False,
            disable_rollback=False,
            show_disabled=None,
            moduleset="",
            module="",
            ruleset_date="",
            dry_run=False,
            refresh=False,
            rid=None,
            tags=None,
            subsets=None,
            discard=False,
            recover=False,
            waitlock=None,
            wait=False,
        )

    def reset_resources(self):
        self.init_resources_errors = 0
        self.resources_initialized = False
        self.resources_by_id = {}
        self.encap_resources = {}
        self.resourcesets_by_id = {}
        self.has_encap_resources = False

    def get_node(self):
        """
        helper for the comm module to find the Node(), for accessing
        its configuration.
        """
        if self.node is None:
            self.node = Node()
        return self.node

    def init_nodes(self):
        """
        Called from __init__, and on node labels change by entities
        holding long-lived BaseSvc objects.
        """
        if want_context():
            return
        try:
            ordered_encapnodes = self.oget("DEFAULT", "encapnodes")
            self.encapnodes = set(ordered_encapnodes)
        except (AttributeError, ValueError):
            ordered_encapnodes = []
            self.encapnodes = set()
        try:
            self.ordered_nodes = self.oget("DEFAULT", "nodes")
        except (AttributeError, ValueError):
            self.ordered_nodes = [Env.nodename]
        if self.encap and Env.nodename not in self.ordered_nodes:
            self.ordered_nodes = [Env.nodename]
        try:
            self.ordered_drpnodes = self.oget("DEFAULT", "drpnodes")
        except (AttributeError, ValueError):
            self.ordered_drpnodes = []
        try:
            self.drpnode = self.oget("DEFAULT", "drpnode")
        except (AttributeError, ValueError):
            self.drpnode = ""
        if self.drpnode and self.drpnode not in self.ordered_drpnodes:
            self.ordered_drpnodes.insert(0, self.drpnode)
        self.nodes = set(self.ordered_nodes)
        self.drpnodes = set(self.ordered_drpnodes)
        self.flex_primary = self.get_flex_primary()
        self.drp_flex_primary = self.get_drp_flex_primary()

    @lazy
    def monitor_action(self):
        return "none"

    @lazy
    def fullname(self):
        return "%s.%s.%s.%s" % (
            self.name,
            self.namespace if self.namespace else "root",
            self.kind,
            self.node.cluster_name
        )

    @lazy
    def var_d(self):
        var_d = svc_pathvar(self.path)
        if not self.volatile:
            makedirs(var_d)
        return var_d

    @lazy
    def log_d(self):
        log_d = svc_pathlog(self.path)
        if not self.volatile:
            makedirs(log_d)
        return log_d

    @lazy
    def loggerpath(self):
        return Env.nodename+"."+self.path.replace("/", ".")

    @lazy
    def log(self):  # pylint: disable=method-hidden
        extra = {
            "path": self.path,
            "node": Env.nodename,
            "sid": Env.session_uuid,
            "cron": self.options.cron,
        }
        return logging.LoggerAdapter(self.logger, extra)

    @lazy
    def logger(self):  # pylint: disable=method-hidden
        if self.volatile:
            handlers = ["stream"]
        else:
            handlers = self.log_handlers
        log_file = os.path.join(self.log_d, self.name+".log")
        return core.logger.initLogger(self.loggerpath, log_file, handlers=handlers)

    @lazy
    def compliance(self):
        from core.compliance import Compliance
        comp = Compliance(self)
        return comp

    @lazy
    def sched(self):
        """
        Lazy init of the service scheduler.
        """
        return Scheduler(
            config_defaults=CONFIG_DEFAULTS,
            options=self.options,
            svc=self,
            scheduler_actions={},
            configure_method="configure_scheduler",
        )

    @lazy
    def orchestrate(self):
        return "no"

    @lazy
    def disable_rollback(self):
        return True

    @lazy
    def show_disabled(self):
        return True

    @lazy
    def encap(self):
        return False

    @lazy
    def freezer(self):
        """
        Lazy allocator for the freezer object.
        """
        return Freezer(self.path)

    @lazy
    def id(self):
        try:
            return self.conf_get("DEFAULT", "id")
        except ex.OptNotFound as exc:
            new_id = self.new_id()
            if not self.volatile:
                self._set("DEFAULT", "id", new_id, validation=False)
            return new_id

    @staticmethod
    def new_id():
        return new_id()

    @lazy
    def peers(self):
        if Env.nodename in self.nodes:
            return self.nodes
        elif Env.nodename in self.drpnodes:
            return self.drpnodes
        else:
            return []

    @lazy
    def ordered_peers(self):
        if Env.nodename in self.nodes:
            return self.ordered_nodes
        elif Env.nodename in self.drpnodes:
            return self.ordered_drpnodes
        else:
            return []

    @lazy
    def placement(self):
        return "nodes order"

    @lazy
    def topology(self):
        return "span"

    @lazy
    def svc_env(self):
        val = self.oget("DEFAULT", "env")
        if val is None:
            return self.node.env
        return val

    @lazy
    def lock_timeout(self):
        return self.oget("DEFAULT", "lock_timeout")

    @lazy
    def priority(self):
        return self.oget("DEFAULT", "priority")

    @lazy
    def cd(self):
        if self.raw_cd is not None:
            return self.raw_cd
        return self.parse_config_file(self.paths.cf)

    @lazy
    def disabled(self):
        return self.oget("DEFAULT", "disable")

    def svclock(self, action=None, timeout=30, delay=1):
        """
        Acquire the service action lock.
        """
        if want_context():
            return
        suffix = None

        if action == "toc" and self.monitor_action in ("reboot", "crash"):
            return

        if (action not in ACTION_NO_ASYNC and self.options.node is not None and self.options.node != "") or \
           action in ACTIONS_NO_LOCK or \
           self.options.nolock or \
           action.startswith("collector") or \
           self.lockfd is not None:
            # explicitly blacklisted or
            # no need to serialize requests or
            # already acquired
            return

        if action.startswith("compliance"):
            # compliance modules are allowed to execute actions on the service
            # so give them their own lock
            suffix = "compliance"
        elif action.startswith("sync"):
            suffix = "sync"

        lockfile = os.path.join(self.var_d, "lock.generic")
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))

        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire service lock %s", details)

        # try an immmediate lock acquire and see if the running action is
        # compatible
        if action in ACTIONS_LOCK_COMPAT:
            try:
                lockfd = utilities.lock.lock(
                    timeout=0,
                    delay=delay,
                    lockfile=lockfile,
                    intent=action
                )
                if lockfd is not None:
                    self.lockfd = lockfd
                return
            except utilities.lock.LockTimeout as exc:
                if exc.intent in ACTIONS_LOCK_COMPAT[action]:
                    return
                # not compatible, continue with the normal acquire
            except Exception:
                pass

        try:
            lockfd = utilities.lock.lock(
                timeout=timeout,
                delay=delay,
                lockfile=lockfile,
                intent=action
            )
        except utilities.lock.LockTimeout as exc:
            raise ex.Error("timed out waiting for lock %s: %s" % (details, str(exc)))
        except utilities.lock.LockNoLockFile:
            raise ex.Error("lock_nowait: set the 'lockfile' param %s" % details)
        except utilities.lock.LockCreateError:
            raise ex.Error("can not create lock file %s" % details)
        except utilities.lock.LockAcquire as exc:
            raise ex.Error("another action is currently running %s: %s" % (details, str(exc)))
        except ex.Signal:
            raise ex.Error("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.Error("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def svcunlock(self):
        """
        Release the service action lock.
        """
        utilities.lock.unlock(self.lockfd)
        self.lockfd = None

    @staticmethod
    def setup_signal_handlers():
        """
        Install signal handlers.
        """
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except ValueError:
            # signal only works in main thread
            pass

    def systemd_join_agent_service(self):
        from utilities.systemd import systemd_system, systemd_join
        if os.environ.get("OSVC_ACTION_ORIGIN") == "daemon" or not systemd_system():
            return
        systemd_join("opensvc-agent.service")

    def action(self, action, options=None):
        self.systemd_join_agent_service()
        try:
            options = self.prepare_options(action, options)
            ret = self.async_action(action)
            if ret is not None:
                return ret
        except ex.Error as exc:
            msg = str(exc)
            if msg:
                self.log.error(msg)
            return 1
        except ex.AbortAction as exc:
            msg = str(exc)
            if msg:
                self.log.info(msg)
            return 0
        except ex.AlreadyDone as exc:
            # so do_svcs_action() can decide not to wait for the
            # service to reach the global_expect
            return -1
        self.allow_on_this_node(action)
        try:
            return self._action(action, options=options)
        except utilities.lock.LOCK_EXCEPTIONS as exc:
            raise ex.Error(str(exc))

    def barrier_sanity_check(self, barrier):
        """
        Raise if the barrier (--upto <barrier> or --downto <barrier>) does not
        match any resource, to avoid a full start when the user makes a typo
        in the barrier selector.
        """
        if barrier is None:
            return
        if self.get_resource(barrier):
            return
        if self.get_resources(barrier):
            return
        raise ex.Error("barrier '%s' does not match any resource" % barrier)

    @sched_action
    def _action(self, action, options=None):
        """
        Filter resources on which the service action must act.
        Abort if the service is frozen, or if --cluster is not set on a HA
        service.
        Set up the environment variables.
        Finally do the service action either in logged or unlogged mode.
        """
        self.barrier_sanity_check(self.options.upto)
        self.barrier_sanity_check(self.options.downto)

        try:
            self.action_rid_before_depends = self.options_to_rids(options, action)
        except ex.AbortAction as exc:
            self.log.error(exc)
            return 1

        depends = set()
        for rid in self.action_rid_before_depends:
            depends |= self.action_rid_dependencies(action, rid) - set(self.action_rid_before_depends)

        self.action_rid = set(self.action_rid_before_depends)
        if len(depends) > 0:
            self.log.info("add rid %s to satisfy dependencies" % ", ".join(depends))
            self.action_rid |= depends

        self.action_rid = list(self.action_rid)
        self.action_rid_depends = list(depends)
        self.action_start_date = datetime.datetime.now()

        if self.node is None:
            self.node = Node()

        if action not in ACTIONS_NO_STATUS_CHANGE and \
                'compliance' not in action and \
                'collector' not in action and \
                not options.dry_run and \
                not action.startswith("oci") and \
                not action.startswith("docker") and \
                not action.startswith("podman"):
            #
            # here we know we will run a resource state-changing action
            # purge the resource status file cache, so that we don't take
            # decision on outdated information
            #
            self.log.debug("purge all resource status file caches before "
                           "action %s", action)
            self.purge_status_last()

        self.setup_signal_handlers()
        self.set_skip_resources(keeprid=self.action_rid, xtags=options.xtags)
        if action in ("status", "decode", "pg_pids", "pg_stats") or \
           action.startswith("print_") or \
           action.startswith("collector") or \
           action.startswith("json_"):
            return self.do_print_action(action, options)

        if self.published_action(action, options):
            if self.node.oget("node", "dblog"):
                err = self.do_logged_action(action, options)
            else:
                self.log_action_header(action, options)
                err = self.do_action(action, options)
        else:
            err = self.do_action(action, options)

        return err

    def purge_status_caches(self):
        """
        Purge the json cache and each resource status on-disk cache.
        """
        self.purge_status_last()
        self.purge_status_data_dump()

    def purge_status_data_dump(self):
        """
        Purge the json status dump
        """
        try:
            os.unlink(self.status_data_dump)
        except Exception:
            pass

    def purge_status_last(self):
        """
        Purge all service resources on-disk status caches.
        """
        import glob
        for fpath in glob.glob(os.path.join(self.var_d, "*#*", "status.last")):
            try:
                os.unlink(fpath)
            except:
                pass

    def published_action(self, action, options):
        if self.volatile:
            return False
        if not os.path.exists(self.paths.cf):
            return False
        if action in ACTIONS_NO_LOG or \
           action.startswith("compliance") or \
           action.startswith("oci") or \
           action.startswith("docker") or \
           action.startswith("podman") or \
           options.dry_run:
            return False
        return True

    def do_print_action(self, action, options):
        """
        Call the service method associated with action. This method produces
        data the caller will print.
        If --cluster is set, execute the action on remote nodes and
        aggregate the results.
        """
        _action = action + ""
        if action.startswith("json_"):
            action = "print_"+action[5:]
            self.node.options.format = "json"
            self.options.format = "json"
            options.format = "json"

        if "_json_" in action:
            action = action.replace("_json_", "_")
            self.node.options.format = "json"
            self.options.format = "json"
            options.format = "json"

        try:
            if action.startswith("collector_"):
                from core.collector.actions import CollectorActions
                collector = CollectorActions(options, self.node, self.path)
                func = getattr(collector, action)
            else:
                func = getattr(self, action)
        except AttributeError:
            raise ex.Error("%s is not implemented" % action)

        if not hasattr(func, "__call__"):
            raise ex.Error("%s is not callable" % action)

        try:
            data = func()
        except Exception as exc:
            data = {"error": str(exc)}

        return data

    def do_action(self, action, options):
        """
        Acquire the service action lock, call the service action method,
        handles its errors, and finally release the lock.

        If --cluster is set, and the service is a flex, and we are
        flex_primary run the action on all remote nodes.
        """

        if action not in ACTIONS_NO_LOCK and self.topology not in TOPOLOGIES:
            raise ex.Error("invalid cluster type '%s'. allowed: %s" % (
                self.topology,
                ', '.join(TOPOLOGIES),
            ))

        err = 0
        waitlock = convert_duration(options.waitlock)
        if waitlock is None or waitlock < 0:
            waitlock = self.lock_timeout

        if action == "sync_all" and self.command_is_scoped():
            for rid in self.action_rid:
                resource = self.get_resource(rid)  # pylint: disable=assignment-from-none
                if not resource or not resource.type.startswith("sync"):
                    continue
                try:
                    resource.reslock(action=action, suffix="sync")
                except ex.Error as exc:
                    self.log.error(str(exc))
                    return 1
        else:
            try:
                self.svclock(action, timeout=waitlock)
            except ex.Error as exc:
                self.log.error(str(exc))
                return 1

        def call_action(action):
            self.setup_environ(action=action, options=options)
            self.action_triggers("pre", action)
            self.action_triggers("blocking_pre", action, blocking=True)
            err = getattr(self, action)()
            self.action_triggers("post", action)
            self.action_triggers("blocking_post", action, blocking=True)
            return err

        try:
            if action.startswith("compliance_"):
                err = getattr(self.compliance, action)()
            elif hasattr(self, action):
                self.running_action = action
                self.notify_action(action, force=options.notify)
                err = call_action(action)
                if err is None:
                    err = 0
            else:
                self.log.info("action '%s' is not applicable to '%s' kind objects", action, self.kind)
                err = 0
        except ex.EndAction as exc:
            self.log.info(exc)
            err = 0
        except ex.AbortAction as exc:
            msg = "'%s' action aborted by last resource" % action
            if len(str(exc)) > 0:
                msg += ": %s" % str(exc)
            self.log.info(msg)
            err = 0
        except ex.Error as exc:
            msg = "'%s' action stopped on execution error" % action
            self.log.debug(msg)
            msg = str(exc)
            if len(msg) > 0:
                self.log.error(msg)
            err = 1
            self.rollback_handler(action)
        except ex.Signal:
            self.log.error("interrupted by signal")
            err = 1
        except:
            err = 1
            self.save_exc()
        finally:
            if action in ACTIONS_CF_CHANGE:
                self.unset_conf_lazy()
                self.reset_resources()
                self.init_resources()
            if not want_context() and \
               action not in ACTIONS_NO_STATUS_CHANGE and \
               not (action == "delete" and not self.command_is_scoped()):
                data = self.print_status_data(refresh=True)
                if action == "start" and not self.command_is_scoped() and \
                   err == 0 and data.get("avail") not in ("up", "stdby up", "n/a", None) and \
                   not self.options.dry_run:
                    # catch drivers reporting no error, but instance not
                    # evaluating as "up", to avoid the daemon entering a
                    # start loop. This also catches resources going down
                    # a short time a startup (app.simple for example)
                    self.log.error("start action returned 0 but instance "
                                   "avail status is %s", data.get("avail"))
                    err = 1
            if action != "toc" or self.monitor_action in ("freezestop", "switch"):
                self.clear_action(action, err, force=options.notify)
            if action not in ("sync_all", "run"):
                # sync_all and run handle notfications at the resource level
                self.notify_done(action)
            self.svcunlock()
            if action == "sync_all" and self.command_is_scoped():
                for rid in self.action_rid:
                    resource = self.resources_by_id[rid]
                    if not resource.type.startswith("sync"):
                        continue
                    resource.resunlock()
            self.running_action = None

        return err

    def action_progress(self, action):
        progress = ACTION_ASYNC.get(action, {}).get("progress")
        if progress is None:
            return
        if action.startswith("sync"):
            progress = "syncing"
        return progress

    def action_need_freeze_instance(self, action):
        if self.orchestrate not in ("ha", "start"):
            return False
        if self.command_is_scoped():
            return False
        if action not in ("stop", "shutdown", "unprovision", "delete", "rollback"):
            return False
        return True

    def action_need_unset_local_expect(self, action):
        if self.command_is_scoped():
            return False
        if action not in ("stop", "shutdown", "unprovision", "delete", "rollback", "toc"):
            return False
        return True

    def notify_action(self, action, force=False):
        if not force and os.environ.get("OSVC_ACTION_ORIGIN") == "daemon":
            return
        if self.options.dry_run:
            return
        progress = self.action_progress(action)
        if progress is None:
            return
        local_expect = None
        if self.action_need_unset_local_expect(action):
            local_expect = "unset"
        if self.action_need_freeze_instance(action):
            self.freezer.freeze()
        try:
            self.set_service_monitor(local_expect=local_expect, status=progress, best_effort=True)
            self.log.debug("daemon notified of action '%s' begin" % action)
        except Exception as exc:
            pass

    def clear_action(self, action, err, force=False):
        if not force and os.environ.get("OSVC_ACTION_ORIGIN") == "daemon":
            return
        progress = self.action_progress(action)
        local_expect = None
        if progress is None:
            return
        if progress == "tocing" and self.monitor_action == "switch":
            return
        if err:
            status = action + " failed"
        else:
            status = "idle"
            if action == "start" and not self.command_is_scoped():
                local_expect = "started"
        try:
            self.set_service_monitor(local_expect=local_expect, status=status)
            self.log.debug("daemon notified of action '%s' end" % action)
        except Exception as exc:
            pass

    def rollback_handler(self, action):
        """
        Call the rollback method if
        * the action triggering this handler is a start*
        * service is not configured to not disable rollback
        * --disable-rollback is not set
        * at least one resource has been flagged rollbackable during the
          start* action
        """
        if 'start' not in action:
            return
        if self.options.disable_rollback:
            self.log.info("skip rollback %s: as instructed by --disable-rollback", action)
            return
        if self.disable_rollback:
            self.log.info("skip rollback %s: as instructed by DEFAULT.rollback=false", action)
            return
        rids = [r.rid for r in self.get_resources()
                if r.can_rollback and (r.rollback_even_if_standby or not r.is_standby)]
        if len(rids) == 0:
            self.log.info("skip rollback %s: no resource activated", action)
            return
        self.log.info("trying to rollback %s on %s", action, ', '.join(rids))
        try:
            self.rollback()
        except ex.Error:
            self.log.error("rollback %s failed", action)

    def dblogger(self, action, begin, end, actionlogfile):
        """
        Send to the collector the service status after an action, and
        the action log.
        """
        self.node.daemon_collector_xmlrpc('end_action', self.path, action,
                                          begin, end, self.options.cron,
                                          actionlogfile)
        try:
            logging.shutdown()
        except:
            pass

    def do_logged_action(self, action, options):
        """
        Setup action logging to a machine-readable temp logfile, in preparation
        to the collector feeding.
        Do the action.
        Finally, feed the log to the collector.
        """
        begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Provision a database entry to store action log later
        self.node.daemon_collector_xmlrpc("begin_action", self.path,
                                          action, self.node.agent_version,
                                          begin, self.options.cron)

        # Per action logfile to push to database at the end of the action
        tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=Env.paths.pathtmp,
                                              prefix=self.name+'.'+action)
        actionlogfile = tmpfile.name
        tmpfile.close()
        fmt = "%(asctime)s;;%(name)s;;%(levelname)s;;%(message)s;;%(process)d;;EOL"
        actionlogformatter = logging.Formatter(fmt)
        actionlogfilehandler = logging.FileHandler(actionlogfile)
        actionlogfilehandler.setFormatter(actionlogformatter)
        actionlogfilehandler.setLevel(logging.INFO)
        self.logger.addHandler(actionlogfilehandler)

        self.log_action_header(action, options)
        err = self.do_action(action, options)

        # Push result and logs to database
        actionlogfilehandler.close()
        self.logger.removeHandler(actionlogfilehandler)
        end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.dblogger(action, begin, end, actionlogfile)
        return err

    def log_action_obfuscate_secret(self, options):
        data = {}
        data.update(options)
        for k in ("svcs", "parm_svcs", "namespace"):
            try:
                del data[k]
            except KeyError:
                pass
        if self.kind not in ("usr", "sec"):
            return data
        for k, v in data.items():
            if k == "value":
                data["value"] = "xxx"
        return data

    def log_action_header(self, action, options):
        from utilities.render.command import format_command
        origin = os.environ.get("OSVC_ACTION_ORIGIN", "user")
        data = self.log_action_obfuscate_secret(options)
        cmd = format_command(self.kind, action, data)
        buff = "do %s (%s origin)" % (" ".join(cmd), origin)
        buff = buff.replace("%", "%%")
        self.log.info(buff, {"f_stream": False})

    def prepare_options(self, action, options):
        """
        Return a Storage() from command line options or dict passed as
        <options>, sanitized, merge with default values in self.options.
        """
        if options is None:
            options = Storage()
        elif isinstance(options, dict):
            options = Storage(options)

        if is_string(options.slave):
            options.slave = options.slave.split(',')

        if isinstance(options.resource, list):
            for idx, resource in enumerate(options.resource):
                if not is_string(resource):
                    continue
                try:
                    options.resource[idx] = json.loads(resource)
                except ValueError:
                    raise ex.Error("invalid json in resource definition: "
                                   "%s" % options.resource[idx])

        self.options.update(options)
        options = self.options

        return options

    def print_config_mtime(self):
        """
        Print the service configuration file last modified timestamp. Used by
        remote agents to determine which agent holds the most recent version.
        """
        mtime = os.stat(self.paths.cf).st_mtime
        print(mtime)

    def prepare_async_cmd(self):
        """
        For encap commands
        """
        if "__main__" in sys.argv[0]:
            # skip selector or subsystem name too
            cmd = sys.argv[2:]
        else:
            cmd = sys.argv[1:]
        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("-s", cmd, drop_value=True)
        cmd = drop_option("--service", cmd, drop_value=True)
        return cmd

    def prepare_async_options(self):
        """
        For jsonrpc commands
        """
        options = {}
        options.update(self.options)
        for opt in ("svcs", "node", "local"):
            if opt in options:
                del options[opt]
        return options

    def is_remote_action(self, action):
        if want_context() and (self.options.node or self.command_is_scoped() or action not in ACTION_ASYNC):
            return True
        if self.options.node is not None and self.options.node != "":
            return True
        if action in ACTION_ANY_NODE and not self.exists():
            return True
        return False

    def async_action(self, action, wait=None, timeout=None):
        if action in ACTION_NO_ASYNC:
            return
        if self.is_remote_action(action):
            options = self.prepare_async_options()
            ret = self.daemon_service_action(action=action, options=options, node=self.options.node, action_mode=False)
            if isinstance(ret, (dict, list)):
                return ret
            if ret == 0:
                raise ex.AbortAction()
            else:
                raise ex.Error()
        if self.options.local or self.options.slave or self.options.slaves or \
           self.options.master:
            return
        if action not in ACTION_ASYNC:
            return
        if "target" not in ACTION_ASYNC[action]:
            return
        if self.command_is_scoped():
            return
        if self.options.dry_run:
            raise ex.AbortAction()
        self.daemon_mon_action(action, wait=wait, timeout=timeout)
        raise ex.AbortAction()

    def daemon_log_result(self, ret, raise_on_errors=True):
        if ret is None:
            return
        info = ret.get("info", [])
        if info is None:
            info = []
        elif not isinstance(info, list):
            info = [info]
        for line in info:
            if not line:
                continue
            self.log.info(line)

        errors = ret.get("error", [])
        if errors is None:
            errors = []
        if not isinstance(errors, list):
            errors = [errors]
        for line in errors:
            if not line:
                continue
            self.log.error(line)
        if errors:
            raise ex.Error
        status = ret.get("status")
        if status not in (None, 0):
            raise ex.Error

    def daemon_mon_action(self, action, wait=None, timeout=None):
        global_expect = self.prepare_global_expect(action)
        if global_expect is None:
            # not applicable action on this service
            return
        begin = time.time()
        data = self.set_service_monitor(global_expect=global_expect)
        if data:
            for line in data.get("error", []):
                self.log.error(line)
            for line in data.get("info", []):
                self.log.info(line)
                if " already " in line:
                    raise ex.AlreadyDone
            if data.get("error", []):
                raise ex.Error
        try:
            # the daemon may have changed and return global expect
            # (placed@<peer>)
            global_expect = data["data"]["global_expect"]
            # save for Node::do_svcs_action()
            self.last_global_expect = global_expect
        except KeyError:
            pass
        self.wait_daemon_mon_action(global_expect, wait=wait, timeout=timeout, begin=begin)

    def prepare_global_expect(self, action):
        global_expect = ACTION_ASYNC[action]["target"]
        if action == "delete" and self.options.unprovision:
            global_expect = "purged"
            action = "purge"
        elif action == "move":
            if self.options.to is None:
                raise ex.Error("the --to <node>[,<node>,...] option is required")
            global_expect += self.options.to
        elif action == "switch":
            dst = self.destination_node_sanity_checks()  # pylint: disable=assignment-from-none
            if dst is None:
                return
            global_expect += dst
        elif action == "takeover":
            dst = self.destination_node_sanity_checks(Env.nodename)  # pylint: disable=assignment-from-none
            if dst is None:
                return
            global_expect += dst
        return global_expect

    def wait_daemon_mon_action(self, global_expect, wait=None, timeout=None, log_progress=True, begin=None):
        if wait is None:
            wait = self.options.wait
        if not wait:
            return
        if timeout is None:
            timeout = self.options.time
        try:
            if global_expect == "frozen":
                self.node._wait(path="monitor.services.'%s'.frozen=frozen" % self.path, duration=timeout)
            elif global_expect == "thawed":
                self.node._wait(path="monitor.services.'%s'.frozen=thawed" % self.path, duration=timeout)
            elif global_expect == "purged":
                self.node._wait(path="!monitor.services.'%s'" % self.path, duration=timeout)
            elif global_expect == "deleted":
                self.node._wait(path="!monitor.services.'%s'" % self.path, duration=timeout)
            elif global_expect == "aborted":
                self.node._wait(path="!monitor.services.'%s'.global_expect" % self.path, duration=timeout)
            elif global_expect == "provisioned":
                self.node._wait(path="monitor.services.'%s'.provisioned=true" % self.path, duration=timeout)
            elif global_expect == "unprovisioned":
                self.node._wait(path="monitor.services.'%s'.provisioned=false" % self.path, duration=timeout)
            elif global_expect == "shutdown":
                self.node._wait(path="monitor.services.'%s'.avail~(down|n/a)" % self.path, duration=timeout)
            elif global_expect == "stopped":
                self.node._wait(path="monitor.services.'%s'.avail~(down|stdby up|n/a)" % self.path, duration=timeout)
            elif global_expect == "started":
                self.node._wait(path="monitor.services.'%s'.avail~(up|n/a)" % self.path, duration=timeout)
            elif global_expect == "placed":
                self.node._wait(path="monitor.services.'%s'.avail~(up|n/a)" % self.path, duration=timeout)
                self.node._wait(path="monitor.services.'%s'.placement=optimal" % self.path, duration=timeout)
            elif global_expect.startswith("placed@"):
                node = global_expect[7:]
                self.node._wait(path="monitor.nodes.'%s'.services.status.'%s'.avail~(up|n/a)" %
                                     (node, self.path), duration=timeout)
        except KeyboardInterrupt:
            raise ex.Error

    def current_node(self):
        data = self.node._daemon_status()
        if not data:
            raise ex.Error("can not migrate when daemon is down")
        for nodename, _data in data["monitor"]["nodes"].items():
            try:
                __data = _data["services"]["status"][self.path]
            except KeyError:
                continue
            if __data["avail"] == "up":
                return nodename

    def command_is_scoped(self, options=None):
        """
        Return True if a resource filter has been setup through
        --rid, --subsets or --tags
        """
        if options is None:
            options = self.options
        if options.rid or options.tags or options.subsets or options.upto or options.downto:
            return True
        return False

    def save_exc(self):
        """
        A helper method to save stacks in the service log.
        """
        self.log.error("a stack has been saved in the logs", exc_info=True)

    def vcall(self, *args, **kwargs):
        """
        Wrap vcall, setting the service logger
        """
        kwargs["log"] = self.log
        return vcall(*args, **kwargs)

    def lcall(self, *args, **kwargs):
        """
        Wrap lcall, setting the service logger
        """
        kwargs["logger"] = self.log
        return lcall(*args, **kwargs)

    def allocate_rid(self, group, sections):
        """
        Return an unused rid in <group>.
        """
        prefix = group + "#"
        rids = [section for section in sections if section.startswith(prefix)]
        idx = 1
        while True:
            rid = "#".join((group, str(idx)))
            if rid in rids:
                idx += 1
                continue
            return rid

    def update(self):
        result = self._update(self.options.resource or [],
                              interactive=self.options.interactive,
                              provision=self.options.provision)
        buff = " "
        if result["created"]:
            buff += "created: %s" % ",".join(result["created"])
        if result["updated"]:
            if buff:
                buff += " "
            buff += "updated: %s" % ",".join(result["updated"])
        if buff:
            self.log.info("%s", buff)

    def _update(self, resources, interactive=False, provision=False):
        """
        The 'update' action entry point.
        Add resources to the service configuration, and provision them if
        instructed to do so.
        """

        result = {
            "created": [],
            "updated": [],
        }
        rtypes = {}
        for section in self.cd:
            elements = section.split('#')
            if len(elements) == 2:
                rtype = elements[0]
                ridx = elements[1]
                if rtype not in rtypes:
                    rtypes[rtype] = set()
                rtypes[rtype].add(ridx)

        import core.objects.builder

        rid = []

        for data in resources:
            is_resource = False
            if 'rid' in data:
                section = data['rid']
                if '#' not in section:
                    raise ex.Error("%s must be formatted as 'rtype#n'" % section)
                elements = section.split('#')
                if len(elements) != 2:
                    raise ex.Error("%s must be formatted as 'rtype#n'" % section)
                del data['rid']
                if section in self.cd:
                    self.cd[section].update(data)
                    result["updated"].append(section)
                else:
                    self.cd[section] = data
                    result["created"].append(section)
                is_resource = True
            elif 'rtype' in data and data["rtype"] == "env":
                del data["rtype"]
                if "env" in self.cd:
                    self.cd["env"].update(data)
                    result["updated"].append("env")
                else:
                    self.cd["env"] = data
                    result["created"].append("env")
            elif 'rtype' in data and data["rtype"] != "DEFAULT":
                section = self.allocate_rid(data['rtype'], self.cd)
                del data['rtype']
                self.cd[section] = data
                result["created"].append(section)
                is_resource = True
            else:
                if "rtype" in data:
                    del data["rtype"]
                if "DEFAULT" in self.cd:
                    self.cd["DEFAULT"].update(data)
                    result["updated"].append("DEFAULT")
                else:
                    self.cd["DEFAULT"] = data
                    result["created"].append("DEFAULT")

            if is_resource:
                rid.append(section)

        self.commit()

        for section in rid:
            group = section.split("#")[0]
            core.objects.builder.add_resource(self, group, section)

        if provision and len(rid) > 0:
            options = Storage(self.options)
            options.rid = rid
            self.action("provision", options)

        return result

    def allow_on_this_node(self, action):
        """
        Raise Error if the service is not allowed to run on this node.
        In other words, the nodename is not a service node or drpnode, nor the
        service mode is cloud proxy.
        """
        if want_context():
            return
        if action in ACTIONS_ALLOW_ON_INVALID_NODE:
            return
        if self.svc_env != 'PRD' and self.node.env == 'PRD':
            raise ex.Error('not allowed to run on this node (svc env=%s node env=%s)' % (self.svc_env, self.node.env))
        if Env.nodename in self.nodes:
            return
        if Env.nodename in self.drpnodes:
            return
        raise ex.Error("action '%s' aborted because this node's hostname "
                       "'%s' is not a member of DEFAULT.nodes, "
                       "DEFAULT.drpnode nor DEFAULT.drpnodes" %
                       (action, Env.nodename))

    def setup_environ(self, action=None, options=None):
        """
        Setup envionment variables.
        Startup scripts and triggers can use them, so their code can be
        more generic.
        All resources can contribute a set of env variables through their
        own setup_environ() method.
        """
        if action in ACTIONS_NO_TRIGGER:
            return
        if not action and os.environ.get("OPENSVC_SVCPATH") == self.path:
            return
        os.environ['OPENSVC_SVCPATH'] = self.path
        os.environ['OPENSVC_SVCNAME'] = self.name
        os.environ['OPENSVC_SVC_ID'] = self.id
        if self.namespace:
            os.environ['OPENSVC_NAMESPACE'] = self.namespace
        if action:
            os.environ['OPENSVC_ACTION'] = action
        if options and options.leader:
            os.environ['OPENSVC_LEADER'] = "1"
        else:
            os.environ['OPENSVC_LEADER'] = "0"
        for resource in self.get_resources():
            resource.setup_environ()

    def print_config(self):
        """
        The 'print config' action entry point.
        Print the service configuration in the format specified by --format.
        """
        if want_context() or (not self.cd and not os.path.exists(self.paths.cf)):
            node, buff = self.remote_service_config(self.options.node)
            if buff is None:
                raise ex.Error("could not fetch remote config")
            try:
                tmpfile = tempfile.NamedTemporaryFile()
                fname = tmpfile.name
                tmpfile.close()
                with open(fname, "w") as tmpfile:
                    tmpfile.write(buff)
                svc = Svc(self.name, self.namespace, node=self.node, cf=fname, volatile=True)
                svc.options = self.options
                return svc._print_config()
            finally:
                try:
                    os.unlink(fname)
                except Exception:
                    pass
        return self._print_config()

    def _print_config(self):
        if self.options.format is not None or self.options.jsonpath_filter:
            return self.print_config_data(evaluate=self.options.eval,
                                          impersonate=self.options.impersonate)
        from utilities.render.color import print_color_config
        print_color_config(self.paths.cf)

    def make_temp_config(self):
        """
        Copy the current service configuration file to a temporary
        location for edition.
        If the temp file already exists, propose the --discard
        or --recover options.
        """
        makedirs(os.path.dirname(self.paths.tmp_cf))
        if os.path.exists(self.paths.tmp_cf):
            if self.options.recover:
                pass
            elif self.options.discard:
                shutil.copy(self.paths.cf, self.paths.tmp_cf)
            else:
                self.edit_config_diff()
                print("%s exists: service is already being edited. Set "
                      "--discard to edit from the current configuration, "
                      "or --recover to open the unapplied config" %
                      self.paths.tmp_cf, file=sys.stderr)
                raise ex.Error
        else:
            shutil.copy(self.paths.cf, self.paths.tmp_cf)
        return self.paths.tmp_cf

    def edit_config_diff(self):
        """
        Display the diff between the current config and the pending
        unvalidated config.
        """
        from subprocess import call

        def diff_capable(opts):
            cmd = ["diff"] + opts + [self.paths.cf, self.paths.cf]
            cmd_results = justcall(cmd)
            if cmd_results[2] == 0:
                return True
            return False

        if not os.path.exists(self.paths.tmp_cf):
            return
        if diff_capable(["-u", "--color"]):
            cmd = ["diff", "-u", "--color", self.paths.cf, self.paths.tmp_cf]
        elif diff_capable(["-u"]):
            cmd = ["diff", "-u", self.paths.cf, self.paths.tmp_cf]
        else:
            cmd = ["diff", self.paths.cf, self.paths.tmp_cf]
        call(cmd)

    def edit_config(self):
        """
        Execute an editor on the service configuration file.
        When the editor exits, validate the new configuration file.
        If validation pass, install the new configuration,
        else keep the previous configuration in place and offer the
        user the --recover or --discard choices for its next edit
        config action.
        """
        try:
            editor = find_editor()
        except ex.Error as error:
            print(error, file=sys.stderr)
            return 1
        from utilities.files import fsum
        if want_context() or not os.path.exists(self.paths.cf):
            node, refcf = self.remote_service_config_fetch()
            need_send = True
            tmpcf = refcf + ".tmp"
            shutil.copy2(refcf, tmpcf)
        else:
            refcf = self.paths.cf
            need_send = False
            tmpcf = self.make_temp_config()
            node = None
        os.system(' '.join((editor, tmpcf)))
        if fsum(tmpcf) == fsum(refcf):
            os.unlink(tmpcf)
            if refcf != self.paths.cf:
                os.unlink(refcf)
            return 0
        if need_send:
            try:
                return self.node.install_service(self.path, fpath=tmpcf,
                                                 restore=True, node=node)
            finally:
                os.unlink(refcf)
                os.unlink(tmpcf)
        else:
            results = self._validate_config(path=tmpcf)
            if results["errors"] == 0:
                shutil.copy(tmpcf, self.paths.cf)
                os.unlink(tmpcf)
            else:
                print("your changes were not applied because of the errors "
                      "reported above. you can use the edit config command "
                      "with --recover to try to fix your changes or with "
                      "--discard to restart from the live config")
            return results["errors"] + results["warnings"]

    #########################################################################
    #
    # daemon communications
    #
    #########################################################################
    def daemon_backlogs(self, server=None, node=None, backlog=None, debug=False):
        req = {
            "action": "object_backlogs",
            "options": {
                "path": self.path,
                "backlog": backlog,
                "debug": debug,
            }
        }
        result = self.daemon_get(req, server=server, node=node)
        if "nodes" in result:
            lines = []
            for logs in result["nodes"].values():
                if not isinstance(logs, list):
                    # happens when no log is present on a peer or when peer
                    # is down
                    continue
                lines += logs
        else:
            lines = result
        try:
            return sorted(lines, key=lambda x: x.get("t", 0))
        except AttributeError:
            return []

    def daemon_logs(self, server=None, node=None, backlog=None, debug=None):
        req = {
            "action": "object_logs",
            "options": {
                "path": self.path,
                "debug": debug,
            }
        }
        for lines in self.daemon_stream(req, server=server, node=node):
            if lines is None:
                break
            for line in lines:
                yield line

    def abort(self):
        pass

    def clear(self):
        self.master_clear()
        self.slave_clear()

    @_slave_action
    def slave_clear(self):
        self.encap_cmd(["clear"], verbose=True)

    @_master_action
    def master_clear(self):
        self._clear(node=self.options.node)

    def _clear(self, server=None, node=None):
        if not node and not self.options.local:
            node = self.ordered_nodes
        req = {
            "action": "object_clear",
            "options": {
                "path": self.path,
            }
        }
        data = self.daemon_post(req, timeout=DEFAULT_DAEMON_TIMEOUT, server=server, node=node)
        status, error, info = self.parse_result(data)
        if info:
            print(info)
        if status:
            raise ex.Error(error)

    def notify_done(self, action, rids=None):
        if not self.options.cron:
            return
        if rids is None:
            rids = self.action_rid
        req = {
            "action": "run_done",
            "options": {
                "action": action,
                "path": self.path,
                "rids": rids,
            }
        }
        try:
            data = self.daemon_post(req, server=self.options.node, silent=True)
            if data and data["status"] != 0:
                if "error" in data:
                    self.log.warning("notify scheduler action is done failed: %s", data["error"])
                else:
                    self.log.warning("notify scheduler action is done failed")
        except Exception as exc:
            self.log.warning("notify scheduler action is done failed: %s", str(exc))

    def post_object_status(self, data):
        req = {
            "action": "object_status",
            "options": {
                "path": self.path,
                "data": data,
            }
        }
        try:
            data = self.daemon_post(
                req,
                server=self.options.server,
                node=self.options.node,
                silent=True,
                timeout=DEFAULT_DAEMON_TIMEOUT,
            )
            status, error, info = self.parse_result(data)
            if status and data.get("errno") != ECONNREFUSED:
                # ECONNREFUSED (ie daemon down)
                if error:
                    self.log.warning("post object status failed: %s", error)
                else:
                    self.log.warning("post object status failed")
        except Exception as exc:
            self.log.warning("post object status failed: %s", str(exc))

    def set_service_monitor(self, status=None, local_expect=None, global_expect=None, stonith=None, path=None,
                            best_effort=False):
        if path is None:
            path = self.path
        if best_effort:
            log = self.log.warning
        else:
            log = self.log.error
        options = {
            "path": path,
            "status": status,
            "local_expect": local_expect,
            "global_expect": global_expect,
            "stonith": stonith,
        }
        try:
            data = self.daemon_post(
                {"action": "object_monitor", "options": options},
                server=self.options.server,
                node=self.options.node,
                silent=True,
                with_result=True,
            )
            status, error, info = self.parse_result(data)
            if info:
                for line in error.splitlines():
                    self.log.info(line)
            if status:
                # ECONNREFUSED (ie daemon down)
                if error and data.get("errno") != ECONNREFUSED:
                    for line in error.splitlines():
                        log(line)
                if not best_effort:
                    raise ex.Error
            return data
        except ex.Error:
            raise
        except Exception as exc:
            log("set monitor status failed: %s", str(exc))
            if not best_effort:
                raise

    def remote_service_config_fetch(self, nodename=None):
        node, buff = self.remote_service_config(nodename=nodename)
        if not buff:
            raise ex.Error
        tmpfile = tempfile.NamedTemporaryFile()
        fname = tmpfile.name
        tmpfile.close()
        with open(fname, "w") as tmpfile:
            tmpfile.write(buff)
        return node, fname

    def remote_service_config(self, nodename=None):
        req = {
            "action": "object_config",
            "options": {
                "path": self.path,
            }
        }
        node = nodename if nodename else "ANY"
        data = self.daemon_get(req, server=self.options.server, node=node, silent=True)
        if not data or data.get("status", 1) != 0:
            try:
                err = data.get("error", "")
            except Exception:
                err = ""
            raise ex.Error(err)
        if "nodes" in data:
            for node in data["nodes"]:
                break
            try:
                return node, data["nodes"][node]["data"]
            except Exception:
                return None, None
        try:
            return None, data["data"]
        except Exception:
            return None, None

    def daemon_service_action(self, action=None, options=None, server=None, node=None, sync=True, timeout=None,
                              collect=False, action_mode=True):
        """
        Execute a service action on a peer node.
        If sync is set, wait for the action result.
        """
        if timeout is not None:
            timeout = convert_duration(timeout)
        if options is None:
            options = {}
        req = {
            "action": "object_action",
            "options": {
                "path": self.path,
                "sync": sync,
                "action": action,
                "options": options,
            }
        }
        if not node and action in ACTION_ANY_NODE:
            node = "ANY"
        display_node = node if node else server
        if action_mode:
            self.log.info("request action '%s' on node %s", action, display_node)
        try:
            data = self.daemon_post(
                req,
                server=server,
                silent=True,
                timeout=timeout,
                node=node,
            )
        except Exception as exc:
            self.log.error("request action '%s' on node %s failed: %s",
                           action, display_node, exc)
            return 1
        status, error, info = self.parse_result(data)
        if error:
            self.log.error(error)

        def print_node_data(nodename, data):
            if data.get("out") and len(data["out"]) > 0:
                for line in data["out"].splitlines():
                    print(line)
            if data.get("err") and len(data["err"]) > 0:
                for line in data["err"].splitlines():
                    print(line, file=sys.stderr)

        if collect:
            if "data" not in data:
                return 0
            data = data["data"]
            return data["ret"], data.get("out", ""), data.get("err", "")
        else:
            if data is None:
                return 1
            if "nodes" in data:
                if self.options.format in ("json", "flat_json"):
                    if len(data["nodes"]) == 1:
                        for _data in data["nodes"].values():
                            return _data
                    return data
                else:
                    ret = 0
                    for n, _data in data["nodes"].items():
                        status = _data.get("status", 0)
                        _data = _data.get("data", {})
                        print_node_data(n, _data)
                        ret += _data.get("ret", 0) + status
                    return ret
            else:
                if "data" not in data:
                    return 0
                data = data["data"]
                print_node_data(server, data)
                return data.get("ret", 0)

    def logs(self):
        node = "*"
        if self.options.local:
            node = None
        elif self.options.node:
            node = self.options.node
        nodes = self.node.nodes_selector(node)
        auto = sorted(nodes, reverse=True)
        self._backlogs(server=self.options.server, node=node,
                       backlog=self.options.backlog,
                       debug=self.options.debug,
                       auto=auto)
        if not self.options.follow:
            return
        try:
            self._followlogs(server=self.options.server, node=node,
                             debug=self.options.debug, auto=auto)
        except ex.Signal:
            return
        except (OSError, IOError) as exc:
            if exc.errno == 32:
                # broken pipe
                return

    def _backlogs(self, server=None, node=None, backlog=None, debug=False, auto=None):
        from utilities.render.color import colorize_log_line
        lines = []
        for line in self.daemon_backlogs(server, node, backlog, debug):
            try:
                line = colorize_log_line(line, auto=auto)
            except Exception as exc:
                print(exc, file=sys.stderr)
            if line:
                print(line)
                sys.stdout.flush()

    def _followlogs(self, server=None, node=None, debug=False, auto=None):
        from utilities.render.color import colorize_log_line
        lines = []
        for line in self.daemon_logs(server, node, debug):
            line = colorize_log_line(line, auto=auto)
            if line:
                print(line)
                sys.stdout.flush()

    def support(self):
        """
        Send a tarball to the OpenSVC support upload site.
        """
        if self.node.sysreport_mod is None:
            return

        todo = [
          ('INC', os.path.join(Env.paths.pathlog, "node.log")),
          ('INC', os.path.join(Env.paths.pathlog, "xmlrpc.log")),
          ('INC', os.path.join(self.log_d, self.name+".log")),
          ('INC', self.var_d),
        ]

        collect_d = os.path.join(Env.paths.pathvar, "support")
        try:
            shutil.rmtree(collect_d)
        except:
            pass
        srep = self.node.sysreport_mod.SysReport(node=self.node, collect_d=collect_d, compress=True)
        srep.todo += todo
        srep.collect()
        tmpf = srep.archive(srep.full)

        try:
            import requests
        except:
            print("uploading the support tarball requires the requests module", file=sys.stderr)
            return 1
        print("uploading the support tarball")
        content_size = os.stat(tmpf).st_size
        import base64
        with open(tmpf, 'rb') as filep:
            files = {"upload": filep}
            headers = {
                'Content-Type': 'application/octet-stream',
                'Content-Filename': "%s_%s_at_%s.tar.gz" %
                                    (self.namespace if self.namespace else "", self.name, Env.nodename),
                'Content-length': '%d' % content_size,
                'Content-Range': 'bytes 0-%d/%d' % (content_size-1, content_size),
                'Maxlife-Unit': "DAYS",
                'Maxlife-Value': "7",
            }
            resp = requests.post("https://sfx.opensvc.com/apis/rest/items",
                                 headers=headers,
                                 data=base64.b64encode(filep.read(content_size)),
                                 auth=("user", "support"))
        loc = resp.headers.get("Content-Location")
        if not loc:
            print(json.dumps(resp.content, indent=4), file=sys.stderr)
            return 1
        print("uploaded as https://sfx.opensvc.com%s" % loc)

    def skip_config_section(self, rid):
        if rid == "DEFAULT":
            return False
        self.init_resources()
        if self.encap and rid not in self.resources_by_id:
            return True
        if not self.encap and rid in self.encap_resources:
            return True
        return False

    def exists(self):
        """
        Return True if the service exists, ie has a configuration file on the
        local node.
        """
        return os.path.exists(self.paths.cf)

    def set_lazy(self, prop, val):
        """
        Expose the set_lazy(self, ...) utility function as a method,
        so Svc() users don't have to import it from utilities.
        """
        set_lazy(self, prop, val)

    def unset_lazy(self, prop):
        """
        Expose the unset_lazy(self, ...) utility function as a method,
        so Svc() users don't have to import it from utilities.
        """
        unset_lazy(self, prop)

    def unset_conf_lazy(self):
        self.clear_ref_cache()
        self.init_nodes()
        self.unset_lazy("cd")
        self.unset_lazy("nodes")
        self.unset_lazy("ordered_nodes")
        self.unset_lazy("peers")
        self.unset_lazy("ordered_peers")
        self.unset_lazy("flex")
        self.unset_lazy("flex_max")
        self.unset_lazy("flex_target")

    def unset_all_lazy(self):
        self.clear_ref_cache()
        self.init_nodes()
        if self.volatile:
            exclude = ["cd"]
        else:
            exclude = []
        unset_all_lazy(self, exclude=exclude)
        for res in self.resources_by_id.values():
            unset_all_lazy(res)

    def action_triggers(self, trigger, action, **kwargs):
        """
        Executes a resource trigger. Guess if the shell mode is needed from
        the trigger syntax.
        """
        action_triggers(self, trigger, action, **kwargs)

    def status(self):
        """
        Return the aggregate status a service.
        """
        refresh = self.options.refresh or (not self.encap and self.options.cron)
        data = self.print_status_data(mon_data=False, refresh=refresh)
        return core.status.Status(data.get("overall", "n/a")).value()

    @fcache
    def get_mon_data(self):
        paths = [self.path]
        paths += [resolve_path(p.split("@")[0], namespace=self.namespace)
                  for p in self.parents+self.children_and_slaves]
        selector = ",".join(paths)
        data = self.node._daemon_status(silent=True, selector=selector)
        if data is not None and "monitor" in data:
            return data["monitor"]
        return {}

    def get_smon_data(self):
        data = {}
        try:
            mon_data = self.get_mon_data()
            data["compat"] = mon_data["compat"]
            data["service"] = mon_data["services"][self.path]
            data["instances"] = {}
            for nodename in mon_data["nodes"]:
                try:
                    data["instances"][nodename] = mon_data["nodes"][nodename]["services"]["status"][self.path]["monitor"]
                except KeyError:
                    pass
            return data
        except Exception:
            return

    @lazy
    def status_data_dump(self):
        return os.path.join(self.var_d, "status.json")

    def status_data_dump_outdated(self):
        """
        Return True if the status data dump is older than the last config file
        modification time.
        """
        try:
            return os.stat(self.paths.cf).st_mtime > os.stat(self.status_data_dump).st_mtime
        except OSError as exc:
            return True

    def load_status_json(self):
        """
        Return a structure containing hierarchical status of
        the service and monitor information. Fetch CRM status from cache if
        possible and allowed by kwargs.
        """
        if self.status_data_dump_outdated():
            return
        try:
            with open(self.status_data_dump, 'r') as filep:
                data = json.load(filep)
        except (OSError, ValueError):
            return
        running = self.get_running(data.get("resources", {}).keys())
        if running:
            data["running"] = running
        return data

    def locking_status_data_eval(self, refresh=False):
        waitlock = convert_duration(self.options.waitlock)
        if waitlock is None or waitlock < 0:
            waitlock = self.lock_timeout
        # use a different lock to not block the faster "from cache" codepath
        lockfile = os.path.join(self.var_d, "lock.status")
        try:
            with utilities.lock.cmlock(timeout=waitlock, delay=1, lockfile=lockfile, intent="status"):
                return self.print_status_data_eval(refresh=refresh)
        except utilities.lock.LOCK_EXCEPTIONS as exc:
            raise ex.AbortAction(str(exc))

    def status_smon_data(self, mon_data=False):
        if not mon_data:
            return {}
        mon_data = self.get_smon_data()
        data = {"cluster": {}}
        try:
            data["cluster"]["compat"] = mon_data["compat"]
        except:
            pass
        try:
            data["cluster"]["avail"] = mon_data["service"]["avail"]
        except:
            pass
        try:
            data["cluster"]["overall"] = mon_data["service"]["overall"]
        except:
            pass
        try:
            data["cluster"]["placement"] = mon_data["service"]["placement"]
        except:
            pass
        try:
            data["monitor"] = mon_data["instances"][Env.nodename]
        except:
            pass
        return data

    def print_status_data(self, from_resource_status_cache=False, mon_data=False, refresh=False):
        if from_resource_status_cache or refresh:
            data = None
        else:
            data = self.load_status_json()
        if data is None:
            data = self.locking_status_data_eval(refresh=refresh)
        data.update(self.status_smon_data(mon_data=mon_data))
        return data

    def print_status_data_eval(self, refresh=False, write_data=True, clear_rstatus=False):
        """
        Return a structure containing hierarchical status of
        the service.
        """
        now = time.time()
        data = {
            "updated": now,
            "kind": self.kind,
        }
        if write_data:
            self.write_status_data(data)
        return data

    def csum_status_data(self, data):
        """
        This checksum is used by the collector thread to detect changes
        requiring a collector update.
        """
        h = hashlib.md5()

        def fn(h, val):
            if type(val) == dict:
                for key in sorted(val.keys()):
                    _val = val[key]
                    if key in ("status_updated", "updated", "mtime", "csum"):
                        continue
                    h = fn(h, _val)
            elif type(val) == list:
                for _val in val:
                    h = fn(h, _val)
            else:
                h.update(repr(val).encode())
            return h
        return fn(h, data).hexdigest()

    def write_status_data(self, data):
        if self.volatile:
            return
        data["csum"] = self.csum_status_data(data)
        fpath = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", delete=False, dir=self.var_d, prefix='status.json.') as filep:
                fpath = filep.name
                json.dump(data, filep)
            os.utime(fpath, (-1, data["updated"]))
            shutil.move(fpath, self.status_data_dump)
            self.post_object_status(data)
        except Exception as exc:
            self.log.warning("failed to update %s: %s",
                             self.status_data_dump, str(exc))
        finally:
            if fpath:
                try:
                    os.unlink(fpath)
                except Exception:
                    pass
        return data

    def update_status_data(self):
        self.log.debug("update status dump")
        # print_status_data() with from_resource_status_cache=True does a status.json write
        return self.print_status_data(from_resource_status_cache=True)

    def env_section_keys_evaluated(self):
        """
        Return the dict of key/val pairs in the [env] section of the
        service configuration, after dereferencing.
        """
        return self.env_section_keys(evaluate=True)

    def env_section_keys(self, evaluate=False):
        """
        Return the dict of key/val pairs in the [env] section of the
        service configuration, without dereferencing.
        """
        config = self.print_config_data()
        try:
            from collections import OrderedDict
            data = OrderedDict()
        except ImportError:
            data = {}
        for key in config.get("env", {}).keys():
            key = key.split("@")[0]
            if key in data:
                continue
            if evaluate:
                data[key] = self.conf_get('env', key)
            else:
                data[key] = config["env"][key]
        return data

    def print_status(self):
        """
        Display in human-readable format the hierarchical service status.
        """
        if want_context():
            mon_data = self.get_mon_data()
            if self.options.node:
                nodename = self.options.node
            else:
                nodename = [n for n in mon_data.get("nodes", {})][0]
            data = mon_data.get("nodes", {}).get(nodename, {}).get("services", {}).get("status", {}).get(self.path, {})
            data["cluster"] = mon_data.get("services", {}).get(self.path, {})
            data["cluster"]["compat"] = mon_data.get("compat")
        elif self.options.node:
            nodename = self.options.node
            mon_data = self.get_mon_data()
            data = mon_data.get("nodes", {}).get(self.options.node, {}).get("services", {}).get("status", {}).get(self.path, {})
            data["cluster"] = mon_data.get("services", {}).get(self.path, {})
            data["cluster"]["compat"] = mon_data.get("compat")
        else:
            nodename = Env.nodename
            data = self.print_status_data(mon_data=True, refresh=self.options.refresh)

        if self.options.format is not None or self.options.jsonpath_filter:
            return data

        # discard disabled resources ?
        if self.options.show_disabled is not None:
            discard_disabled = not self.options.show_disabled
        else:
            discard_disabled = not self.show_disabled

        from utilities.render.instance import format_instance
        mon_data = self.get_mon_data()
        format_instance(self.path, data, mon_data=mon_data, discard_disabled=discard_disabled, nodename=nodename)

    def purge(self):
        self.options.unprovision = True
        self.delete()

    def delete(self):
        """
        The 'delete' action entrypoint.
        If no resource specifier is set, remove all service files in
        <pathetc>.
        If a resource specifier is set, only delete the corresponding
        sections in the configuration file.
        """
        rids = self.action_rid
        if rids:
            self.delete_sections(rids)
        else:
            self.delete_service_conf()
            self.delete_service_logs()
            self.set_purge_collector_tag()

    def delete_service_logs(self):
        """
        Delete the service configuration logs
        """
        import glob
        patterns = [
            os.path.join(self.log_d, self.name+".log*"),
            os.path.join(self.log_d, self.name+".debug.log*"),
            os.path.join(self.log_d, '.'+self.name+".log*"),
            os.path.join(self.log_d, '.'+self.name+".debug.log*"),
            os.path.join(self.var_d, "frozen"),
        ]
        for pattern in patterns:
            for fpath in glob.glob(pattern):
                self.log.info("remove %s", fpath)
                os.unlink(fpath)

    def delete_service_sched(self):
        dpath = os.path.join(self.var_d, "scheduler")
        self.log.info("remove %s", dpath)
        try:
            shutil.rmtree(dpath)
        except OSError:
            # errno 39: not empty (racing with a writer)
            pass

    def purge_var_d(self):
        scoped = self.command_is_scoped()
        for res in self.get_resources():
            if scoped and res.skip:
                continue
            res.purge_var_d()

    def delete_service_conf(self):
        """
        Delete the service configuration files
        """
        dpaths = [
            self.paths.alt_initd,
            self.paths.initd,
            self.var_d,
        ]
        fpaths = [
            self.paths.cf,
            self.paths.initd,
        ]
        for fpath in fpaths:
            if os.path.exists(fpath) and \
               (os.path.islink(fpath) or os.path.isfile(fpath)):
                self.log.info("remove %s", fpath)
                os.unlink(fpath)
        for dpath in dpaths:
            if os.path.exists(dpath):
                self.log.info("remove %s", dpath)
                try:
                    shutil.rmtree(dpath)
                except OSError:
                    # errno 39: not empty (racing with a writer)
                    pass

    def set_purge_collector_tag(self):
        if not self.node.collector_env.dbopensvc:
            return
        if not self.options.purge_collector:
            return
        try:
            self._set_purge_collector_tag()
        except Exception as exc:
            self.log.warning(exc)

    def _set_purge_collector_tag(self):
        self.log.info("tag the service for purge on the collector")
        try:
            data = self.collector_rest_get("/services/self", {"props": "svc_id"})
            svc_id = data["data"][0]["svc_id"]
            data = self.collector_rest_get("/tags/@purge")
            if len(data["data"]) == 0:
                data = self.collector_rest_post("/tags", {"tag_name": "@purge"})
            data = self.collector_rest_post("/tags/services", {
                "svc_id": svc_id,
                "tag_id": data["data"][0]["tag_id"],
            })
            if "info" in data:
                self.log.info(data["info"])
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])

    def collector_rest_get(self, *args, **kwargs):
        kwargs["path"] = self.path
        return self.node.collector_rest_get(*args, **kwargs)

    def collector_rest_post(self, *args, **kwargs):
        kwargs["path"] = self.path
        return self.node.collector_rest_post(*args, **kwargs)

    def collector_rest_put(self, *args, **kwargs):
        kwargs["path"] = self.path
        return self.node.collector_rest_put(*args, **kwargs)

    def collector_rest_delete(self, *args, **kwargs):
        kwargs["path"] = self.path
        return self.node.collector_rest_delete(*args, **kwargs)

    def options_to_rids(self, options, action):
        rid = options.get("rid", [])
        if rid is None:
            rid = []
        elif is_string(rid):
            if rid:
                rid = rid.split(',')
            else:
                rid = []
        return set(rid)

    def set_skip_resources(self, *args, **kwargs):
        pass

    def init_resources(self):
        return 0

    def freeze(self):
        pass

    def thaw(self):
        pass

    def frozen(self):
        return 0

    def action_rid_dependencies(self, action, rid):
        return set()

    def get_running(self, *args, **kwargs):
        return []

    def get_resources(self, *args, **kwargs):
        return []

    def destination_node_sanity_checks(self, *args, **kwargs):
        return

    def encap_cmd(self, *args, **kwargs):
        pass

    def get_resource(self, *args, **kwargs):
        return

    def rollback(self):
        pass

    @lazy
    def parents(self):
        return []

    @lazy
    def children(self):
        return []

    @lazy
    def slaves(self):
        return []

    @lazy
    def children_and_slaves(self):
        return []

    @lazy
    def scale_target(self):
        pass

    def pg_stats(self):
        return {}

    @lazy
    def flex_min(self):
        return 0

    @lazy
    def flex_max(self):
        return 0

    @lazy
    def flex_target(self):
        return 0

    @lazy
    def flex_cpu_low_threshold(self):
        return 0

    @lazy
    def flex_cpu_high_threshold(self):
        return 0

    @lazy
    def comment(self):
        return ""

    @lazy
    def app(self):
        return

    @lazy
    def ha(self):
        return False

    def get_flex_primary(self):
        return ""

    def get_drp_flex_primary(self):
        return ""

    def postinstall(self, *args, **kwargs):
        pass

    def post_commit(self):
        self.unset_all_lazy()
        self.sched.reconfigure()

    def configure_scheduler(self, *args, **kwargs):
        pass


class Svc(PgMixin, BaseSvc):
    """
    The svc kind class.
    A service is a collection of resources.
    It exposes operations methods like provision, unprovision, stop, start,
    and sync.
    """
    kind = "svc"

    @lazy
    def kwstore(self):
        from .svcdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .svcdict import KEYS, SECTIONS, DATA_SECTIONS
        from utilities.drivers import load_drivers
        load_drivers(SECTIONS + DATA_SECTIONS)
        return KEYS

    def load_driver(self, driver_group, driver_basename):
        try:
            driver_group, driver_basename = DRV_GRP_XLATE[driver_group]
        except KeyError:
            pass
        if driver_group in ("container", "task") and driver_basename == "oci":
            driver_basename = self.node.oci
        elif driver_group == "ip" and driver_basename == "docker":
            driver_basename = "netns"
        elif driver_group == "disk" and driver_basename == "lvm":
            driver_basename = "vg"
        elif driver_group == "disk" and driver_basename == "veritas":
            driver_basename = "vxdg"
        return driver_import("resource", driver_group, driver_basename)

    @lazy
    def ha(self):
        if self.topology == "flex":
            return True
        if self.has_monitored_resources():
            return True
        if self.orchestrate == "ha":
            return True
        return False

    @lazy
    def parents(self):
        return self.oget("DEFAULT", "parents")

    @lazy
    def placement(self):
        return self.oget("DEFAULT", "placement")

    @lazy
    def stonith(self):
        return self.oget("DEFAULT", "stonith")

    @lazy
    def comment(self):
        return self.oget("DEFAULT", "comment")

    @lazy
    def aws(self):
        return self.oget("DEFAULT", "aws")

    @lazy
    def disable_rollback(self):
        return not self.oget("DEFAULT", "rollback")

    @lazy
    def aws_profile(self):
        return self.oget("DEFAULT", "aws_profile")

    @lazy
    def show_disabled(self):
        return self.oget("DEFAULT", "show_disabled")

    @lazy
    def pre_monitor_action(self):
        return self.oget("DEFAULT", "pre_monitor_action")

    @lazy
    def monitor_action(self):
        return self.oget("DEFAULT", "monitor_action")

    @lazy
    def bwlimit(self):
        return self.oget("DEFAULT", "bwlimit")

    @lazy
    def encap(self):
        return Env.nodename in self.encapnodes

    @lazy
    def presnap_trigger(self):
        return self.oget("DEFAULT", "presnap_trigger")

    @lazy
    def postsnap_trigger(self):
        return self.oget("DEFAULT", "postsnap_trigger")

    @lazy
    def pool(self):
        return self.oget("DEFAULT", "pool")

    @lazy
    def size(self):
        return self.oget("DEFAULT", "size")

    @lazy
    def orchestrate(self):
        if self.encap:
            return "no"
        return self.oget("DEFAULT", "orchestrate")

    @lazy
    def topology(self):
        return self.oget("DEFAULT", "topology")

    @lazy
    def access(self):
        """
        Volume service property
        """
        return self.oget("DEFAULT", "access")

    @lazy
    def children(self):
        children = self.oget('DEFAULT', "children")
        for i, child in enumerate(children):
            children[i] = resolve_path(child, self.namespace)
        return children

    @lazy
    def slaves(self):
        slaves = self.oget('DEFAULT', "slaves")
        for i, slave in enumerate(slaves):
            slaves[i] = resolve_path(slave, self.namespace)
        return slaves

    @lazy
    def children_and_slaves(self):
        data = self.children + self.slaves
        if self.scaler is not None:
            data += self.scaler.slaves
        return data

    @lazy
    def scaler_slave(self):
        return self.oget('DEFAULT', "scaler_slave")

    @lazy
    def scale_target(self):
        val = self.oget("DEFAULT", "scale")
        if isinstance(val, int) and val < 0:
            val = 0
        return val

    def get_flex_primary(self):
        try:
            flex_primary = self.conf_get("DEFAULT", "flex_primary")
        except ex.OptNotFound as exc:
            if len(self.ordered_nodes) > 0:
                flex_primary = self.ordered_nodes[0]
            else:
                flex_primary = ""
        return flex_primary

    def get_drp_flex_primary(self):
        try:
            drp_flex_primary = self.conf_get("DEFAULT", "drp_flex_primary")
        except ex.OptNotFound as exc:
            if len(self.ordered_drpnodes) > 0:
                drp_flex_primary = self.ordered_drpnodes[0]
            else:
                drp_flex_primary = ""
        return drp_flex_primary

    @lazy
    def scaler(self):
        if self.scale_target is None:
            return
        data = Storage({
            "slaves": [],
        })
        if self.topology == "flex":
            data.width = len(self.peers)
            if data.width == 0:
                data.left = 0
            else:
                data.left = self.scale_target % data.width
            if self.scale_target == 0:
                data.slaves_count = 0
            elif data.left == 0:
                data.slaves_count = self.scale_target // data.width
            else:
                data.slaves_count = (self.scale_target // data.width) + 1
        else:
            data.width = 1
            data.left = 0
            data.slaves_count = self.scale_target

        for idx in range(data.slaves_count):
            name = str(idx) + "." + self.name
            if name not in data.slaves:
                data.slaves.append(name)
        return data

    @lazy
    def constraints(self):
        """
        Return True if no constraints is defined or if defined constraints are
        met. Otherwise return False.
        """
        try:
            return convert_boolean(self.conf_get("DEFAULT", "constraints"))
        except ex.OptNotFound:
            return True
        except ex.Error:
            return True

    @lazy
    def hard_affinity(self):
        return self.oget("DEFAULT", "hard_affinity")

    @lazy
    def hard_anti_affinity(self):
        return self.oget("DEFAULT", "hard_anti_affinity")

    @lazy
    def soft_affinity(self):
        return self.oget("DEFAULT", "soft_affinity")

    @lazy
    def soft_anti_affinity(self):
        return self.oget("DEFAULT", "soft_anti_affinity")

    @lazy
    def flex_min(self):
        try:
            val = self.oget("DEFAULT", "flex_min")
            int(val)
        except (ValueError, TypeError, ex.OptNotFound):
            val = 0
        if val < 0:
            val = 0
        nb_nodes = len(self.nodes | self.drpnodes)
        if val > nb_nodes:
            val = nb_nodes
        return val

    @lazy
    def flex_max(self):
        nb_nodes = len(self.peers)
        try:
            val = self.conf_get("DEFAULT", "flex_max")
            int(val)
        except (ValueError, TypeError, ex.OptNotFound):
            return nb_nodes
        if val > nb_nodes:
            val = nb_nodes
        if val < self.flex_min:
            val = self.flex_min
        return val

    @lazy
    def flex_target(self):
        try:
            val = self.conf_get("DEFAULT", "flex_target")
            return int(val)
        except (ValueError, TypeError, ex.OptNotFound):
            return self.flex_min

    @lazy
    def flex_cpu_low_threshold(self):
        val = self.oget("DEFAULT", "flex_cpu_low_threshold")
        if val < 0:
            return 0
        if val > 100:
            return 100
        return val

    @lazy
    def flex_cpu_high_threshold(self):
        val = self.oget("DEFAULT", "flex_cpu_high_threshold")
        if val < self.flex_cpu_low_threshold:
            return self.flex_cpu_low_threshold
        if val > 100:
            return 100
        return val

    @lazy
    def app(self):
        """
        Return the service app code.
        """
        return self.oget("DEFAULT", "app")

    def __lt__(self, other):
        """
        Order by service name
        """
        return self.path < other.path

    def register_dependency(self, action, _from, _to):
        if action not in self.dependencies:
            self.dependencies[action] = {}
        if _from not in self.dependencies[action]:
            self.dependencies[action][_from] = set()
        self.dependencies[action][_from].add(_to)

    @lazy
    def shared_resources(self):
        return [res for res in self.get_resources() if res.shared]

    def action_rid_dependencies(self, action, rid):
        if action in ("provision", "start"):
            action = "start"
        elif action in ("shutdown", "unprovision", "stop", "toc"):
            action = "stop"
        else:
            return set()
        if action not in self.dependencies:
            return set()
        if rid not in self.dependencies[action]:
            return set()
        return self.dependencies[action][rid]

    def action_rid_dependency_of(self, action, rid):
        if action in ("provision", "start"):
            action = "start"
        elif action in ("shutdown", "unprovision", "stop", "toc"):
            action = "stop"
        else:
            return set()
        if action not in self.dependencies:
            return set()
        dependency_of = set()
        for _rid, dependencies in self.dependencies[action].items():
            if rid in dependencies:
                dependency_of.add(_rid)
        return dependency_of

    def resource_handling_file(self, path):
        path = os.path.dirname(path)
        return self.resource_handling_dir(path)

    def resource_handling_dir(self, path):
        mntpts = {}
        for res in self.get_resources(["fs", "volume"]):
            if not hasattr(res, "mount_point"):
                # fs.flag for ex. has no mount_point
                continue
            mntpts[res.mount_point] = res
        while True:
            if path in mntpts.keys():
                return mntpts[path]
            path = os.path.dirname(path)
            if path == os.sep:
                return

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        return self.sched.print_schedule()

    def has_monitored_resources(self):
        for res in self.get_resources(with_encap=True):
            if res.monitor and not res.is_disabled():
                return True
        return False

    def configure_scheduler(self, action=None):
        """
        Add resource-dependent tasks to the scheduler.
        Called by the @scheduler decorator if not already run once.
        Rearm with .reconfigure_scheduler()
        """
        def need_configure(action):
            if action in (None, "print_schedule"):
                return True
            if self.options.cron and action in ("push_resinfo",
                                                "compliance_auto",
                                                "run",
                                                "resource_monitor",
                                                "sync_all",
                                                "status"):
                return True
            return False

        if not need_configure(action):
            return

        monitor_schedule = self.oget('DEFAULT', 'monitor_schedule')

        self.sched.update({
            "compliance_auto": [SchedOpts(
                "DEFAULT",
                fname="last_comp_check",
                schedule_option="comp_schedule",
                req_collector=True,
            )],
            "push_resinfo": [SchedOpts(
                "DEFAULT",
                fname="last_push_resinfo",
                schedule_option="resinfo_schedule",
                req_collector=True,
            )],
        })
        if not self.encap:
            self.sched.update({
                "status": [SchedOpts(
                    "DEFAULT",
                    fname="last_status",
                    schedule_option="status_schedule"
                )]
            })
            if self.has_monitored_resources() or monitor_schedule is not None:
                self.sched.update({
                    "resource_monitor": [SchedOpts(
                        "DEFAULT",
                        fname="last_resource_monitor",
                        schedule_option="monitor_schedule"
                    )]
                })

        resource_schedules = {}
        for resource in self.get_resources():
            if resource.is_disabled():
                continue
            try:
                if resource.confirmation is True:
                    continue
            except AttributeError:
                pass
            sopts = resource.schedule_options()
            if not sopts:
                continue
            for saction, sopt in sopts.items():
                if saction not in resource_schedules:
                    resource_schedules[saction] = [sopt]
                else:
                    resource_schedules[saction] += [sopt]
        self.sched.update(resource_schedules)

    def get_subset_parallel(self, rtype):
        """
        Return True if the resources of a resourceset can run an action in
        parallel executing per-resource workers.
        """
        rtype = rtype.split(".")[0]
        subset_section = 'subset#' + rtype
        return self.oget(subset_section, "parallel")

    def get_scsireserv(self, rid):
        """
        Get the 'scsireserv' config keyword value for rid.
        """
        return self.oget(rid, 'scsireserv')

    def add_scsireserv(self, resource):
        """
        Add a 'pr' suffixed co-resource.
        """
        try:
            if not self.get_scsireserv(resource.rid):
                # scsireserv not enabled on this resource
                return
        except Exception:
            # scsireserv not supported on this resource
            return

        try:
            sr = self.load_driver("disk", "scsireserv")
        except ImportError:
            return

        kwargs = {}
        pr_rid = resource.rid+"pr"
        kwargs["prkey"] = self.oget(resource.rid, 'prkey')
        kwargs['no_preempt_abort'] = self.oget(resource.rid, 'no_preempt_abort')

        try:
            kwargs['optional'] = self.conf_get(pr_rid, "optional")
        except ex.OptNotFound:
            kwargs['optional'] = resource.is_optional()

        try:
            kwargs['disabled'] = self.conf_get(pr_rid, "disable")
        except ex.OptNotFound:
            kwargs['disabled'] = resource.is_disabled()

        try:
            kwargs['restart'] = self.conf_get(pr_rid, "restart")
        except ex.OptNotFound:
            kwargs['restart'] = resource.restart if hasattr(resource, "restart") else False

        try:
            kwargs['monitor'] = self.conf_get(pr_rid, "monitor")
        except ex.OptNotFound:
            kwargs['monitor'] = resource.monitor

        try:
            kwargs['tags'] = self.conf_get(pr_rid, "tags")
        except ex.OptNotFound:
            kwargs['tags'] = resource.tags

        try:
            kwargs['subset'] = self.conf_get(pr_rid, "subset")
        except ex.OptNotFound:
            kwargs['subset'] = resource.subset

        try:
            kwargs['shared'] = self.conf_get(pr_rid, "shared")
        except ex.OptNotFound:
            kwargs['shared'] = resource.shared

        try:
            kwargs['standby'] = self.conf_get(pr_rid, "standby")
        except ex.OptNotFound:
            kwargs['standby'] = resource.is_standby

        kwargs['rid'] = resource.rid
        kwargs['peer_resource'] = resource

        r = sr.DiskScsireserv(**kwargs)
        self += r

    def add_requires(self, resource):
        actions = [
          'unprovision', 'provision',
          'stop', 'start',
        ]
        if resource.type == "sync":
            actions += [
                'sync_nodes', 'sync_drp',
                'sync_resync', 'sync_break',
                'sync_update',
            ]
        if resource.type in ("task.host", "task.docker", "task.podman"):
            actions += [
                'run',
            ]
        for action in actions:
            try:
                s = self.conf_get(resource.rid, action+'_requires')
            except ex.OptNotFound:
                continue
            except ValueError:
                # keyword not supported. data resources for example.
                continue
            s = s.replace("stdby ", "stdby_")
            l = s.split(" ")
            l = list(map(lambda x: x.replace("stdby_", "stdby "), l))
            setattr(resource, action+'_requires', l)

    def __iadd__(self, other):
        """
        Svc += ResourceSet
        Svc += Resource
        """
        if hasattr(other, 'resources'):
            return self.__iadd_resourceset__(other)
        elif isinstance(other, Resource):
            return self.__iadd_resource__(other)
        else:
            return self

    def __iadd_resourceset__(self, other):
        """
        Svc += ResourceSet
        """
        other.svc = self
        if other.rid in self.resourcesets_by_id:
            self.resourcesets_by_id[other.rid] += other
        else:
            self.resourcesets_by_id[other.rid] = other
        return self

    def __iadd_resource__(self, other):
        """
        Svc += Resource
        """
        if not other.rid or "#" not in other.rid:
            self.log.error("__iadd_resource__ unexpected rid: %s", other)
            return self
        if other.rset_id in self.resourcesets_by_id:
            # the resource set already exists. add resource or resourceset.
            self.resourcesets_by_id[other.rset_id] += other
        else:
            parallel = self.get_subset_parallel(other.rset_id)
            rset = ResourceSet(other.rset_id, resources=[other], parallel=parallel)
            rset.svc = self
            rset.pg_settings = self.get_pg_settings("subset#"+other.rset_id)
            self += rset

        other.svc = self
        other.pg_settings = self.get_pg_settings(other.rid)
        self.add_scsireserv(other)
        self.add_requires(other)
        self.resources_by_id[other.rid] = other

        if not other.is_disabled():
            other.on_add()

        return self

    def started_on(self):
        nodenames = []
        data = self.get_smon_data()
        if data is None:
            return []
        if "instances" not in data:
            return []
        for nodename, _data in data["instances"].items():
            status = _data.get("local_expect")
            if status == "started":
                nodenames.append(nodename)
        return nodenames

    def init_resources(self):
        if self.resources_initialized:
            return self.init_resources_errors
        self.resources_initialized = True
        if self.scale_target is not None:
            # scalers can't have resources
            return 0
        from core.objects.builder import add_resources
        self.init_resources_errors = add_resources(self)
        self.log.debug("resources initialized")
        return self.init_resources_errors

    def get_resource(self, rid, with_encap=False):
        """
        Return a resource object by id.
        Return None if the rid is not found.
        """
        self.init_resources()
        if rid in self.resources_by_id:
            return self.resources_by_id[rid]
        if with_encap and rid in self.encap_resources:
            return self.encap_resources[rid]
        return

    def get_resources(self, _type=None, discard_disabled=True, with_encap=False):
        """
        Return the list of resources matching criteria.

        <_type> can be:
          None: all resources are returned
        """
        self.init_resources()
        if with_encap:
            allresources = itertools.chain(self.resources_by_id.values(), self.encap_resources.values())
        else:
            allresources = self.resources_by_id.values()
        if _type is None:
            return allresources
        if not isinstance(_type, (list, tuple)):
            _types = [_type]
        else:
            _types = _type

        resources = []
        for resource in allresources:
            if not with_encap and not self.encap and resource.encap:
                continue
            if discard_disabled and resource.is_disabled():
                continue
            for t in _types:
                if "." in t and resource.type == t or \
                   "." not in t and t == resource.driver_group:
                    resources.append(resource)
        return resources

    def get_resourcesets(self, _type=None, strict=False):
        """
        Return the list of resourceset matching the specified types.
        """
        self.init_resources()
        if _type is None:
            _types = [res.type for res in self.resources_by_id.values()]
        elif not isinstance(_type, (set, list, tuple)):
            _types = [_type]
        else:
            _types = _type
        rsets = {}
        for _type in _types:
            rsets[_type] = {}
        for _type in _types:
            for rset in self.resourcesets_by_id.values():
                if rset.has_resource_with_types([_type], strict=strict):
                    rsets[_type][rset.rid] = rset
        _rsets = []
        for _type in _types:
            for rset_id, rset in sorted(rsets[_type].items()):
                if rset not in _rsets:
                    _rsets.append(rset)
        return _rsets

    def all_set_action(self, action=None, tags=None):
        """
        Execute an action on all resources all resource sets.
        """
        self.set_action(self.resourcesets_by_id.values(), action=action, tags=tags)

    def sub_set_action(self, _type=None, action=None, tags=None, xtags=None,
                       strict=False):
        """
        Execute an action on all resources of the resource sets of the
        specified type.
        """
        if not isinstance(_type, (list, tuple, set)):
            _type = [_type]
        rsets = []
        for __type in _type:
            _rsets = []
            for rset in self.get_resourcesets(__type, strict=strict):
                if rset not in rsets and rset not in _rsets:
                    _rsets.append(rset)

            if action in ["start", "startstandby", "provision"] or \
                    __type.startswith("sync"):
                _rsets.sort()
            else:
                _rsets.sort(reverse=True)
            rsets += _rsets
        self.set_action(rsets, _type=_type, action=action, tags=tags, xtags=xtags)

    def need_snap_trigger(self, rsets, action):
        """
        Return True if the action is a sync action and at least one of the
        specified resource sets has a resource requiring a snapshot.
        """
        if action not in ACTIONS_NEED_SNAP_TRIGGER:
            return False
        for rset in rsets:
            for resource in rset.resources:
                # avoid to run pre/post snap triggers when there is no
                # resource flagged for snap and on drpnodes
                if hasattr(resource, "snap") and resource.snap is True and \
                   Env.nodename in self.nodes:
                    return True
        return False

    def set_action(self, rsets=None, _type=None, action=None, tags=None, xtags=None):
        """
        Call the action on all sets sorted resources.
        If the sets define a pre_snap trigger run that before the action.
        If the sets define a pre_<action> trigger run that before the action.
        If the sets define a post_<action> trigger run that after the action.
        """
        if rsets is None:
            rsets = []
        if tags is None:
            tags = set()
        if xtags is None:
            xtags = set()

        def do_trigger(when):
            """
            Excecute a <when> trigger on each resource of the set,
            if the action allows triggers.
            If a trigger raises,
            * Error, stop looping over the resources and propagate up
              to the caller.
            * any other exception, save the traceback in the debug log
              and stop looping over the resources and raise an Error
            """
            aborted = []
            for rset in rsets:
                if action in ACTIONS_NO_TRIGGER or rset.all_skip(action):
                    break
                try:
                    rset.log.debug("start %s %s_action", rset.rid, when)
                    aborted += getattr(rset, when + "_action")(action, types=_type, tags=tags, xtags=xtags)
                except ex.Error:
                    raise
                except:
                    self.save_exc()
                    raise ex.Error
            return aborted

        def do_snap_trigger(when):
            """
            Execute the <when>snap trigger.
            """
            if not need_snap:
                return
            trigger = getattr(self, when + "snap_trigger")
            if trigger is None:
                return
            results = self.vcall(trigger)
            if results[0] != 0:
                raise ex.Error(results[2])

        need_snap = self.need_snap_trigger(rsets, action)

        # snapshots are created in pre_action and destroyed in post_action
        # place presnap and postsnap triggers around pre_action
        do_snap_trigger("pre")
        aborted = do_trigger("pre")
        do_snap_trigger("post")

        last = None
        for rset in rsets:
            # upto / downto break
            current = rset.driver_group
            if last and current != last and (self.options.upto == last or self.options.downto == last):
                if self.options.upto:
                    barrier = "up to %s" % self.options.upto
                else:
                    barrier = "down to %s" % self.options.downto
                self.log.info("reached '%s' barrier" % barrier)
                break
            last = current
            self.log.debug('set_action: action=%s rset=%s', action, rset.rid)
            rset.action(action, types=_type, tags=tags, xtags=xtags, xtypes=aborted)

        do_trigger("post")

    def __str__(self):
        """
        The Svc class print formatter.
        """
        output = self.path
        for rset in self.resourcesets_by_id.values():
            output += "  [%s]" % str(rset)
        return output

    def prstatus(self):
        status = core.status.Status()
        for resource in self.get_resources("disk.scsireserv"):
            status += resource.status()
        return int(status)

    def get_running(self, rids=None):
        lockfile = os.path.join(self.var_d, "lock.generic")
        running = []
        running += [self._get_running(lockfile).get("rid")]

        # sync
        lockfile = os.path.join(self.var_d, "lock.sync")
        running += [self._get_running(lockfile).get("rid")]

        # tasks
        if rids is None:
            rids = [r.rid for r in self.get_resources("task")]
        else:
            rids = [rid for rid in rids if rid.startswith("task")]
        for rid in rids:
            lockfile = os.path.join(self.var_d, rid, "run.lock")
            if self._get_running(lockfile).get("intent") == "run":
                running.append(rid)
        return [rid for rid in running if rid]

    def _get_running(self, lockfile):
        try:
            with open(lockfile, "r") as ofile:
                lock_data = json.load(ofile)
                return lock_data
        except Exception:
            pass
        return {}

    def print_resource_status(self):
        """
        Print a single resource status string.
        """
        if len(self.action_rid) != 1:
            print("action 'print_resource_status' is not allowed on mutiple "
                  "resources", file=sys.stderr)
            return 1
        for rid in self.action_rid:
            resource = self.get_resource(rid)
            if resource is None:
                print("resource %s not found" % rid)
                continue
            print(core.status.colorize_status(str(resource.status(refresh=self.options.refresh))))
        return 0

    def print_status_data_eval(self, refresh=False, write_data=True, clear_rstatus=False):
        """
        Return a structure containing hierarchical status of
        the service.
        """
        now = time.time()

        if clear_rstatus:
            # Clear resource status in-memory cache, so the value is loaded
            # from on-disk cache if refresh=False.
            # Used by the daemon which holds long lived Svc objects that can
            # have outdated in-mem caches.
            for res in self.get_resources():
                res.clear_status_cache()

        group_status = self.group_status(refresh=refresh)

        data = {
            "updated": now,
            "kind": self.kind,
            "app": self.app,
            "env": self.svc_env,
            "placement": self.placement,
            "topology": self.topology,
            "frozen": self.frozen(),
            "subsets": {},
            "resources": {},
        }
        if self.kind == "svc" and self.priority != Env.default_priority:
            data["priority"] = self.priority
        running = self.get_running()
        if running:
            data["running"] = running
        if self.topology == "flex":
            data.update({
                "flex_target": self.flex_target,
                "flex_min": self.flex_min,
                "flex_max": self.flex_max,
            })
        if not self.constraints:
            data["constraints"] = self.constraints
        if self.slaves:
            data["slaves"] = self.slaves
        if len(self.parents) > 0:
            data["parents"] = self.parents
        if len(self.children) > 0:
            data["children"] = self.children
        if self.orchestrate != "no":
            data["orchestrate"] = self.orchestrate
        if self.scale_target is not None:
            data["scale"] = self.scale_target
        if self.scaler_slave:
            data["scaler_slave"] = self.scaler_slave
        if self.scaler is not None:
            data["scaler_slaves"] = self.scaler.slaves
        if Env.nodename in self.drpnodes:
            data["drp"] = True
        if self.pool:
            data["pool"] = self.pool
        if self.size:
            data["size"] = self.size

        for sid, subset in self.resourcesets_by_id.items():
            if not subset.parallel:
                continue
            data["subsets"][sid] = {
                "parallel": subset.parallel,
            }

        containers = self.get_resources('container')
        if self.encap:
            data["encap"] = True
        elif len(containers) > 0 and self.has_encap_resources:
            data["encap"] = {}
            for container in containers:
                try:
                    data["encap"][container.rid] = self.encap_json_status(container, refresh=refresh)
                    # merge container overall status, so we propagate encap alerts
                    # up to instance and service level.
                    group_status["overall"] += core.status.Status(data["encap"][container.rid]["overall"] if "overall" in data["encap"][container.rid] else "n/a")
                    group_status["avail"] += core.status.Status(data["encap"][container.rid]["avail"] if "avail" in data["encap"][container.rid] else "n/a")
                except:
                    data["encap"][container.rid] = {"resources": {}}
                if hasattr(container, "vm_hostname"):
                    data["encap"][container.rid]["hostname"] = container.vm_hostname

        prov_states = set()
        for rset in self.get_resourcesets(strict=True):
            for resource in rset.resources:
                status = core.status.Status(resource.status(verbose=True))
                log = resource.status_logs_strlist()
                info = resource.last_status_info # refreshed by resource.status() if necessary
                tags = sorted(list(resource.tags))
                disable = resource.is_disabled()
                _data = {
                    "status": str(status),
                    "type": resource.type,
                    "label": resource.label,
                }
                prov_data = resource.provisioned_data()
                if prov_data:
                    _data["provisioned"] = prov_data
                if disable:
                    _data["disable"] = disable
                if resource.is_standby:
                    _data["standby"] = resource.is_standby
                if resource.encap:
                    _data["encap"] = resource.encap
                if resource.optional:
                    _data["optional"] = resource.optional
                if resource.monitor:
                    _data["monitor"] = resource.monitor
                if resource.nb_restart:
                    _data["restart"] = resource.nb_restart
                if len(log) > 0:
                    _data["log"] = log
                if len(info) > 0:
                    _data["info"] = info
                if len(tags) > 0:
                    _data["tags"] = tags
                if not disable and not resource.skip_unprovision and not resource.skip_provision:
                    prov_states.add(_data.get("provisioned", {}).get("state"))
                if resource.subset:
                    _data["subset"] = resource.subset
                data["resources"][resource.rid] = _data
        for group in TOP_STATUS_GROUPS:
            group_status[group] = str(group_status[group])
        for group in group_status["status_group"]:
            group_status["status_group"][group] = str(group_status["status_group"][group])
        data.update(group_status)
        if prov_states == set():
            pass
        elif prov_states <= set([True, None]):
            data["provisioned"] = True
        elif prov_states <= set([False, None]):
            data["provisioned"] = False
        else:
            data["provisioned"] = "mixed"
        if self.stonith and self.topology == "failover" and data["avail"] == "up":
            data["stonith"] = True
        if write_data:
            self.write_status_data(data)
        return data

    def get_rset_status(self, groups, refresh=False):
        """
        Return the aggregated status of all resources of the specified resource
        sets, as a dict of status indexed by resourceset id.
        """
        self.setup_environ()
        rsets_status = {}
        for rset in self.get_resourcesets(groups):
            rsets_status[rset.rid] = rset.status(refresh=refresh)
        return rsets_status

    def need_encap_resource_monitor(self):
        for res in self.encap_resources.values():
            if res.monitor or res.restart:
                return True
        return False

    def resource_monitor(self):
        """
        The resource monitor action. Refresh important resources at a different
        schedule.
        """
        from utilities.journaled_data import JournaledData
        dataset = JournaledData(
            initial_data=self.print_status_data(),
            journal_head=[],
        )
        for resource in self.get_resources():
            if resource.monitor or resource.nb_restart:
                resource.status(refresh=True)
        if self.need_encap_resource_monitor():
            self.encap_cmd(["resource_monitor"])
        data = self.print_status_data_eval(write_data=False)
        dataset.set([], data)
        diff = dataset.pop_diff()
        significant_changes = [change for change in diff if change[0][-1] not in ("updated", "csum")]
        if significant_changes:
            self.log.debug("changes detected in monitored resources: %s", significant_changes)
            self.write_status_data(data)

    def reboot(self):
        """
        A method wrapper the node reboot method.
        """
        self.node.sys_reboot()

    def crash(self):
        """
        A method wrapper the node crash method.
        """
        self.node.sys_crash()

    @lazy
    def nscfgpath(self):
        return fmt_path("", self.namespace, "nscfg")

    def nscfg(self):
        return factory("nscfg")("", self.namespace, volatile=True, node=self.node)

    def ns_pg_update(self):
        nscfg = self.nscfg()
        if not nscfg:
            return
        nscfg.pg_update(children=False)

    def do_pre_monitor_action(self):
        if self.pre_monitor_action is None:
            return
        kwargs = {}
        if "|" in self.pre_monitor_action or \
           "&&" in self.pre_monitor_action or \
           ";" in self.pre_monitor_action:
            kwargs["shell"] = True

        import shlex
        if not kwargs.get("shell", False):
            if sys.version_info[0] < 3:
                cmdv = shlex.split(self.pre_monitor_action.encode('utf8'))
                cmdv = [elem.decode('utf8') for elem in cmdv]
            else:
                cmdv = shlex.split(self.pre_monitor_action)
        else:
            cmdv = self.pre_monitor_action

        try:
            result = self.vcall(cmdv, **kwargs)
            ret = result[0]
        except OSError as exc:
            ret = 1
            if exc.errno == 8:
                self.log.error("%s exec format error: check the script shebang", self.pre_monitor_action)
            else:
                self.log.error("%s error: %s", self.pre_monitor_action, str(exc))
        except Exception as exc:
            ret = 1
            self.log.error("%s error: %s", self.pre_monitor_action, str(exc))

    def toc(self):
        """
        Call the resource monitor action.
        """
        if self.frozen():
            self.log.info("refuse to toc from a frozen instance")
            return
        self.do_pre_monitor_action()
        if self.monitor_action is None:
            return
        if not hasattr(self, self.monitor_action):
            self.log.error("invalid monitor action '%s'", self.monitor_action)
            return
        self.log.info("start monitor action '%s'", self.monitor_action)
        if self.monitor_action not in ("freezestop", "switch"):
            time.sleep(2)
        getattr(self, self.monitor_action)()

    def encap_cmd(self, cmd, verbose=False, unjoinable="raise", error="raise"):
        """
        Execute a command in all service containers.
        If error is set to "raise", stop iterating at first error.
        If error is set to "continue", log errors and proceed to the next
        container.
        """
        for container in self.get_resources('container'):
            try:
                self._encap_cmd(cmd, container=container, verbose=verbose)
            except ex.EncapUnjoinable:
                if unjoinable != "continue":
                    self.log.error("container %s is not joinable to execute "
                                   "action '%s'", container.name, ' '.join(cmd))
                    raise
                elif verbose:
                    self.log.warning("container %s is not joinable to execute "
                                     "action '%s'", container.name, ' '.join(cmd))
            except ex.Error as exc:
                if error != "continue":
                    raise

    def _encap_cmd(self, cmd, container, verbose=False, push_config=True, fwd_options=True):
        """
        Execute a command in a service container.
        """
        if container.pg_frozen():
            raise ex.Error("can't join a frozen container. abort encap command.")
        if cmd == ["start"] and container.booted:
            return '', '', 0
        if not self.has_encap_resources:
            self.log.debug("skip encap %s: no encap resource", ' '.join(cmd))
            return '', '', 0
        if not container.is_up():
            msg = "skip encap %s: the container is not running here" % ' '.join(cmd)
            if verbose:
                self.log.info(msg)
            else:
                self.log.debug(msg)
            return '', '', 0

        if self.options.slave is not None and not \
                (container.name in self.options.slave or
                 container.rid in self.options.slave):
            # no need to run encap cmd (container not specified in --slave)
            return '', '', 0

        if cmd == ['start'] and not self.command_is_scoped():
            return '', '', 0

        # make sure the container has an up-to-date service config
        if push_config:
            try:
                self._push_encap_config(container)
            except ex.Error:
                pass

        # wait for the container multi-user state
        if cmd[0] in ["start"] and hasattr(container, "wait_multi_user"):
            container.wait_multi_user()

        if fwd_options:
            options = []
            if self.options.dry_run:
                options.append('--dry-run')
                cmd = drop_option("--dry-run", cmd, drop_value=False)
            if self.options.restore:
                options.append("--restore")
                cmd = drop_option("--restore", cmd, drop_value=False)
            if self.options.force:
                options.append('--force')
                cmd = drop_option("--force", cmd, drop_value=False)
            if self.options.local and "status" not in cmd:
                options.append('--local')
                cmd = drop_option("--local", cmd, drop_value=False)
            if self.options.leader:
                options.append('--leader')
                cmd = drop_option("--leader", cmd, drop_value=False)
            if self.options.disable_rollback:
                options.append('--disable-rollback')
                cmd = drop_option("--disable-rollback", cmd, drop_value=False)
            if self.options.rid:
                options.append('--rid')
                options.append(self.options.rid if is_string(self.options.rid) else ",".join(self.options.rid))
                cmd = drop_option("--rid", cmd, drop_value=True)
            if self.options.tags:
                options.append('--tags')
                options.append(self.options.tags if is_string(self.options.tags) else ",".join(self.options.tags))
                cmd = drop_option("--tags", cmd, drop_value=True)
            if self.options.subsets:
                options.append('--subsets')
                options.append(self.options.subsets if is_string(self.options.subsets) else ",".join(self.options.subsets))
                cmd = drop_option("--subsets", cmd, drop_value=True)
        else:
            options = []

        cmd = drop_option("--slaves", cmd, drop_value=False)
        cmd = drop_option("--slave", cmd, drop_value=True)

        if self.options.namespace:
            options += ["--namespace", self.options.namespace]

        paths = Paths(osvc_root_path=container.osvc_root_path)
        cmd = [paths.om, "svc", "-s", self.path] + options + cmd
        if verbose:
            self.log.info(" ".join(cmd))

        cmd = ["env", "OSVC_DETACHED=1", "OSVC_ACTION_ORIGIN='master %s'" % os.environ.get("OSVC_ACTION_ORIGIN", "user")] + cmd
        if container is not None and hasattr(container, "rcmd") and callable(container.rcmd):
            try:
                out, err, ret = container.rcmd(cmd)
            except Exception as exc:
                self.log.error(exc)
                out, err, ret = "", "", 1
        elif hasattr(container, "runmethod"):
            cmd = container.runmethod + cmd
            out, err, ret = justcall(cmd, stdin=self.node.devnull)
        else:
            raise ex.EncapUnjoinable("undefined rcmd/runmethod in resource %s" % container.rid)

        if verbose:
            # self.log.info('logs from %s child service:', container.name)
            print(out)
            if len(err) > 0:
                print(err)
        if ret == 127:
            # opensvc is not installed
            raise ex.EncapUnjoinable
        if ret == 255:
            raise ex.EncapUnjoinable
        if "resource_monitor" in cmd:
            try:
                self.encap_json_status(container, refresh=True, push_config=False, cache=False)
            except (ex.NotAvailable, ex.EncapUnjoinable, ex.Error):
                pass
        elif "print" not in cmd and "create" not in cmd:
            self.log.info("refresh encap json status after action")
            try:
                self.encap_json_status(container, refresh=True, push_config=push_config)
            except (ex.NotAvailable, ex.EncapUnjoinable, ex.Error):
                pass
        if ret != 0:
            raise ex.Error("error from encap service command '%s': "
                           "%d\n%s\n%s" % (' '.join(cmd), ret, out, err))
        return out, err, ret

    def get_encap_json_status_path(self, rid):
        """
        Return the path of the file where the status data of the service
        encapsulated in the container identified by <rid> will be written
        for caching.
        """
        return os.path.join(self.var_d, rid, "encap.status.json")

    def purge_cache_encap_json_status(self, rid):
        """
        Delete the on-disk cache of status of the service encapsulated in
        the container identified by <rid>.
        """
        if rid in self.encap_json_status_cache:
            del self.encap_json_status_cache[rid]
        path = self.get_encap_json_status_path(rid)
        if os.path.exists(path):
            os.unlink(path)

    def put_cache_encap_json_status(self, rid, data):
        """
        Write the on-disk cache of status of the service encapsulated in
        the container identified by <rid>.
        """
        if self.volatile:
            return
        self.encap_json_status_cache[rid] = data
        path = self.get_encap_json_status_path(rid)
        directory = os.path.dirname(path)
        makedirs(directory)
        try:
            with open(path, "w") as ofile:
                json.dump(data, ofile)
                ofile.flush()
        except (IOError, OSError, ValueError):
            os.unlink(path)

    def get_cache_encap_json_status(self, rid):
        """
        Fetch the on-disk cache of status of the service encapsulated in
        the container identified by <rid>.
        """
        if rid in self.encap_json_status_cache:
            return self.encap_json_status_cache[rid]
        path = self.get_encap_json_status_path(rid)
        try:
            with open(path, 'r') as ofile:
                group_status = json.loads(ofile.read())
        except (IOError, OSError, ValueError):
            group_status = None
        return group_status

    @lazy
    def encap_groups(self):
        from .svcdict import DEPRECATED_SECTIONS
        egroups = set()
        for rid in self.encap_resources:
            egroup = rid.split('#')[0]
            if egroup in DEPRECATED_SECTIONS:
                egroup = DEPRECATED_SECTIONS[egroup][0]
            egroups.add(egroup)
        return egroups

    def encap_json_status(self, container, refresh=False, push_config=True, cache=True):
        """
        Return the status data from the agent runnning the encapsulated part
        of the service.
        """
        if container.guestos == 'windows':
            raise ex.NotAvailable

        if container.status(ignore_nostatus=True, refresh=refresh) == core.status.DOWN:
            #
            #  passive node for the vservice => forge encap resource status
            #    - encap sync are n/a
            #    - other encap res are down
            #
            group_status = {
                "avail": "down",
                "overall": "down",
                "type": container.type,
                "frozen": False,
                "resources": {},
            }
            groups = set(["container", "ip", "disk", "fs", "share"])
            for group in groups:
                if group in self.encap_groups:
                    group_status[group] = 'down'
                else:
                    group_status[group] = 'n/a'
            for resource in self.get_resources(groups):
                group = resource.driver_group
                if group not in groups:
                    continue
                if not self.encap and resource.encap:
                    group_status['resources'][resource.rid] = {'status': 'down'}

            groups = set(["app", "sync"])
            for group in groups:
                group_status[group] = 'n/a'
            for resource in self.get_resources(groups):
                group = resource.driver_group
                if group not in groups:
                    continue
                if not self.encap and resource.encap:
                    group_status['resources'][resource.rid] = {'status': 'n/a'}

            return group_status

        if not refresh and cache:
            group_status = self.get_cache_encap_json_status(container.rid)
            if group_status:
                return group_status

        group_status = {
            "avail": "n/a",
            "overall": "n/a",
            "type": container.type,
            "frozen": False,
            "resources": {},
        }
        groups = set([
            "container",
            "ip",
            "disk",
            "fs",
            "share",
            "task",
            "app",
            "sync"
        ])

        for group in groups:
            group_status[group] = 'n/a'

        cmd = ['print', 'status', '--format', 'json', '--color=no']
        if refresh:
            cmd.append('--refresh')
        try:
            results = self._encap_cmd(cmd, container, fwd_options=False, push_config=push_config)
        except ex.Error as exc:
            return group_status
        except Exception as exc:
            print(exc, file=sys.stderr)
            return group_status

        try:
            group_status = json.loads(results[0])
        except Exception:
            pass

        self.put_cache_encap_json_status(container.rid, group_status)

        return group_status

    def group_status(self, groups=None, excluded_groups=None, refresh=False):
        """
        Return the status data of the service.
        """
        if excluded_groups is None:
            excluded_groups = set()
        if groups is None:
            groups = set(self.kwstore.sections)

        status = {
            "status_group": {},
        }
        groups = groups - excluded_groups
        self.get_rset_status(groups, refresh=refresh)

        # initialise status of each group
        for group in TOP_STATUS_GROUPS:
            status[group] = core.status.Status(core.status.NA)
        for group in groups:
            status["status_group"][group] = core.status.Status(core.status.NA)

        for group in self.kwstore.sections:
            if group not in groups:
                continue
            for resource in self.get_resources(group):
                if resource.type in excluded_groups:
                    continue
                rstatus = resource.status()
                if resource.type.startswith("sync"):
                    if rstatus == core.status.UP:
                        rstatus = core.status.NA
                    elif rstatus == core.status.DOWN:
                        rstatus = core.status.WARN
                status["status_group"][group] += rstatus
                if resource.is_optional():
                    status["optional"] += rstatus
                else:
                    status["avail"] += rstatus
                if resource.status_logs_count(levels=["warn", "error"]) > 0:
                    status["overall"] += core.status.WARN

        if status["avail"].status == core.status.STDBY_UP_WITH_UP:
            # now we know the avail status we can promote
            # stdbyup to up
            status["avail"].status = core.status.UP
            for group in status:
                if status[group] == core.status.STDBY_UP:
                    status[group].status = core.status.UP
        elif status["avail"].status == core.status.STDBY_UP_WITH_DOWN:
            status["avail"].status = core.status.STDBY_UP

        if status["optional"].status == core.status.STDBY_UP_WITH_UP:
            status["optional"].status = core.status.UP
        elif status["optional"].status == core.status.STDBY_UP_WITH_DOWN:
            status["optional"].status = core.status.STDBY_UP

        status["overall"] += core.status.Status(status["avail"])
        status["overall"] += core.status.Status(status["optional"])

        return status

    def print_exposed_devs(self):
        self.print_devs(categories=["exposed"])

    def print_sub_devs(self):
        self.print_devs(categories=["sub"])

    def print_base_devs(self):
        self.print_devs(categories=["base"])

    def print_devs(self, categories=None):
        """
        Print the list of devices the service exposes.
        """
        if categories is None:
            categories = ["exposed", "sub", "base"]
        data = self.devs(categories=categories)
        if self.options.format is not None or self.options.jsonpath_filter:
            return data
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        node1 = tree.add_node()
        node1.add_column(self.path, color.BOLD)
        for rid, _data in data.items():
            node = node1.add_node()
            text = "%s (%s)" % (rid, _data["type"])
            node.add_column(text, color.BROWN)
            for cat, __data in _data.items():
                if cat == "type":
                    continue
                catnode = node.add_node()
                catnode.add_column(cat, color.LIGHTBLUE)
                for dev in __data:
                    devnode = catnode.add_node()
                    devnode.add_column(dev)
        tree.out()

    def devs(self, categories=None):
        """
        Return the list of devices the service exposes.
        """
        if categories is None:
            categories = ("exposed", "sub", "base")
        data = {}
        if self.action_rid == []:
            resources = self.get_resources()
        else:
            resources = [self.get_resource(rid) for rid in self.action_rid]
        for resource in resources:
            if resource is None:
                continue
            for cat in categories:
                try:
                    devs = sorted(list(getattr(resource, cat+"_devs")()))
                except Exception as exc:
                    import traceback
                    traceback.print_exc()
                    continue
                if len(devs) == 0:
                    continue
                if resource.rid not in data:
                    data[resource.rid] = {}
                data[resource.rid]["type"] = resource.type
                data[resource.rid][cat] = devs
        return data

    def sub_devs(self):
        """
        Return the list of sub devices of each resource, aggregated as a single
        list. Used by the checkers to parent checks to a service.
        """
        data = self.devs(categories=["sub"])
        devs = set()
        for rid, _data in data.items():
            devs |= set(_data.get("sub", []))
        return devs

    def exposed_devs(self):
        """
        Return the list of exposed devices of each resource, aggregated as a
        single list. Used by the fs unprovisioner.
        """
        data = self.devs(categories=["exposed"])
        devs = set()
        for rid, _data in data.items():
            devs |= set(_data.get("exposed", []))
        return devs

    def run(self):
        self.master_run()
        self.slave_run()

    @_master_action
    def master_run(self):
        self.sub_set_action("task", "run")

    @_slave_action
    def slave_run(self):
        self.encap_cmd(['run'], verbose=True)

    def startstandby(self):
        self.master_startstandby()
        self.slave_startstandby()

    @_master_action
    def master_startstandby(self):
        self.sub_set_action(START_GROUPS, "startstandby", xtags=set(["zone", "docker", "podman"]))

    @_slave_action
    def slave_startstandby(self):
        cmd = self.prepare_async_cmd()
        self.encap_cmd(cmd, verbose=True)

    def start(self):
        self.abort_start()
        self.master_start()
        self.slave_start()

    @_master_action
    def master_start(self):
        self.ns_pg_update()
        self.sub_set_action(START_GROUPS, "start", xtags=set(["zone", "docker", "podman"]))

    @_slave_action
    def slave_start(self):
        cmd = self.prepare_async_cmd()
        self.encap_cmd(cmd, verbose=True)

    def rollback(self):
        self.sub_set_action(STOP_GROUPS, "rollback", xtags=set(["zone", "docker", "podman"]))

    def stop(self):
        self.slave_stop()
        self.master_stop()

    @_master_action
    def master_stop(self):
        self.sub_set_action(STOP_GROUPS, "stop", xtags=set(["zone", "docker", "podman"]))
        self.pg_remove()

    @_slave_action
    def slave_stop(self):
        self.encap_cmd(['stop'], verbose=True, unjoinable="continue")

    @_slave_action
    def slave_freezestop(self):
        self.encap_cmd(['stop'], verbose=True, unjoinable="continue", error="continue")

    def boot(self):
        self.options.force = True
        self.master_boot()

    @_master_action
    def master_boot(self):
        self.sub_set_action(START_GROUPS, "boot")

    def shutdown(self):
        self.options.force = True
        self.slave_shutdown()
        self.master_shutdown()

    @_master_action
    def master_shutdown(self):
        self.sub_set_action(STOP_GROUPS, "shutdown", xtags=set(["zone", "docker", "podman"]))
        self.pg_remove()

    @_slave_action
    def slave_shutdown(self):
        self.encap_cmd(['shutdown'], verbose=True, unjoinable="continue", error="continue")

    def unprovision(self):
        self.slave_unprovision()
        self.master_unprovision()

    @_master_action
    def master_unprovision(self):
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(["zone", "docker", "podman"]))
        self.sub_set_action(STOP_GROUPS, "unprovision", xtags=set(["zone", "docker", "podman"]))
        if not self.command_is_scoped():
            self.pg_remove()
            self.delete_service_sched()
        self.purge_var_d()

    @_slave_action
    def slave_unprovision(self):
        cmd = self.prepare_async_cmd()
        self.encap_cmd(cmd, verbose=True)

    def provision(self):
        self.master_provision()
        self.slave_provision()

    @_master_action
    def master_provision(self):
        self.sub_set_action(START_GROUPS, "provision", xtags=set(["zone", "docker", "podman"]))

        if not self.options.disable_rollback and len(self.peers) > 1:
            # set by the daemon on the placement leaders.
            # return the service to standby if not a placement leader
            self.rollback()

    @_slave_action
    def slave_provision(self):
        cmd = self.prepare_async_cmd()
        self.encap_cmd(cmd, verbose=True)

    def set_provisioned(self):
        self.sub_set_action(START_GROUPS, "set_provisioned")

    def set_unprovisioned(self):
        self.sub_set_action(START_GROUPS, "set_unprovisioned")

    def abort_start(self):
        """
        Give a chance to all resources concerned by the action to voice up
        their rebutal of the action before it begins.
        """
        self.abort_start_done = True
        resources = [res for res in self.get_resources()
                     if not res.skip and not res.is_disabled() and hasattr(res, "abort_start")]
        if len(resources) < 2:
            parallel = False
        else:
            try:
                import concurrent.futures
            except ImportError:
                parallel = False
            else:
                parallel = True

        procs = {}
        if not parallel:
            for resource in resources:
                if resource.abort_start():
                    raise ex.Error("start aborted due to resource %s "
                                   "conflict" % resource.rid)
        else:
            def wrapper(func):
                try:
                    if func():
                        return 1
                except Exception:
                    return 1
                return 0

            err = []

            with concurrent.futures.ThreadPoolExecutor() as executor:
                for resource in resources:
                    procs[executor.submit(wrapper, resource.abort_start)] = resource.rid
                for future in concurrent.futures.as_completed(procs):
                    rid = procs[future]
                    result = future.result()
                    if result:
                        err.append(rid)

            if len(err) > 0:
                raise ex.Error("start aborted due to resource %s "
                               "conflict" % ",".join(err))

    def refresh_ip_status(self):
        """ Used after start/stop container because the ip resource
            status change after its own start/stop
        """
        for resource in self.get_resources("ip"):
            resource.status(refresh=True)

    def dns_update(self):
        """
        Call the dns update method of each resource.
        """
        self.all_set_action("dns_update")

    def postsync(self):
        """ action triggered by a remote master node after
            sync_nodes and sync_drp. Typically make use of files
            received in var/
        """
        self.all_set_action("postsync")

    def presync(self):
        """ prepare files to send to slave nodes in var/.
            Each resource can prepare its own set of files.
        """
        if self.presync_done:
            return
        self.all_set_action("presync")
        self.presync_done = True

    def sync_nodes(self):
        rtypes = [
            "sync.rsync",
            "sync.zfs",
            "sync.btrfs",
            "sync.docker",
            "sync.dds",
        ]
        if not self.can_sync(rtypes, 'nodes'):
            return
        self.presync()
        self.sub_set_action(rtypes, "sync_nodes")

    def sync_drp(self):
        rtypes = [
            "sync.rsync",
            "sync.zfs",
            "sync.btrfs",
            "sync.docker",
            "sync.dds",
        ]
        if not self.can_sync(rtypes, 'drpnodes'):
            return
        self.presync()
        self.sub_set_action(rtypes, "sync_drp")

    def sync_swap(self):
        rtypes = [
            "sync.netapp",
            "sync.symsrdfs",
            "sync.hp3par",
            "sync.nexenta",
        ]
        self.sub_set_action(rtypes, "sync_swap")

    def sync_revert(self):
        rtypes = [
            "sync.hp3par",
        ]
        self.sub_set_action(rtypes, "sync_revert")

    def sync_resume(self):
        rtypes = [
            "sync.netapp",
            "sync.symsrdfs",
            "sync.hp3par",
            "sync.nexenta",
        ]
        self.sub_set_action(rtypes, "sync_resume")

    def sync_quiesce(self):
        rtypes = [
            "sync.netapp",
            "sync.nexenta",
        ]
        self.sub_set_action(rtypes, "sync_quiesce")

    def resync(self):
        self.stop()
        self.sync_resync()
        self.start()

    def sync_resync(self):
        rtypes = [
            "sync.netapp",
            "sync.nexenta",
            "sync.radossnap",
            "sync.radosclone",
            "sync.dds",
            "sync.symclone",
            "sync.symsnap",
            "sync.ibmdssnap",
            "sync.evasnap",
            "sync.necismsnap",
            "disk.md",
        ]
        self.sub_set_action(rtypes, "sync_resync")

    def sync_break(self):
        rtypes = [
            "sync.netapp",
            "sync.nexenta",
            "sync.hp3par",
            "sync.symclone",
            "sync.symsnap",
        ]
        self.sub_set_action(rtypes, "sync_break")

    def sync_update(self):
        rtypes = [
            "sync.netapp",
            "sync.nexenta",
            "sync.hp3par",
            "sync.hp3parsnap",
            "sync.dds",
            "sync.btrfssnap",
            "sync.zfs",
            "sync.zfssnap",
            "sync.s3",
            "sync.symclone",
            "sync.symsnap",
            "sync.ibmdssnap",
        ]
        self.sub_set_action(rtypes, "sync_update")

    def sync_full(self):
        rtypes = [
            "sync.dds",
            "sync.zfs",
            "sync.btrfs",
            "sync.s3",
        ]
        self.sub_set_action(rtypes, "sync_full")

    def sync_restore(self):
        rtypes = [
            "sync.s3",
            "sync.symclone",
            "sync.symsnap",
        ]
        self.sub_set_action(rtypes, "sync_restore")

    def sync_split(self):
        rtypes = [
            "sync.symsrdfs",
        ]
        self.sub_set_action(rtypes, "sync_split")

    def sync_establish(self):
        rtypes = [
            "sync.symsrdfs",
        ]
        self.sub_set_action(rtypes, "sync_establish")

    def sync_verify(self):
        rtypes = [
            "sync.dds",
        ]
        self.sub_set_action(rtypes, "sync_verify")

    def can_sync(self, rtypes=None, target=None):
        """
        Return True if any resource of type in <rtypes> yields it can sync.
        """
        if rtypes is None:
            rtypes = []
        ret = False
        for rtype in rtypes:
            for resource in self.get_resources(rtype):
                try:
                    ret |= resource.can_sync(target)
                    self.log.debug("resource %s can sync: %s" % (resource.rid, str(ret)))
                except ex.Error as exc:
                    return False
                if ret:
                    return True
        self.log.debug("nothing to sync for the service for now")
        return False

    def sync_all(self):
        """
        The 'sync all' action entrypoint.
        """
        if not self.can_sync(["sync"]):
            return
        self.sync_update()
        self.presync()
        rtypes = [
            "sync.rsync",
            "sync.btrfs",
            "sync.docker",
            "sync.dds",
        ]
        self.sub_set_action(rtypes, "sync_all")

    def service_status(self):
        """
        The 'service_status' scheduler task and action entrypoint.

        This method returns early if called from an encapsulated agent, as
        the master agent is responsible for pushing the encapsulated
        status.
        """
        if self.encap:
            if not self.options.cron:
                self.log.info("push service status is disabled for encapsulated services")
            return
        self.print_status_data(mon_data=False, refresh=True)

    def resinfo(self):
        """
        Return a list of (key, val) for each service resource and
        env section.
        """
        data = {}
        for res in self.get_resources():
            try:
                _data = res.info()
            except Exception as exc:
                _data = []
                import traceback
                traceback.print_exc()
            for __data in _data:
                rid = __data[-3]
                if rid not in data:
                    data[rid] = []
                data[rid].append(__data)
        if "env" not in self.cd:
            return data
        for key in self.cd["env"]:
            try:
                val = self.conf_get("env", key)
            except ex.OptNotFound as exc:
                continue
            if "env" not in data:
                data["env"] = []
            data["env"].append([
                self.path,
                Env.nodename,
                self.topology,
                "env",
                key if key is not None else "",
                val if val is not None else "",
            ])
        return data

    def print_resinfo(self):
        if self.options.format is None and not self.options.jsonpath_filter:
            self.print_resinfo_tree()
            return
        data = [[
            'res_svcname',
            'res_nodename',
            'topology',
            'rid',
            'res_key',
            'res_value',
        ]]
        for _data in self.resinfo().values():
            data += _data
        return data

    def print_resinfo_tree(self):
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        node1 = tree.add_node()
        node1.add_column(self.path, color.BOLD)
        data = self.resinfo()
        for rid in sorted(data.keys()):
            _data = data[rid]
            node = node1.add_node()
            text = "%s" % rid
            node.add_column(text, color.BROWN)
            for __data in _data:
                catnode = node.add_node()
                catnode.add_column(__data[-2], color.LIGHTBLUE)
                catnode.add_column(__data[-1])
        tree.out()

    def push_status(self):
        """
        Push the service instance status to the collector synchronously.
        Usually done asynchronously and automatically by the collector thread.
        """
        self.node.collector.call('push_status', self.path, self.print_status_data(mon_data=False, refresh=True))

    def push_config(self):
        """
        Push the service config to the collector. Usually done
        automatically by the collector thread.
        """
        self.node.collector.call('push_config', self)

    def push_resinfo(self):
        """
        The 'push_resinfo' scheduler task and action entrypoint.
        Push the per-resource key/value pairs to the collector.
        """
        return self._push_resinfo(sync=self.options.syncrpc)

    def _push_resinfo(self, sync=False):
        """
        The 'push_resinfo' scheduler task and action entrypoint.
        Push the per-resource key/value pairs to the collector.
        """
        data = []
        for _data in self.resinfo().values():
            data += _data
        self.node.collector.call('push_resinfo', data, sync=sync)

    def push_encap_config(self):
        """
        Synchronize the configuration file between encap and master agent,
        This action is skipped when run by an encapsulated agent.

        Verify the service has an encapsulated part, and if so, for each
        container in up state running an encapsulated part, synchronize the
        service configuration file.
        """
        self.init_resources()
        if self.encap or not self.has_encap_resources:
            return

        for resource in self.get_resources('container'):
            if resource.status(ignore_nostatus=True) not in (core.status.STDBY_UP, core.status.UP):
                continue
            self._push_encap_config(resource)

    def _push_encap_config(self, container):
        if len(self.logger.handlers) > 1:
            self.logger.handlers[1].setLevel(logging.CRITICAL)
        try:
            self.__push_encap_config(container)
        finally:
            if len(self.logger.handlers) > 1:
                self.logger.handlers[1].setLevel(Env.loglevel)

    def __push_encap_config(self, container):
        """
        Compare last modification time of the master and slave service
        configuration file, and copy the most recent version over the least
        recent.
        """
        def pulled_config_sanity_check(cf):
            """
            If the encap cf is flushed at the time of the copy,
            avoid installing it, and hope the new version will
            arrive later.
            """
            if os.path.getsize(cf) <= 60:
                raise ex.Error("pulled an empty configuration from %s. "
                               "abort install." % container.name)

        def encap_config_mtime():
            cmd = ['print', 'config', 'mtime']
            try:
                cmd_results = self._encap_cmd(cmd, container, push_config=False, fwd_options=False)
                out = cmd_results[0]
            except ex.Error as exc:
                return

            if out == "":
                # this is what happens when the container is down
                raise ex.EncapUnjoinable

            try:
                return int(float(out.strip()))
            except Exception:
                return

        def pull_encap_config():
            paths = Paths(osvc_root_path=container.osvc_root_path)
            encap_cf = os.path.join(paths.pathetc, self.paths.cf[len(Env.paths.pathetc)+1:])
            tmpfile = tempfile.NamedTemporaryFile(delete=False,
                                                  dir=os.path.dirname(self.paths.cf),
                                                  prefix="." + self.name + ".conf.")
            tmpcf = tmpfile.name
            tmpfile.close()
            try:
                if hasattr(container, 'rcp_from'):
                    cmd_results = container.rcp_from(encap_cf, tmpcf)
                else:
                    cmd = Env.rcp.split() + [container.name+':'+encap_cf, tmpcf]
                    cmd_results = justcall(cmd)
                self.log.info("fetch %s from %s", encap_cf, container.name)
                if cmd_results[2] != 0:
                    raise ex.Error()
                pulled_config_sanity_check(tmpcf)
                os.utime(tmpcf, (encap_mtime, encap_mtime))
                shutil.move(tmpcf, self.paths.cf)
                return
            finally:
                try:
                    os.unlink(tmpcf)
                except Exception:
                    pass

        def push_encap_config():
            """
            Use a tempory conf staging to not have to care about ns dir create
            """
            paths = Paths(osvc_root_path=container.osvc_root_path)
            encap_cf = os.path.join(paths.pathtmp, self.id+".conf")
            if hasattr(container, 'rcp'):
                cmd_results = container.rcp(self.paths.cf, encap_cf)
            else:
                cmd = Env.rcp.split() + [self.paths.cf, container.name+':'+encap_cf]
                cmd_results = justcall(cmd)
            if cmd_results[2] != 0:
                raise ex.Error("failed to send %s to %s" % (self.paths.cf, container.name))
            self.log.info("send %s to %s", self.paths.cf, container.name)

            cmd = ["create", "--restore", "--config", encap_cf]
            try:
                cmd_results = self._encap_cmd(cmd, container=container,
                                              push_config=False, fwd_options=False)
            except ex.Error:
                raise ex.Error("failed to create %s slave service" % container.name)
            self.log.info("create %s slave service", container.name)

        try:
            encap_mtime = encap_config_mtime()
        except ex.EncapUnjoinable:
            return

        local_mtime = os.path.getmtime(self.paths.cf)
        if encap_mtime == local_mtime:
            return

        if encap_mtime and encap_mtime > local_mtime:
            pull_encap_config()
            return

        push_encap_config()

    @staticmethod
    def _tag_match(rtags, keeptags):
        """
        Return True if any tag of <rtags> is in <keeptags>.
        """
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def set_skip_resources(self, keeprid=None, xtags=None):
        """
        Set the 'skip' flag of all resources.
        * set to False if keeprid is empty and xtags is empty
        * set to False if rid is in keeprid and not in xtags
        * else set to True
        """
        if keeprid is None:
            keeprid = []

        if xtags is None:
            xtags = set()

        ridfilter = len(keeprid) > 0
        tagsfilter = len(xtags) > 0

        if not tagsfilter and not ridfilter:
            return

        for resource in self.get_resources():
            if self._tag_match(resource.tags, xtags):
                resource.skip = True
            if ridfilter and resource.rid in keeprid:
                continue
            resource.skip = True

    def all_rids(self):
        self.init_resources()
        return [rid for rid in self.resources_by_id if rid is not None] + list(self.encap_resources.keys())

    def expand_rid(self, rid):
        """
        Given a rid return a set containing either the rid itself if it is
        a known rid, or containing the rid of all resources whose prefix
        matches the name given as rid.
        """
        retained_rids = set()
        for _rid in self.all_rids():
            if '#' in rid:
                if _rid == rid:
                    retained_rids.add(_rid)
                else:
                    continue
            elif '.' in rid:
                if _rid in self.resources_by_id and rid == self.resources_by_id[_rid].type:
                    retained_rids.add(_rid)
                elif _rid in self.encap_resources and rid == self.encap_resources[_rid].type:
                    retained_rids.add(_rid)
            elif _rid[:_rid.index('#')] == rid:
                retained_rids.add(_rid)
        return retained_rids

    def expand_rids(self, rids):
        """
        Parse the --rid value and return the retained corresponding resource
        ids.
        Filter out non existing resource ids.
        If a rid has no "#", expand to the set of rids of resources whose
        prefix matches the name given as a rid.

        Example:
        --rid disk: return all rids of disk resources.
        --rid disk#0: return disk#0 if such a resource exists
        """
        if len(rids) == 0:
            return
        retained_rids = set()
        for rid in set(rids):
            retained_rids |= self.expand_rid(rid)
        if len(retained_rids) > 0:
            self.log.debug("rids added from --rid %s: %s", ",".join(rids),
                           ",".join(retained_rids))
        return retained_rids

    def expand_subsets(self, subsets):
        """
        Parse the --subsets value and return the retained corresponding resource
        ids.
        """
        if subsets is None or self.options.subsets is None:
            return
        retained_rids = set()
        for resource in itertools.chain(self.resources_by_id.values(), self.encap_resources.values()):
            if resource.subset in subsets:
                retained_rids.add(resource.rid)
        if len(retained_rids) > 0:
            self.log.debug("rids added from --subsets %s: %s",
                           ",".join(subsets), ",".join(retained_rids))
        return retained_rids

    def expand_tags(self, tags):
        """
        Parse the --tags value and return the retained corresponding resource
        ids.
        ',' is interpreted as OR
        '+' is interpreted as AND
        '+' are evaluated before ','

        Example:
        --tags A,B : return rids of resource with either tag A or B
        --tags A+B : return rids of resource with both tags A and B
        --tags A+B,B+C : return rids of resource with either tags A and B
                         or tags B and C
        """
        if len(tags) == 0 or tags is None:
            return
        retained_rids = set()
        unions = []
        intersection = []
        for idx, tag in enumerate(tags):
            if tag[0] == "+":
                tag = tag[1:]
                intersection.append(tag)
                if idx == len(tags) - 1:
                    unions.append(intersection)
            else:
                if len(intersection) > 0:
                    # new intersection, store the current
                    unions.append(intersection)
                # open a new intersection
                intersection = [tag]
                if idx == len(tags) - 1:
                    unions.append(intersection)

        for intersection in unions:
            for resource in itertools.chain(self.resources_by_id.values(), self.encap_resources.values()):
                if set(intersection) & resource.tags == set(intersection):
                    retained_rids.add(resource.rid)
        if len(retained_rids) > 0:
            self.log.debug("rids added from --tags %s: %s", ",".join(tags),
                           ",".join(retained_rids))
        return retained_rids

    def standby_resources(self):
        """
        Return the list of resources flagged always on on this node
        """
        return [resource for resource in self.resources_by_id.values()
                if resource.is_standby]

    def options_to_rids(self, options, action):
        """
        Return the list of rids to apply an action to, from the command
        line options passed as <options>.
        """
        rid = options.get("rid", None)
        tags = options.get("tags", None)
        subsets = options.get("subsets", None)
        xtags = options.get("xtags", None)

        if rid or tags or subsets or xtags:
            self.init_resources()

        if rid is None:
            rid = []
        elif is_string(rid):
            if rid:
                rid = rid.split(',')
            else:
                rid = []

        if tags is None:
            tags = []
        elif is_string(tags):
            tags = tags.replace("+", ",+").split(',')

        if subsets is None:
            subsets = []
        elif is_string(subsets):
            subsets = subsets.split(',')

        if xtags is None:
            xtags = set()
        elif is_string(xtags):
            xtags = xtags.split(',')

        if len(self.resources_by_id.keys()) > 0:
            rids = set(self.all_rids())
            unsupported_rids = set(rid) - rids

            # --rid
            retained_rids = self.expand_rids(rid)
            if retained_rids is not None:
                rids &= retained_rids

            # --subsets
            retained_rids = self.expand_subsets(subsets)
            if retained_rids is not None:
                rids &= retained_rids

            # --tags
            retained_rids = self.expand_tags(tags)
            if retained_rids is not None:
                rids &= retained_rids

            # for delete, retain rids not in the built resources
            # (resources no longer supported)
            if action == "delete":
                rids |= unsupported_rids

            rids = list(rids)
            self.log.debug("rids retained after expansions intersection: %s",
                           ",".join(rids))

            if self.command_is_scoped(options) and len(rids) == 0:
                raise ex.AbortAction("no resource match the given --rid, --subset "
                                     "and --tags specifiers")
        else:
            # let the action go on. 'delete', for one, takes a --rid but does
            # not need resource initialization
            rids = rid

        return rids

    def restart(self):
        """
        The 'restart' action entrypoint.
        This action translates into 'stop' followed by 'start'
        """
        self.options.local = True
        self.stop()
        self.log.info("instance stopped, ready for restart.")
        self.unset_all_lazy()
        self.start()

    def _migrate(self):
        """
        Call the migrate action on all relevant resources.
        """
        rtypes = [
            "container.ovm",
            "container.hpvm",
            "container.esx",
            "container.lxd",
        ]
        self.sub_set_action(rtypes, "_migrate")

    def destination_node_sanity_checks(self, destination_node=None):
        """
        Raise an Error if
        * the destination node --to arg not set
        * the specified destination is the current node
        * the specified destination is not a service candidate node

        If the destination node is not specified and the cluster has
        only 2 nodes, consider the destination node is our peer.

        Return the validated destination node name.
        """
        if destination_node is None:
            destination_node = self.options.to
        if destination_node is None:
            destination_node = "<peer>"
        return destination_node

    @_master_action
    def migrate(self):
        """
        Service online migration.
        """
        dst = self.destination_node_sanity_checks()
        self.svcunlock()
        self._clear(server=Env.nodename)
        self._clear(server=dst)
        self.daemon_mon_action("freeze", wait=True)
        src_node = self.current_node()
        self.daemon_service_action(action="prstop", server=src_node)
        try:
            self.daemon_service_action(action="startfs", options={"master": True}, server=dst)
            self._migrate()
        except:
            if len(self.get_resources('disk.scsireserv')) > 0:
                self.log.error("scsi reservations were dropped. you have to "
                               "acquire them now using the 'prstart' action "
                               "either on source node or destination node, "
                               "depending on your problem analysis.")
            raise
        self.daemon_service_action(action="stop", server=src_node)
        self.daemon_service_action(action="prstart", options={"master": True}, server=dst)

    def takeover(self):
        """
        Orchestrated move of a failover running instance to the local node.
        """
        pass

    def giveback(self):
        """
        Optimize service placement.
        """
        pass

    def scale(self):
        """
        Set the scale keyword.
        """
        if self.scale_target is None:
            raise ex.Error("can't scale: not a scaler")
        try:
            value = int(self.options.to)
            assert value >= 0
        except Exception:
            raise ex.Error("invalid scale target: set '--to <n>' where n>=0")
        self._set("DEFAULT", "scale", str(value), validation=False)
        self.set_service_monitor()

    def move(self):
        """
        Orchestrated move of running instances to the nodes specified by --to.
        """
        pass

    def switch(self):
        """
        The "switch" Monitor Action implementation.
        """
        self.stop()
        self.set_service_monitor(status="start failed", global_expect="placed")

    def set_disable(self, rids=None, disable=True):
        """
        Set the disable to <disable> (True|False) in the configuration file,
        * at DEFAULT level if no resources were specified
        * in each resource section if resources were specified
        """
        if rids is None:
            rids = []

        if not self.command_is_scoped() and \
           (len(rids) == 0 or len(rids) == len(self.resources_by_id)):
            rids = ['DEFAULT']

        changes = []
        unsets = []
        for rid in rids:
            if disable:
                self.log.info("set %s.disable = true", rid)
                changes.append("%s.disable=true" % rid)
            elif "disable" in self.cd[rid]:
                self.log.info("remove %s.disable", rid)
                unsets.append("%s.disable" % rid)

            #
            # if we set <section>.disable = <bool>,
            # remove <section>.disable@<scope> = <not bool>
            #
            if rid not in self.cd:
                items = {}
            else:
                items = self.cd[rid]
            for option, value in items.items():
                if not option.startswith("disable@"):
                    continue
                if value is True:
                    continue
                self.log.info("remove %s.%s = false", rid, option)
                unsets.append("DEFAULT.%s" % option)

        self.unset_lazy("disabled")

        # update the resource objects disable prop
        for rid in rids:
            if rid == "DEFAULT":
                continue
            resource = self.get_resource(rid)
            if resource is None:
                continue
            if disable:
                resource.disable()
            else:
                resource.enable()

        if changes:
            self.set_multi(changes)
        if unsets:
            self.unset_multi(unsets)

    def enable(self):
        """
        The 'enable' action entrypoint.
        """
        return self.set_disable(self.action_rid, False)

    def disable(self):
        """
        The 'disable' action entrypoint.
        """
        return self.set_disable(self.action_rid, True)

    def delete_resources(self, rids=None):
        """
        Delete service resources objects and references and in configuration file
        """
        if rids is None:
            rids = self.action_rid
        self._delete_resources_config(rids)
        self._delete_resources_live(rids)

    def _delete_resources_live(self, rids):
        """
        Delete service resources objects and references
        """
        for rid in rids:
            self._delete_resource_live(rid)

    def _delete_resource_live(self, rid):
        """
        Delete service a resource object and references
        """
        if rid in self.resources_by_id:
            del(self.resources_by_id[rid])
        if rid in self.encap_resources:
            del(self.encap_resources[rid])
        rs_to_delete = []
        for rset_id, rset in self.resourcesets_by_id.items():
            to_delete = []
            for idx, res in enumerate(rset.resources):
                if res.rid == rid:
                    res.remove_is_provisioned_flag()
                    to_delete.append(idx)
            for idx in to_delete:
                del rset.resources[idx]
            if len(rset.resources) == 0:
                rs_to_delete.append(rset_id)
        for rset_id in rs_to_delete:
            del self.resourcesets_by_id[rset_id]

    def _delete_resources_config(self, rids):
        """
        Delete service resources from its configuration file
        """
        self.delete_sections(rids)

    def delete(self):
        """
        The 'delete' action entrypoint.
        If --unprovision is set, call the unprovision method.
        Then if no resource specifier is set, remove all service files in
        <pathetc>.
        If a resource specifier is set, only delete the corresponding
        sections in the configuration file.
        """
        if self.options.unprovision:
            self.unprovision()

        if not self.command_is_scoped():
            if os.environ.get("OSVC_ACTION_ORIGIN") != "daemon":
                # the daemon only delete the whole service, so no
                # need to remove this node from the nodes list of
                # remote instances
                if Env.nodename in self.nodes:
                    self.set_multi([
                       "nodes="+Env.nodename,
                       "drpnodes=",
                    ])
                elif Env.nodename in self.drpnodes:
                    self.set_multi([
                       "drpnodes="+Env.nodename,
                       "nodes=",
                    ])
                self.svcunlock()
                for peer in self.peers:
                    if peer == Env.nodename:
                        continue
                    self.daemon_service_action(
                        action="set",
                        options={
                            "kw": [
                                "nodes-=" + Env.nodename,
                                "drpnodes-=" + Env.nodename,
                            ],
                            "eval": True
                        },
                        server=peer,
                        sync=True
                    )
            self.delete_service_conf()
            self.delete_service_logs()
            self.set_purge_collector_tag()
        else:
            self.delete_resources()

    def enter(self):
        if not self.options.rid:
            resources = self.get_resources("container")
            if len(resources) == 1:
                rid = resources[0].rid
            else:
                raise ex.Error("this svc has multiple containers. select one with --rid <id>")
        elif is_string(self.options.rid):
            rid = self.options.rid
        else:
            rid = self.options.rid[0]
        self._enter(rid)

    def _enter(self, rid):
        res = self.get_resource(rid)
        if res is None:
            raise ex.Error("rid %s not found" % rid)
        if not hasattr(res, "enter"):
            raise ex.Error("rid %s does not support enter" % rid)
        res.enter()

    def docker(self):
        self.container_manager_passthrough("docker")

    def podman(self):
        self.container_manager_passthrough("podman")

    def oci(self):
        self.container_manager_passthrough("oci")

    def container_manager_passthrough(self, ctype):
        """
        The 'docker|podman|oci' action entry point.
        Parse the docker argv and substitute known patterns before relaying
        the argv to the docker command.
        Set the socket to point the service-private docker daemon if
        the service has such a daemon.
        """
        import subprocess
        if ctype == "oci":
            containers = self.get_resources(["container.docker", "container.podman"])
        else:
            containers = self.get_resources("container." + ctype)
        if self.options.extra_argv is None:
            print("no docker command arguments supplied", file=sys.stderr)
            return 1

        def subst(argv):
            """
            Parse the docker argv and substitute known patterns.
            """
            import re
            for idx, arg in enumerate(argv):
                if re.match(r'[%{][#.-_\w]+[%\}]', arg):
                    container_rid = arg.strip("{}%")
                    if not container_rid.startswith("container#"):
                        container_rid = "container#" + container_rid
                    if container_rid not in self.resources_by_id:
                        continue
                    container = self.resources_by_id[container_rid]
                    name = container.container_name
                    del argv[idx]
                    argv.insert(idx, name)
            for idx, arg in enumerate(argv):
                if arg in ("%instances%", "{instances}"):
                    del argv[idx]
                    instances = [resource.container_name for resource in containers
                                 if not resource.skip and not resource.is_disabled()]
                    for instance in instances:
                        argv.insert(idx, instance)
            for idx, arg in enumerate(argv):
                if arg in ("%images%", "{images}"):
                    del argv[idx]
                    images = list(set([resource.image for resource in containers
                                       if not resource.skip and not resource.is_disabled()]))
                    for image in images:
                        argv.insert(idx, image)
            for idx, arg in enumerate(argv):
                if arg in ("%as_service%", "{as_service}"):
                    container = containers[0]
                    del argv[idx]
                    argv[idx:idx] = container.lib.login_as_service_args()
            return argv

        if len(containers) == 0:
            print("this service has no %s resource" % ctype, file=sys.stderr)
            return 1

        if containers[0].type == "docker":
            containers[0].lib.docker_start(verbose=False)
        cmd = containers[0].lib.docker_cmd + subst(self.options.extra_argv)
        proc = subprocess.Popen(cmd)
        proc.communicate()
        return proc.returncode

    def freezestop(self):
        """
        The freezestop monitor action.
        """
        self.master_freeze()
        self.slave_freezestop()
        self.master_stop()

    def freeze(self):
        """
        Set the frozen flag.
        """
        self.master_freeze()
        self.slave_freeze()

    @_master_action
    def master_freeze(self):
        self.freezer.freeze()

    @_slave_action
    def slave_freeze(self):
        self.encap_cmd(['freeze'], verbose=True)

    def thaw(self):
        """
        Unset the frozen flag.
        """
        self.master_thaw()
        self.slave_thaw()

    @_master_action
    def master_thaw(self):
        self.freezer.thaw()

    @_slave_action
    def slave_thaw(self):
        self.encap_cmd(['thaw'], verbose=True)

    def frozen(self):
        """
        Return True if the service is frozen.
        """
        return self.freezer.frozen(strict=True)

    def pull(self):
        """
        Pull a service configuration from the collector and install it.
        """
        data = self.node.collector_rest_get("/services/"+self.path+"?props=svc_config&meta=0")
        if "error" in data:
            raise ex.Error(data["error"])
        if len(data["data"]) == 0:
            raise ex.Error("service not found on the collector")
        if data["data"][0]["svc_config"] is None:
            raise ex.Error("service has an empty configuration on the collector")
        if len(data["data"][0]["svc_config"]) == 0:
            raise ex.Error("service has an empty configuration on the collector")
        buff = data["data"][0]["svc_config"].replace("\\n", "\n").replace("\\t", "\t")
        import codecs
        with codecs.open(self.paths.cf, "w", "utf8") as ofile:
            ofile.write(buff)
            ofile.flush()
        self.log.info("%s pulled", self.paths.cf)

        if self.options.provision:
            self.action("provision")

    @lazy
    def slave_num(self):
        try:
            return int(self.name.split(".")[0])
        except ValueError:
            return 0

    def snooze(self):
        """
        Snooze notifications on the service.
        """
        if self.options.duration is None:
            print("set --duration", file=sys.stderr)
            raise ex.Error
        data = self._snooze(self.options.duration)
        print(data.get("info", ""))

    def unsnooze(self):
        data = self._unsnooze()
        print(data.get("info", ""))

    def _snooze(self, duration):
        """
        Snooze notifications on the service.
        """
        try:
            data = self.collector_rest_post("/services/self/snooze", {
                "duration": self.options.duration,
            })
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])
        return data

    def _unsnooze(self):
        """
        Unsnooze notifications on the service.
        """
        try:
            data = self.collector_rest_post("/services/self/snooze")
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])
        return data

    def mount_point(self):
        """
        Return the shortest service mount point.
        The volume resource in the consumer service uses this function as the
        prefix of its own mount_point property.
        """
        candidates = [res for res in self.get_resources("fs")]
        if not candidates:
            return
        for candidate in sorted(candidates):
            if not hasattr(candidate, "mount_point"):
                continue
            return candidate.mount_point
        raise IndexError

    def device(self):
        """
        Return the unambiguous exposed device. Volume services naturally
        have such a device.
        """
        candidates = sorted([res for res in self.get_resources("disk")
                             if res.type != "disk.scsireserv"], key=lambda r: r.rid)
        if not candidates:
            return
        try:
            return list(candidates[-1].exposed_devs())[0]
        except Exception:
            return

    def get_volume(self, name):
        """
        Return the volume resource matching name.
        Raise Error if not found or found more than one matching resource.
        """
        candidates = [res for res in self.get_resources("volume") if res.name == name]
        if not candidates:
            raise ex.Error("volume %s not found" % name)
        if not candidates or len(candidates) > 1:
            raise ex.Error("found multiple volumes names" % name)
        return candidates[0]

    def get_volume_rid(self, volname):
        candidates = [rid for rid, res in self.resources_by_id.items()
                      if rid.startswith("volume#") and res.name == volname]
        try:
            return candidates[0]
        except IndexError:
            return

    def replace_volname(self, buff, mode="file", strict=False, errors=None):
        """
        In a string starting with a volume name, replace the volume name with,
        * the volume mount point path if mode=="file"
        * the volume device path if mode=="blk"

        If strict is True, raise if the string does not start with a volume
        name (starts with / actually).

        If errors is "ignore", ignore all errors:
        * string does not start with a volume name (even if strict is True)
        * volume does not have a mount point
        * volume instance is down
        """
        l = buff.split("/")
        volname = l[0]
        if not volname:
            if strict and errors != "ignore":
                raise ex.Error("a volume path can't start with /")
            else:
                return buff, None
        vol = self.get_volume(volname)
        if mode == "file" and vol.mount_point is None:
            if errors == "ignore":
                return buff, None
            raise ex.Error("referenced volume %s has no mount point" % l[0])
        volstatus = vol.status()
        if volstatus not in (core.status.UP, core.status.STDBY_UP, core.status.NA):
            if errors != "ignore":
                raise ex.Error("volume %s is %s" % (volname, core.status.Status(volstatus)))
        if mode == "blk":
            if vol.device is None:
                return None, vol
            l[0] = vol.device
        else:
            if vol.mount_point is None:
                return None, vol
            l[0] = vol.mount_point
        return "/".join(l), vol

    def install_data(self):
        rtypes = [
            "volume",
        ]
        self.sub_set_action(rtypes, "install_data")
