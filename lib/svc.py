"""
The module defining the Svc class.
"""
from __future__ import print_function

import sys
import os
import signal
import logging
import datetime
import lock

from resources import Resource
from resourceset import ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv, get_osvc_paths, Storage
from rcUtilities import justcall, lazy, vcall
from rcConfigParser import RawConfigParser
from svcBuilder import conf_get_string_scope, conf_get_boolean_scope, get_pg_settings
import rcExceptions as ex
import rcLogger
import node
from rcScheduler import scheduler_fork, Scheduler, SchedOpts

if sys.version_info[0] < 3:
    BrokenPipeError = IOError

def signal_handler(*args):
    """
    A signal handler raising the excSignal exception.
    Args can be signum and frame, but we don't use them.
    """
    raise ex.excSignal

DEFAULT_STATUS_GROUPS = [
    "container",
    "ip",
    "disk",
    "fs",
    "share",
    "sync",
    "app",
    "hb",
    "stonith",
]

CONFIG_DEFAULTS = {
    'push_schedule': '00:00-06:00@361',
    'sync_schedule': '04:00-06:00@121',
    'comp_schedule': '00:00-06:00@361',
    'status_schedule': '@9',
    'monitor_schedule': '@1',
    'resinfo_schedule': '@60',
    'no_schedule': '',
}

ACTIONS_TRANSLATIONS = {
    "push_env_mtime": "push_config_mtime",
    "push_env": "push_config",
    "json_env": "json_config",
    "syncall": "sync_all",
    "syncbreak": "sync_break",
    "syncdrp": "sync_drp",
    "syncestablish": "sync_establish",
    "syncfullsync": "sync_full",
    "syncnodes": "sync_nodes",
    "syncquiesce": "sync_quiesce",
    "syncrestore": "sync_restore",
    "syncresume": "sync_resume",
    "syncresync": "sync_resync",
    "syncrevert": "sync_revert",
    "syncsplit": "sync_split",
    "syncupdate": "sync_update",
    "syncverify": "sync_verify",
}

ACTIONS_ALLOW_ON_FROZEN = [
    "autopush",
    "delete",
    "disable",
    "edit_config",
    "enable",
    "freeze",
    "frozen",
    "get",
    "json_config",
    "json_status",
    "json_disklist",
    "json_devlist",
    "logs",
    "print_config",
    "print_devlist",
    "print_disklist",
    "print_config_mtime",
    "print_resource_status",
    "print_schedule",
    "print_status",
    "push",
    "push_resinfo",
    "push_config",
    "push_service_status",
    "prstatus",
    "scheduler",
    "set",
    "status",
    "thaw",
    "update",
    "unset",
    "validate_config",
]

ACTIONS_ALLOW_ON_CLUSTER = ACTIONS_ALLOW_ON_FROZEN + [
    "boot",
    "docker",
    "dns_update",
    "postsync",
    "presync",
    "resource_monitor",
    "startstandby",
    "sync_all",
    "sync_drp",
    "sync_nodes",
    "toc",
    "validate_config",
]

ACTIONS_NO_LOG = [
    "delete",
    "edit_config",
    "get",
    "group_status",
    "logs",
    "push",
    "push_resinfo",
    "push_config",
    "push_service_status",
    "resource_monitor",
    "scheduler",
    "set",
    "status",
    "unset",
    "validate_config",
]

ACTIONS_NO_TRIGGER = [
    "delete",
    "dns_update",
    "enable",
    "disable",
    "status",
    "scheduler",
    "pg_freeze",
    "pg_thaw",
    "pg_kill",
    "logs",
    "edit_config",
    "push_resinfo",
    "push",
    "group_status",
    "presync",
    "postsync",
    "freezestop",
    "resource_monitor",
]

ACTIONS_NO_LOCK = [
    "docker",
    "edit_config",
    "freeze",
    "freezestop",
    "frozen",
    "get",
    "logs",
    "push",
    "push_resinfo",
    "push_config",
    "push_service_status",
    "run",
    "scheduler",
    "status",
    "thaw",
    "toc",
    "validate_config",
]

DISK_TYPES = [
    "disk.drbd",
    "disk.gandi",
    "disk.gce",
    "disk.lock",
    "disk.loop",
    "disk.md",
    "disk.rados",
    "disk.raw",
    "disk.vg",
    "disk.zpool",
]

STATUS_TYPES = [
    "app",
    "container.amazon",
    "container.docker",
    "container.esx",
    "container.hpvm",
    "container.jail",
    "container.kvm",
    "container.lxc",
    "container.ldom",
    "container.openstack",
    "container.ovm",
    "container.srp",
    "container.vbox",
    "container.vcloud",
    "container.vz",
    "container.xen",
    "container.zone",
    "disk.drbd",
    "disk.gandi",
    "disk.gce",
    "disk.lock",
    "disk.loop",
    "disk.md",
    "disk.lv",
    "disk.raw",
    "disk.rados",
    "disk.scsireserv",
    "disk.vg",
    "disk.zpool",
    "fs",
    "hb.linuxha",
    "hb.openha",
    "hb.ovm",
    "hb.rhcs",
    "hb.sg",
    "hb.vcs",
    "ip",
    "share.nfs",
    "sync.btrfs",
    "sync.btrfssnap",
    "sync.dcsckpt",
    "sync.dcssnap",
    "sync.dds",
    "sync.docker",
    "sync.evasnap",
    "sync.hp3par",
    "sync.hp3parsnap",
    "sync.ibmdssnap",
    "sync.necismsnap",
    "sync.netapp",
    "sync.nexenta",
    "sync.rados",
    "sync.rsync",
    "sync.symclone",
    "sync.symsnap",
    "sync.symsrdfs",
    "sync.s3",
    "sync.zfs",
    "stonith.callout",
    "stonith.ilo",
]

ACTIONS_DO_MASTER_AND_SLAVE = [
    "boot",
    "migrate",
    "prstart",
    "prstop",
    "restart",
    "shutdown",
    "start",
    "startstandby",
    "stop",
    "stopstandby",
    "switch",
]

ACTIONS_NEED_SNAP_TRIGGER = [
    "sync_drp",
    "sync_nodes",
    "sync_resync",
    "sync_update",
]

os.environ['LANG'] = 'C'

def _slave_action(func):
    def need_specifier(self):
        """
        Raise an exception if --master or --slave(s) need to be set
        """
        if self.command_is_scoped():
            return
        if self.running_action in ACTIONS_DO_MASTER_AND_SLAVE:
            return
        if self.options.master or self.options.slaves or self.options.slave is not None:
            return
        raise ex.excError("specify either --master, --slave(s) or both (%s)" % func.__name__)

    def _func(self):
        if self.encap or not self.has_encap_resources:
            return
        need_specifier(self)
        if self.options.slaves or \
           self.options.slave is not None or \
           (not self.options.master and not self.options.slaves and self.options.slave is None):
            try:
                func(self)
            except Exception as exc:
                raise ex.excError(str(exc))
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
        if self.running_action in ACTIONS_DO_MASTER_AND_SLAVE:
            return
        if self.options.master or self.options.slaves or self.options.slave is not None:
            return
        raise ex.excError("specify either --master, --slave(s) or both (%s)" % func.__name__)

    def _func(self):
        need_specifier(self)
        if self.options.master or \
           (not self.options.master and not self.options.slaves and self.options.slave is None):
            func(self)
    return _func

class Svc(Scheduler):
    """
    A OpenSVC service class.
    A service is a collection of resources.
    It exposes operations methods like provision, unprovision, stop, start,
    and sync.
    """

    def __init__(self, svcname=None):
        self.type = "hosted"
        self.svcname = svcname
        self.hostid = rcEnv.nodename
        self.paths = Storage(
            cf=os.path.join(rcEnv.pathetc, self.svcname+'.conf'),
            push_flag=os.path.join(rcEnv.pathvar, self.svcname, 'last_pushed_config'),
            run_flag=os.path.join(os.sep, "var", "run", "opensvc."+self.svcname),
        )
        self.resources_by_id = {}
        self.resourcesets = []
        self.resourcesets_by_type = {}
        self.disks = set()
        self.devs = set()

        self.ref_cache = {}
        self.encap_json_status_cache = {}
        self.rset_status_cache = None
        self.lockfd = None
        self.group_status_cache = None
        self.abort_start_done = False
        self.action_start_date = datetime.datetime.now()
        self.ha = False
        self.has_encap_resources = False
        self.encap = False
        self.action_rid = []
        self.running_action = None
        self.config = None
        self.need_postsync = set()

        # set by the builder
        self.node = None
        self.clustertype = "failover"
        self.show_disabled = False
        self.svc_env = rcEnv.node_env
        self.nodes = set()
        self.drpnodes = set()
        self.drpnode = ""
        self.encapnodes = set()
        self.flex_primary = ""
        self.drp_flex_primary = ""
        self.sync_dblogger = False
        self.create_pg = False
        self.disable_rollback = False
        self.presync_done = False
        self.presnap_trigger = None
        self.postsnap_trigger = None
        self.monitor_action = None
        self.disabled = False
        self.anti_affinity = None
        self.autostart_node = []
        self.lock_timeout = 60

        # merged by the cmdline parser
        self.options = Storage(
            color="auto",
            slaves=False,
            slave=None,
            master=False,
            cron=False,
            force=False,
            remote=False,
            ignore_affinity=False,
            debug=False,
            disable_rollback=False,
            show_disabled=None,
            moduleset="",
            module="",
            ruleset_date="",
            dry_run=False,
            refresh=False,
            parm_rid=None,
            parm_tags=None,
            parm_subsets=None,
            discard=False,
            recover=False,
        )

        self.log = rcLogger.initLogger(self.svcname)
        Scheduler.__init__(self, config_defaults=CONFIG_DEFAULTS)

        self.scsirelease = self.prstop
        self.scsireserv = self.prstart
        self.scsicheckreserv = self.prstatus


    @lazy
    def freezer(self):
        """
        Lazy allocator for the freezer object.
        """
        return Freezer(self.svcname)

    @lazy
    def scheduler_actions(self):
        """
        Define the non-resource dependent scheduler tasks.
        """
        return {
            "compliance_auto": SchedOpts(
                "DEFAULT",
                fname=self.svcname+os.sep+"last_comp_check",
                schedule_option="comp_schedule"
            ),
            "push_service_status": SchedOpts(
                "DEFAULT",
                fname=self.svcname+os.sep+"last_push_service_status",
                schedule_option="status_schedule"
            ),
        }

    def __lt__(self, other):
        """
        Order by service name
        """
        return self.svcname < other.svcname

    def scheduler(self):
        """
        The service scheduler action entrypoint.
        """
        self.options.cron = True
        self.sync_dblogger = True
        if not self.has_run_flag():
            self.log.info("the scheduler is off during init")
            return
        for action in self.scheduler_actions:
            try:
                if action == "sync_all":
                    # save the action logging to the collector if sync_all
                    # is not needed
                    self.sched_sync_all()
                elif action.startswith("task#"):
                    self.run_task(action)
                else:
                    self.action(action)
            except:
                self.save_exc()

    def post_build(self):
        """
        A method run after the service is done building.
        Add resource-dependent tasks to the scheduler.
        """
        if not self.encap:
            self.scheduler_actions["push_config"] = SchedOpts(
                "DEFAULT",
                fname=self.svcname+os.sep+"last_push_config",
                schedule_option="push_schedule"
            )

        if self.ha and "flex" not in self.clustertype:
            self.scheduler_actions["resource_monitor"] = SchedOpts(
                "DEFAULT",
                fname=self.svcname+os.sep+"last_resource_monitor",
                schedule_option="monitor_schedule"
            )

        syncs = []

        for resource in self.get_resources("sync"):
            syncs += [SchedOpts(
                resource.rid,
                fname=self.svcname+os.sep+"last_syncall_"+resource.rid,
                schedule_option="sync_schedule"
            )]

        if len(syncs) > 0:
            self.scheduler_actions["sync_all"] = syncs

        for resource in self.get_resources("task"):
            self.scheduler_actions[resource.rid] = SchedOpts(
                resource.rid,
                fname=self.svcname+os.sep+"last_"+resource.rid,
                schedule_option="no_schedule"
            )

        self.scheduler_actions["push_resinfo"] = SchedOpts(
            "DEFAULT",
            fname=self.svcname+os.sep+"last_push_resinfo",
            schedule_option="resinfo_schedule"
        )

    def purge_status_last(self):
        """
        Purge all service resources on-disk status caches.
        """
        for rset in self.resourcesets:
            rset.purge_status_last()

    def get_subset_parallel(self, rtype):
        """
        Return True if the resources of a resourceset can run an action in
        parallel executing per-resource workers.
        """
        rtype = rtype.split(".")[0]
        subset_section = 'subset#' + rtype
        if self.config is None:
            self.load_config()
        if not self.config.has_section(subset_section):
            return False
        try:
            return conf_get_boolean_scope(self, self.config, subset_section, "parallel")
        except ex.OptNotFound:
            return False

    def __iadd__(self, other):
        """
        Svc += ResourceSet
        Svc += Resource
        """
        if hasattr(other, 'resources'):
            # new ResourceSet or ResourceSet-derived class
            self.resourcesets.append(other)
            self.resourcesets_by_type[other.type] = other
            other.svc = self
            return self

        if other.subset is not None:
            # the resource wants to be added to a specific resourceset
            # for action grouping, parallel execution or sub-resource
            # triggers
            base_type = other.type.split(".")[0]
            rtype = "%s:%s" % (base_type, other.subset)
        else:
            rtype = other.type

        if rtype in self.resourcesets_by_type:
            # the resource set already exists. add resource or resourceset.
            self.resourcesets_by_type[rtype] += other
        elif isinstance(other, Resource):
            parallel = self.get_subset_parallel(rtype)
            if hasattr(other, 'rset_class'):
                rset = other.rset_class(type=rtype, resources=[other], parallel=parallel)
            else:
                rset = ResourceSet(type=rtype, resources=[other], parallel=parallel)
            rset.rid = rtype
            rset.svc = self
            rset.pg_settings = get_pg_settings(self, "subset#"+rtype)
            self.__iadd__(rset)
        else:
            self.log.debug("unexpected object addition to the service: %s",
                           str(other))

        if isinstance(other, Resource) and other.rid and "#" in other.rid:
            self.resources_by_id[other.rid] = other

        other.svc = self

        if other.type.startswith("hb"):
            self.ha = True

        if not other.disabled and hasattr(other, "on_add"):
            other.on_add()

        return self

    def dblogger(self, action, begin, end, actionlogfile):
        """
        Send to the collector the service status after an action, and
        the action log.
        """
        self.node.collector.call(
            'end_action', self, action, begin, end, actionlogfile,
            sync=self.sync_dblogger
        )
        g_vars, g_vals, r_vars, r_vals = self.svcmon_push_lists()
        self.node.collector.call(
            'svcmon_update_combo', g_vars, g_vals, r_vars, r_vals,
            sync=self.sync_dblogger
        )
        os.unlink(actionlogfile)
        try:
            logging.shutdown()
        except:
            pass

    def svclock(self, action=None, timeout=30, delay=5):
        """
        Acquire the service action lock.
        """
        suffix = None
        if action in ACTIONS_NO_LOCK or \
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

        lockfile = os.path.join(rcEnv.pathlock, self.svcname)
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))

        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire service lock %s", details)
        try:
            lockfd = lock.lock(
                timeout=timeout,
                delay=delay,
                lockfile=lockfile,
                intent=action
            )
        except lock.lockTimeout as exc:
            raise ex.excError("timed out waiting for lock %s: %s" % (details, str(exc)))
        except lock.lockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.lockCreateError:
            raise ex.excError("can not create lock file %s" % details)
        except lock.lockAcquire as exc:
            raise ex.excError("another action is currently running %s: %s" % (details, str(exc)))
        except ex.excSignal:
            raise ex.excError("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.excError("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def svcunlock(self):
        """
        Release the service action lock.
        """
        lock.unlock(self.lockfd)
        self.lockfd = None

    @staticmethod
    def setup_signal_handlers():
        """
        Install signal handlers.
        """
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_resources(self, _type=None, strict=False, discard_disabled=True):
        """
        Return the list of resources matching criteria.
        """
        if _type is None:
            rsets = self.resourcesets
        else:
            rsets = self.get_resourcesets(_type, strict=strict)

        resources = []
        for rset in rsets:
            for resource in rset.resources:
                if not self.encap and 'encap' in resource.tags:
                    continue
                if discard_disabled and resource.disabled:
                    continue
                resources.append(resource)
        return resources

    def get_resourcesets(self, _type, strict=False):
        """
        Return the list of resourceset matching the specified types.
        """
        if not isinstance(_type, list):
            _types = [_type]
        else:
            _types = _type
        rsets_by_type = {}
        for rset in self.resourcesets:
            if ':' in rset.type and rset.has_resource_with_types(_types, strict=strict):
                # subset
                rsets_by_type[rset.type] = rset
                continue
            rs_base_type = rset.type.split(".")[0]
            if rset.type in _types:
                # exact match
                if rs_base_type not in rsets_by_type:
                    rsets_by_type[rs_base_type] = type(rset)(type=rs_base_type)
                    rsets_by_type[rs_base_type].svc = self
                rsets_by_type[rs_base_type] += rset
            elif rs_base_type in _types and not strict:
                # group match
                if rs_base_type not in rsets_by_type:
                    rsets_by_type[rs_base_type] = type(rset)(type=rs_base_type)
                    rsets_by_type[rs_base_type].svc = self
                rsets_by_type[rs_base_type] += rset
        rsets = list(rsets_by_type.values())
        rsets.sort()
        return rsets

    def has_resourceset(self, _type, strict=False):
        """
        Return True if the service has a resource set of the specified type.
        """
        return len(self.get_resourcesets(_type, strict=strict)) > 0

    def all_set_action(self, action=None, tags=None):
        """
        Execute an action on all resources all resource sets.
        """
        self.set_action(self.resourcesets, action=action, tags=tags)

    def sub_set_action(self, _type=None, action=None, tags=None, xtags=None,
                       strict=False):
        """
        Execute an action on all resources of the resource sets of the
        specified type.
        """
        rsets = self.get_resourcesets(_type, strict=strict)
        self.set_action(rsets, action=action, tags=tags, xtags=xtags)

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
                   rcEnv.nodename in self.nodes:
                    return True
        return False

    def set_action(self, rsets=None, action=None, tags=None, xtags=None):
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
            * excError, stop looping over the resources and propagate up
              to the caller.
            * excAbortAction, continue looping over the resources
            * any other exception, save the traceback in the debug log
              and stop looping over the resources and raise an excError
            """
            for rset in rsets:
                if action in ACTIONS_NO_TRIGGER or rset.all_skip(action):
                    break
                try:
                    rset.log.debug("start %s %s_action", rset.type, when)
                    getattr(rset, when + "_action")(action)
                except ex.excError:
                    raise
                except ex.excAbortAction:
                    continue
                except:
                    self.save_exc()
                    raise ex.excError

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
                raise ex.excError(results[2])

        need_snap = self.need_snap_trigger(rsets, action)

        # Multiple resourcesets of the same type need to be sorted
        # so that the start and stop action happen in a predictible order.
        # Sort alphanumerically on reseourceset type.
        #
        #  Example, on start:
        #   app
        #   app.1
        #   app.2
        #  on stop:
        #   app.2
        #   app.1
        #   app
        reverse = "stop" in action or action in ("rollback", "shutdown", "unprovision")
        rsets = sorted(rsets, key=lambda x: x.type, reverse=reverse)

        # snapshots are created in pre_action and destroyed in post_action
        # place presnap and postsnap triggers around pre_action
        do_snap_trigger("pre")
        do_trigger("pre")
        do_snap_trigger("post")

        for rset in rsets:
            self.log.debug('set_action: action=%s rset=%s', action, rset.type)
            rset.action(action, tags=tags, xtags=xtags)

        do_trigger("post")

    def __str__(self):
        """
        The Svc class print formatter.
        """
        output = "Service %s available resources:" % self.svcname
        for key in self.resourcesets_by_type:
            output += " %s" % key
        output += "\n"
        for rset in self.resourcesets:
            output += "  [%s]" % str(rset)
        return output

    def status(self):
        """
        Return the aggregate status a service.
        """
        group_status = self.group_status()
        return group_status["overall"].status

    def print_status_data(self):
        """
        Return a structure containing hierarchical status of
        the service.
        """
        data = {
            "resources": {},
            "frozen": self.frozen(),
        }

        containers = self.get_resources('container')
        if len(containers) > 0:
            data['encap'] = {}
            for container in containers:
                if container.name is None or len(container.name) == 0:
                    continue
                try:
                    data['encap'][container.name] = self.encap_json_status(container)
                except:
                    data['encap'][container.name] = {'resources': {}}

        for rset in self.get_resourcesets(STATUS_TYPES, strict=True):
            for resource in rset.resources:
                (
                    rid,
                    rtype,
                    status,
                    label,
                    log,
                    monitor,
                    disable,
                    optional,
                    encap
                ) = resource.status_quad(color=False)
                data['resources'][rid] = {
                    'status': status,
                    'type': rtype,
                    'label': label,
                    'log': log,
                    'tags': sorted(list(resource.tags)),
                    'monitor':monitor,
                    'disable': disable,
                    'optional': optional,
                    'encap': encap,
                }
        group_status = self.group_status()
        for group in group_status:
            data[group] = str(group_status[group])
        return data

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
            if evaluate:
                data[key] = conf_get_string_scope(self, self.config, 'env', key)
            else:
                data[key] = config["env"][key]
        return data

    def print_config_data(self):
        """
        Return a simple dict (OrderedDict if possible), fed with the
        service configuration sections and keys
        """
        try:
            from collections import OrderedDict
            best_dict = OrderedDict
        except ImportError:
            best_dict = dict
        svc_config = best_dict()
        tmp = best_dict()
        self.load_config()
        config = self.config

        defaults = config.defaults()
        for key in defaults.keys():
            tmp[key] = defaults[key]

        svc_config['DEFAULT'] = tmp
        config._defaults = {}

        sections = config.sections()
        for section in sections:
            options = config.options(section)
            tmpsection = best_dict()
            for option in options:
                if config.has_option(section, option):
                    tmpsection[option] = config.get(section, option)
            svc_config[section] = tmpsection
        self.load_config()
        return svc_config

    def logs(self):
        """
        Extract and display the service logs, honoring --color and --debug
        """
        if not os.path.exists(rcEnv.logfile):
            return

        from rcColor import color, colorize
        class Shared(object):
            """
            A bare class to store a persistent flag.
            """
            skip = False

        def fmt(line):
            """
            Format a log line, colorizing the log level.
            Return the line as a string buffer.
            """
            line = line.rstrip("\n")
            elements = line.split(" - ")

            if len(elements) < 3 or elements[2] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
                # this is a log line continuation (command output for ex.)
                if Shared.skip:
                    return
                else:
                    return line

            if not self.options.debug and elements[2] == "DEBUG":
                Shared.skip = True
                return
            else:
                Shared.skip = False

            if not rcLogger.include_svcname:
                elements[1] = elements[1].replace(self.svcname, "").lstrip(".")
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
            with open(rcEnv.logfile, "r") as ofile:
                for line in ofile.readlines():
                    buff = fmt(line)
                    if buff:
                        print(buff)
        except BrokenPipeError:
            try:
                sys.stdout = os.fdopen(1)
            except:
                pass

    def print_resource_status(self):
        """
        Print a single resource status string.
        """
        if len(self.action_rid) != 1:
            print("action 'print_resource_status' is not allowed on mutiple "
                  "resources", file=sys.stderr)
            return 1
        for rid in self.action_rid:
            if rid not in self.resources_by_id:
                print("resource not found")
                continue
            resource = self.resources_by_id[rid]
            print(rcStatus.colorize_status(str(resource.status())))
        return 0

    def print_status(self):
        """
        Display in human-readable format the hierarchical service status.
        """
        if self.options.format is not None:
            return self.print_status_data()

        from textwrap import wrap
        from rcUtilities import term_width
        from rcColor import color, colorize

        width = term_width()

        def print_res(squad, fmt, pfx, subpfx=None):
            """
            Print a resource line, with forest markers, rid, flags, label and
            resource log.
            """
            if subpfx is None:
                subpfx = pfx
            rid, status, label, log, monitor, disabled, optional, encap = squad
            flags = ''
            flags += 'M' if monitor else '.'
            flags += 'D' if disabled else '.'
            flags += 'O' if optional else '.'
            flags += 'E' if encap else '.'
            print(fmt % (rid, flags, rcStatus.colorize_status(status), label))
            for msg in log.split("\n"):
                if len(msg) > 0:
                    if subpfx and not subpfx.startswith(color.END):
                        subpfx = color.END + subpfx
                    print('\n'.join(wrap(msg,
                                         initial_indent=subpfx,
                                         subsequent_indent=subpfx,
                                         width=width
                                        )
                                   )
                         )

        if self.options.show_disabled is not None:
            discard_disabled = not self.options.show_disabled
        else:
            discard_disabled = not self.show_disabled

        def get_res(group):
            """
            Wrap get_resources() with discard_disable relaying and sorted
            resultset.
            """
            resources = self.get_resources(
                group,
                discard_disabled=discard_disabled,
            )
            return sorted(resources)

        avail_resources = get_res("ip")
        avail_resources += get_res("disk")
        avail_resources += get_res("fs")
        avail_resources += get_res("container")
        avail_resources += get_res("share")
        avail_resources += get_res("app")
        accessory_resources = get_res("hb")
        accessory_resources += get_res("stonith")
        accessory_resources += get_res("sync")
        n_accessory_resources = len(accessory_resources)

        print(colorize(self.svcname, color.BOLD))
        frozen = 'frozen' if self.frozen() else ''
        fmt = "%-20s %4s %-10s %s"
        color_status = rcStatus.colorize_status(self.group_status()['overall'])
        print(fmt % ("overall", '', color_status, frozen))
        if n_accessory_resources == 0:
            fmt = "'- %-17s %4s %-10s %s"
            head_c = " "
        else:
            fmt = "|- %-17s %4s %-10s %s"
            head_c = "|"
        color_status = rcStatus.colorize_status(self.group_status()['avail'])
        print(fmt % ("avail", '', color_status, ''))

        ers = {}
        for container in self.get_resources('container'):
            try:
                ejs = self.encap_json_status(container)
                ers[container.rid] = ejs["resources"]
                if ejs.get("frozen", False):
                    container.status_log("frozen", "info")
            except ex.excNotAvailable:
                ers[container.rid] = {}
            except Exception as exc:
                print(exc)
                ers[container.rid] = {}

        lines = []
        encap_squad = {}
        for resource in avail_resources:
            (
                rid,
                rtype,
                status,
                label,
                log,
                monitor,
                disable,
                optional,
                encap
            ) = resource.status_quad()
            lines.append((rid, status, label, log, monitor, disable, optional, encap))
            if rid.startswith("container") and rid in ers:
                squad = []
                for _rid, val in ers[rid].items():
                    squad.append((
                        _rid,
                        val['status'],
                        val['label'],
                        val['log'],
                        val['monitor'],
                        val['disable'],
                        val['optional'],
                        val['encap'],
                    ))
                encap_squad[rid] = squad

        last = len(lines) - 1
        if last >= 0:
            for idx, line in enumerate(lines):
                if idx == last:
                    fmt = head_c+"  '- %-14s %4s %-10s %s"
                    pfx = head_c+"     %-14s %4s %-10s " % ('', '', '')
                    subpfx = head_c+"        %-11s %4s %-10s " % ('', '', '')
                    print_res(line, fmt, pfx, subpfx=subpfx)
                    subresbar = " "
                else:
                    fmt = head_c+"  |- %-14s %4s %-10s %s"
                    pfx = head_c+"  |  %-14s %4s %-10s " % ('', '', '')
                    if line[0] in encap_squad and len(encap_squad[line[0]]) > 0:
                        subpfx = head_c+"  |  |  %-11s %4s %-10s " % ('', '', '')
                    else:
                        subpfx = None
                    print_res(line, fmt, pfx, subpfx=subpfx)
                    subresbar = "|"
                if line[0] in encap_squad:
                    _last = len(encap_squad[line[0]]) - 1
                    if _last >= 0:
                        for _idx, _line in enumerate(encap_squad[line[0]]):
                            if _idx == _last:
                                fmt = head_c+"  "+subresbar+"  '- %-11s %4s %-10s %s"
                                pfx = head_c+"  "+subresbar+"     %-11s %4s %-10s " % ('', '', '')
                                print_res(_line, fmt, pfx)
                            else:
                                fmt = head_c+"  "+subresbar+"  |- %-11s %4s %-10s %s"
                                pfx = head_c+"  "+subresbar+"  |  %-11s %4s %-10s " % ('', '', '')
                                print_res(_line, fmt, pfx)

        if n_accessory_resources > 0:
            fmt = "'- %-17s %4s %-10s %s"
            print(fmt%("accessory", '', '', ''))

        lines = []
        for resource in accessory_resources:
            rid, rtype, status, label, log, monitor, disable, optional, encap = resource.status_quad()
            if rid in ers:
                status = rcStatus.Status(rcStatus.status_value(ers[rid]['status']))
            lines.append((rid, status, label, log, monitor, disable, optional, encap))

        last = len(lines) - 1
        if last >= 0:
            for idx, line in enumerate(lines):
                if idx == last:
                    fmt = "   '- %-14s %4s %-10s %s"
                    pfx = "      %-14s %4s %-10s " % ('', '', '')
                    print_res(line, fmt, pfx)
                else:
                    fmt = "   |- %-14s %4s %-10s %s"
                    pfx = "   |  %-14s %4s %-10s " % ('', '', '')
                    print_res(line, fmt, pfx)

    def svcmon_push_lists(self, status=None):
        """
        Return the list of resource status in a format adequate for
        collector feeding.
        """
        if status is None:
            status = self.group_status()

        if self.frozen():
            frozen = "1"
        else:
            frozen = "0"

        r_vars = [
            "svcname",
            "nodename",
            "vmname",
            "rid",
            "res_type",
            "res_desc",
            "res_status",
            "res_monitor",
            "res_optional",
            "res_disable",
            "updated",
            "res_log",
        ]
        r_vals = []
        now = datetime.datetime.now()

        for rset in self.resourcesets:
            for resource in rset.resources:
                if 'encap' in resource.tags:
                    continue
                rstatus = str(resource.rstatus)
                r_vals.append([
                    self.svcname,
                    rcEnv.nodename,
                    "",
                    resource.rid,
                    resource.type,
                    resource.label,
                    str(rstatus),
                    "1" if resource.monitor else "0",
                    "1" if resource.optional else "0",
                    "1" if resource.disabled else "0",
                    str(now),
                    resource.status_logs_str(),
                ])

        g_vars = [
            "mon_svcname",
            "mon_svctype",
            "mon_nodname",
            "mon_vmname",
            "mon_vmtype",
            "mon_nodtype",
            "mon_ipstatus",
            "mon_diskstatus",
            "mon_syncstatus",
            "mon_hbstatus",
            "mon_containerstatus",
            "mon_fsstatus",
            "mon_sharestatus",
            "mon_appstatus",
            "mon_availstatus",
            "mon_overallstatus",
            "mon_updated",
            "mon_prinodes",
            "mon_frozen",
        ]

        containers = self.get_resources('container')
        containers = [container for container in containers \
                      if container.type != "container.docker"]
        if len(containers) == 0:
            g_vals = [
                self.svcname,
                self.svc_env,
                rcEnv.nodename,
                "",
                "hosted",
                rcEnv.node_env,
                str(status["ip"]),
                str(status["disk"]),
                str(status["sync"]),
                str(status["hb"]),
                str(status["container"]),
                str(status["fs"]),
                str(status["share"]),
                str(status["app"]),
                str(status["avail"]),
                str(status["overall"]),
                str(now),
                ' '.join(self.nodes),
                frozen,
            ]
        else:
            g_vals = []
            for container in containers:
                ers = {}
                try:
                    ers = self.encap_json_status(container)
                except ex.excNotAvailable:
                    ers = {
                        'resources': [],
                        'ip': 'n/a',
                        'disk': 'n/a',
                        'sync': 'n/a',
                        'hb': 'n/a',
                        'container': 'n/a',
                        'fs': 'n/a',
                        'share': 'n/a',
                        'app': 'n/a',
                        'avail': 'n/a',
                        'overall': 'n/a',
                    }
                except Exception as exc:
                    print(exc)
                    continue

                vhostname = container.vm_hostname()

                for rid in ers['resources']:
                    rstatus = ers['resources'][rid]['status']
                    r_vals.append([
                        self.svcname,
                        rcEnv.nodename,
                        vhostname,
                        str(rid),
                        ers['resources'][rid].get('type', ''),
                        str(ers['resources'][rid]['label']),
                        str(rstatus),
                        "1" if ers['resources'][rid].get('monitor', False) else "0",
                        "1" if ers['resources'][rid].get('optional', False) else "0",
                        "1" if ers['resources'][rid].get('disabled', False) else "0",
                        str(now),
                        str(ers['resources'][rid]['log']),
                    ])

                if 'avail' not in status or 'avail' not in ers:
                    continue

                g_vals.append([
                    self.svcname,
                    self.svc_env,
                    rcEnv.nodename,
                    vhostname,
                    container.type.replace('container.', ''),
                    rcEnv.node_env,
                    str(status["ip"]+rcStatus.Status(ers['ip'])),
                    str(status["disk"]+rcStatus.Status(ers['disk'])),
                    str(status["sync"]+rcStatus.Status(ers['sync'])),
                    str(status["hb"]+rcStatus.Status(ers['hb'])),
                    str(status["container"]+rcStatus.Status(ers['container'])),
                    str(status["fs"]+rcStatus.Status(ers['fs'])),
                    str(status["share"]+rcStatus.Status(ers['share'] if 'share' in ers else 'n/a')),
                    str(status["app"]+rcStatus.Status(ers['app'])),
                    str(status["avail"]+rcStatus.Status(ers['avail'])),
                    str(status["overall"]+rcStatus.Status(ers['overall'])),
                    str(now),
                    ' '.join(self.nodes),
                    frozen,
                ])

        return g_vars, g_vals, r_vars, r_vals

    def get_rset_status(self, groups):
        """
        Return the aggregated status of all resources of the specified resource
        sets, as a dict of status indexed by resourceset type.
        """
        self.setup_environ()
        rsets_status = {}
        for status_type in STATUS_TYPES:
            group = status_type.split('.')[0]
            if group not in groups:
                continue
            for rset in self.get_resourcesets(status_type, strict=True):
                if rset.type not in rsets_status:
                    rsets_status[rset.type] = rset.status()
                else:
                    rsets_status[rset.type] += rset.status()
        return rsets_status

    def resource_monitor(self):
        """
        The resource monitor action entrypoint
        """
        if self.skip_action("resource_monitor"):
            return
        self.task_resource_monitor()

    @scheduler_fork
    def task_resource_monitor(self):
        """
        The resource monitor action.
        Trigger the service defined monitor_action if the hb resource is up
        but a monitored resource is down and not restartable.
        """
        self.options.refresh = True
        if self.group_status_cache is None:
            self.group_status(excluded_groups=set(['sync']))
        if not self.ha:
            self.log.debug("no active heartbeat resource. no need to check "
                           "monitored resources.")
            return
        hb_status = self.group_status_cache['hb']
        if hb_status.status != rcStatus.UP:
            self.log.debug("heartbeat status is not up. no need to check "
                           "monitored resources.")
            return

        monitored_resources = []
        for resource in self.get_resources():
            if resource.monitor:
                monitored_resources.append(resource)

        for resource in monitored_resources:
            if resource.rstatus not in (rcStatus.UP, rcStatus.STDBY_UP, rcStatus.NA):
                if len(resource.status_logs) > 0:
                    rstatus_log = " (%s)" % resource.status_logs_str().strip().strip("# ")
                else:
                    rstatus_log = ''
                self.log.info("monitored resource %s is in state %s%s",
                              resource.rid,
                              str(resource.rstatus),
                              rstatus_log)

                if self.monitor_action is not None and \
                   hasattr(self, self.monitor_action):
                    raise ex.MonitorAction
                else:
                    self.log.info("Would TOC but no (or unknown) resource "
                                  "monitor action set.")
                return

        for container in self.get_resources('container'):
            try:
                encap_status = self.encap_json_status(container)
                res = encap_status["resources"]
            except Exception:
                encap_status = {}
                res = {}
            if encap_status.get("frozen"):
                continue
            for rid, rdata in res.items():
                if not rdata.get("monitor"):
                    continue
                erid = rid+"@"+container.name
                monitored_resources.append(erid)
                if rdata.get("status") not in ("up", "n/a"):
                    if len(rdata.get("log")) > 0:
                        rstatus_log = " (%s)" % rdata.get("log").strip().strip("# ")
                    else:
                        rstatus_log = ""
                    self.log.info("monitored resource %s is in state %s%s",
                                  erid, rdata.get("status"), rstatus_log)

                    if self.monitor_action is not None and \
                       hasattr(self, self.monitor_action):
                        raise ex.MonitorAction
                    else:
                        self.log.info("Would TOC but no (or unknown) resource "
                                      "monitor action set.")
                    return

        if len(monitored_resources) == 0:
            self.log.debug("no monitored resource")
        else:
            rids = ','.join([res if isinstance(res, (str, unicode)) else res.rid \
                             for res in monitored_resources])
            self.log.debug("monitored resources are up (%s)", rids)

    def reboot(self):
        """
        A method wrapper the node reboot method.
        """
        self.node.system._reboot()

    def crash(self):
        """
        A method wrapper the node crash method.
        """
        self.node.system.crash()

    def _pg_freeze(self):
        """
        Wrapper function for the process group freeze method.
        """
        return self._pg_freezer("freeze")

    def _pg_thaw(self):
        """
        Wrapper function for the process group thaw method.
        """
        return self._pg_freezer("thaw")

    def _pg_kill(self):
        """
        Wrapper function for the process group kill method.
        """
        return self._pg_freezer("kill")

    def _pg_freezer(self, action):
        """
        Wrapper function for the process group methods.
        """
        if not self.create_pg:
            return
        if self.pg is None:
            return
        if action == "freeze":
            self.pg.freeze(self)
        elif action == "thaw":
            self.pg.thaw(self)
        elif action == "kill":
            self.pg.kill(self)

    @lazy
    def pg(self):
        """
        A lazy property to import the system-specific process group module
        on-demand and expose it as self.pg
        """
        try:
            mod = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.log.info("process group are not supported on this platform")
            return
        except Exception as exc:
            print(exc)
            raise
        return mod

    def pg_freeze(self):
        """
        Freeze all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_freeze')
            self.sub_set_action('container', '_pg_freeze')
        else:
            self._pg_freeze()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True, restart=False)

    def pg_thaw(self):
        """
        Thaw all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_thaw')
            self.sub_set_action('container', '_pg_thaw')
        else:
            self._pg_thaw()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True, restart=False)

    def pg_kill(self):
        """
        Kill all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_kill')
            self.sub_set_action('container', '_pg_kill')
        else:
            self._pg_kill()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True, restart=False)

    def freezestop(self):
        """
        The 'freezestop' action entrypoint.
        Call the freezestop method of resources implementing it.
        """
        self.sub_set_action('hb.openha', 'freezestop')

    def stonith(self):
        """
        The 'stonith' action entrypoint.
        Call the stonith method of resources implementing it.
        """
        self.sub_set_action('stonith.ilo', 'start')
        self.sub_set_action('stonith.callout', 'start')

    def toc(self):
        """
        Call the resource monitor action.
        """
        self.log.info("start monitor action '%s'", self.monitor_action)
        getattr(self, self.monitor_action)()

    def encap_cmd(self, cmd, verbose=False, error="raise"):
        """
        Execute a command in all service containers.
        If error is set to "raise", stop iterating at first error.
        If error is set to "continue", log errors and proceed to the next
        container.
        """
        for container in self.get_resources('container'):
            try:
                self._encap_cmd(cmd, container, verbose=verbose)
            except ex.excEncapUnjoignable:
                if error != "continue":
                    self.log.error("container %s is not joinable to execute "
                                   "action '%s'", container.name, ' '.join(cmd))
                    raise
                elif verbose:
                    self.log.warning("container %s is not joinable to execute "
                                     "action '%s'", container.name, ' '.join(cmd))

    def _encap_cmd(self, cmd, container, verbose=False):
        """
        Execute a command in a service container.
        """
        if container.pg_frozen():
            raise ex.excError("can't join a frozen container. abort encap "
                              "command.")
        vmhostname = container.vm_hostname()
        try:
            autostart_node = conf_get_string_scope(self, self.config,
                                                   'DEFAULT', 'autostart_node',
                                                   impersonate=vmhostname).split()
        except ex.OptNotFound:
            autostart_node = []
        if cmd == ["start"] and container.booted and vmhostname in autostart_node:
            self.log.info("skip encap service start in container %s: already "
                          "started on boot", vmhostname)
            return '', '', 0
        if not self.has_encap_resources:
            self.log.debug("skip encap %s: no encap resource", ' '.join(cmd))
            return '', '', 0
        if not container.is_up():
            self.log.info("skip encap %s: the container is not running here",
                          ' '.join(cmd))
            return '', '', 0

        if self.options.slave is not None and not \
           (container.name in self.options.slave or \
            container.rid in self.options.slave):
            # no need to run encap cmd (container not specified in --slave)
            return '', '', 0

        if cmd == ['start'] and not self.need_start_encap(container):
            self.log.info("skip start in container %s: the encap service is "
                          "configured to start on container boot.",
                          container.name)
            return '', '', 0

        # now we known we'll execute a command in the slave, so purge the
        # encap cache
        self.purge_cache_encap_json_status(container.rid)

        options = ['--daemon']
        if self.options.dry_run:
            options.append('--dry-run')
        if self.options.refresh:
            options.append('--refresh')
        if self.options.disable_rollback:
            options.append('--disable-rollback')
        if self.options.parm_rid:
            options.append('--rid')
            options.append(self.options.parm_rid)
        if self.options.parm_tags:
            options.append('--tags')
            options.append(self.options.parm_tags)
        if self.options.parm_subsets:
            options.append('--subsets')
            options.append(self.options.parm_subsets)

        paths = get_osvc_paths(osvc_root_path=container.osvc_root_path,
                               sysname=container.guestos)
        cmd = [paths.svcmgr, '-s', self.svcname] + options + cmd

        if container is not None and hasattr(container, "rcmd"):
            out, err, ret = container.rcmd(cmd)
        elif hasattr(container, "runmethod"):
            cmd = container.runmethod + cmd
            out, err, ret = justcall(cmd)
        else:
            raise ex.excEncapUnjoignable("undefined rcmd/runmethod in "
                                         "resource %s"%container.rid)

        if verbose:
            self.log.info('logs from %s child service:', container.name)
            print(out)
            if len(err) > 0:
                print(err)
        if ret != 0:
            raise ex.excError("error from encap service command '%s': "
                              "%d\n%s\n%s"%(' '.join(cmd), ret, out, err))
        return out, err, ret

    def get_encap_json_status_path(self, rid):
        """
        Return the path of the file where the status data of the service
        encapsulated in the container identified by <rid> will be written
        for caching.
        """
        return os.path.join(rcEnv.pathvar, self.svcname, "encap.status."+rid)

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
        import json
        self.encap_json_status_cache[rid] = data
        path = self.get_encap_json_status_path(rid)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with open(path, 'w') as ofile:
                ofile.write(json.dumps(data))
        except (IOError, OSError, ValueError):
            os.unlink(path)

    def get_cache_encap_json_status(self, rid):
        """
        Fetch the on-disk cache of status of the service encapsulated in
        the container identified by <rid>.
        """
        import json
        if rid in self.encap_json_status_cache:
            return self.encap_json_status_cache[rid]
        path = self.get_encap_json_status_path(rid)
        try:
            with open(path, 'r') as ofile:
                group_status = json.loads(ofile.read())
        except (IOError, OSError, ValueError):
            group_status = None
        return group_status

    def encap_json_status(self, container, refresh=False):
        """
        Return the status data from the agent runnning the encapsulated part
        of the service.
        """
        if container.guestos == 'windows':
            raise ex.excNotAvailable

        if container.status(ignore_nostatus=True) == rcStatus.DOWN:
            #
            #  passive node for the vservice => forge encap resource status
            #    - encap sync are n/a
            #    - other encap res are down
            #
            group_status = {
                "avail": "down",
                "overall": "down",
                "resources": {},
            }
            groups = set(["container", "ip", "disk", "fs", "share", "hb"])
            for group in groups:
                group_status[group] = 'down'
            for rset in self.get_resourcesets(STATUS_TYPES, strict=True):
                group = rset.type.split('.')[0]
                if group not in groups:
                    continue
                for resource in rset.resources:
                    if not self.encap and 'encap' in resource.tags:
                        group_status['resources'][resource.rid] = {'status': 'down'}

            groups = set(["app", "sync"])
            for group in groups:
                group_status[group] = 'n/a'
            for rset in self.get_resourcesets(groups):
                group = rset.type.split('.')[0]
                if group not in groups:
                    continue
                for resource in rset.resources:
                    if not self.encap and 'encap' in resource.tags:
                        group_status['resources'][resource.rid] = {'status': 'n/a'}

            return group_status

        if not refresh and not self.options.refresh:
            group_status = self.get_cache_encap_json_status(container.rid)
            if group_status:
                return group_status

        group_status = {
            "avail": "n/a",
            "overall": "n/a",
            "resources": {},
        }
        groups = set(["container", "ip", "disk", "fs", "share", "hb", "app", "sync"])
        for group in groups:
            group_status[group] = 'n/a'

        cmd = ['json', 'status']
        try:
            results = self._encap_cmd(cmd, container)
        except ex.excError:
            return group_status
        except Exception as exc:
            print(exc)
            return group_status

        import json
        try:
            group_status = json.loads(results[0])
        except:
            pass

        self.put_cache_encap_json_status(container.rid, group_status)

        return group_status

    def group_status(self, groups=None, excluded_groups=None):
        """
        Return the status data of the service.
        """
        if excluded_groups is None:
            excluded_groups = set()
        if groups is None:
            groups = set(DEFAULT_STATUS_GROUPS)

        status = {}
        moregroups = groups | set(["overall", "avail"])
        groups = groups - excluded_groups
        self.get_rset_status(groups)

        # initialise status of each group
        for group in moregroups:
            status[group] = rcStatus.Status(rcStatus.NA)

        for driver in [_driver for _driver in STATUS_TYPES if \
                  not _driver.startswith('sync') and \
                  not _driver.startswith('hb') and \
                  not _driver.startswith('stonith')]:
            if driver in excluded_groups:
                continue
            group = driver.split('.')[0]
            if group not in groups:
                continue
            for resource in self.get_resources(driver):
                rstatus = resource.status()
                status[group] += rstatus
                status["avail"] += rstatus

        if status["avail"].status == rcStatus.STDBY_UP_WITH_UP:
            status["avail"].status = rcStatus.UP
            # now that we now the avail status we can promote
            # stdbyup to up
            for group in status:
                if status[group] == rcStatus.STDBY_UP:
                    status[group] = rcStatus.UP
        elif status["avail"].status == rcStatus.STDBY_UP_WITH_DOWN:
            status["avail"].status = rcStatus.STDBY_UP

        # overall status is avail + accessory resources status
        # seed overall with avail
        status["overall"] = rcStatus.Status(status["avail"])

        for resource in self.get_resources():
            group = resource.type.split(".")[0]
            if group not in groups:
                continue
            if resource.status_logs_count(levels=["warn", "error"]) > 0:
                status["overall"] += rcStatus.WARN
                break

        for driver in [_driver for _driver in STATUS_TYPES if \
                       _driver.startswith('stonith')]:
            if 'stonith' not in groups:
                continue
            if driver in excluded_groups:
                continue
            for resource in self.get_resources(driver):
                rstatus = resource.status()
                status['stonith'] += rstatus
                status["overall"] += rstatus

        for driver in [_driver for _driver in STATUS_TYPES if \
                       _driver.startswith('hb')]:
            if 'hb' not in groups:
                continue
            if driver in excluded_groups:
                continue
            for resource in self.get_resources(driver):
                rstatus = resource.status()
                status['hb'] += rstatus
                status["overall"] += rstatus

        for driver in [_driver for _driver in STATUS_TYPES if \
                       _driver.startswith('sync')]:
            if 'sync' not in groups:
                continue
            if driver in excluded_groups:
                continue
            for resource in self.get_resources(driver):
                #" sync are expected to be up
                rstatus = resource.status()
                status['sync'] += rstatus
                if rstatus == rcStatus.UP:
                    status["overall"] += rcStatus.UNDEF
                elif rstatus in [rcStatus.NA, rcStatus.UNDEF]:
                    status["overall"] += rstatus
                else:
                    status["overall"] += rcStatus.WARN

        self.group_status_cache = status
        return status

    def print_disklist(self):
        """
        Print the list of disks the service handles.
        """
        if self.options.format is not None:
            return self.print_disklist_data()
        disks = self.disklist()
        if len(disks) > 0:
            print('\n'.join(disks))

    def print_devlist(self):
        """
        Print the list of devices the service handles.
        """
        if self.options.format is not None:
            return self.print_devlist_data()
        devs = self.devlist()
        if len(devs) > 0:
            print('\n'.join(devs))

    def print_disklist_data(self):
        """
        Return the list of disks the service handles.
        """
        return list(self.disklist())

    def print_devlist_data(self):
        """
        Return the list of devices the service handles.
        """
        return list(self.devlist())

    def disklist(self):
        """
        Return the set of disks the service handles, from cache if possible.
        """
        if len(self.disks) == 0:
            self.disks = self._disklist()
        return self.disks

    def _disklist(self):
        """
        Return the set of disks the service handles.
        """
        disks = set()
        for resource in self.get_resources():
            if resource.skip:
                continue
            disks |= resource.disklist()
        self.log.debug("found disks %s held by service", disks)
        return disks

    def devlist(self, filtered=True):
        """
        Return the set of devices the service handles, from cache if possible.
        """
        if len(self.devs) == 0:
            self.devs = self._devlist(filtered=filtered)
        return self.devs

    def _devlist(self, filtered=True):
        """
        Return the set of devices the service handles.
        """
        devs = set()
        for resource in self.get_resources():
            if filtered and resource.skip:
                continue
            devs |= resource.devlist()
        self.log.debug("found devs %s held by service", devs)
        return devs

    def get_non_affine_svc(self):
        """
        Return the list services defined as anti-affine, filtered to retain
        only the running ones (those that will cause an actual affinity error
        on start for this service).
        """
        if not self.anti_affinity:
            return []
        self.log.debug("build anti-affine services %s", str(self.anti_affinity))
        self.node.build_services(svcnames=self.anti_affinity)
        running_af_svc = []
        for svc in self.node.svcs:
            if svc.svcname == self.svcname:
                continue
            avail = svc.group_status()['avail']
            if str(avail) != "down":
                running_af_svc.append(svc.svcname)
        return running_af_svc

    def print_config_mtime(self):
        """
        Print the service configuration file last modified timestamp. Used by
        remote agents to determine which agent holds the most recent version.
        """
        mtime = os.stat(self.paths.cf).st_mtime
        print(mtime)

    def need_start_encap(self, container):
        """
        Return True if this service has an encapsulated part that would need
        starting.
        """
        self.load_config()
        defaults = self.config.defaults()
        if defaults.get('autostart_node@'+container.name) in (container.name, 'encapnodes'):
            return False
        elif defaults.get('autostart_node@encapnodes') in (container.name, 'encapnodes'):
            return False
        elif defaults.get('autostart_node') in (container.name, 'encapnodes'):
            return False
        return True

    def boot(self):
        """
        The 'boot' action entrypoint.
        A boot is a start if the running node is defined as autostart_node.
        A boot is a startstandby if the running node is not defined as autostart_node.
        A boot is a startstandby if the service is handled by a HA monitor.
        Start errors cause a fallback to startstandby as a best effort.
        """
        if rcEnv.nodename not in self.autostart_node:
            self.startstandby()
            return

        resources = self.get_resources('hb')
        if len(resources) > 0:
            self.log.warning("cluster nodes should not be in autostart_nodes for HA configuration")
            self.startstandby()
            return

        try:
            self.start()
        except ex.excError as exc:
            self.log.error(str(exc))
            self.log.info("start failed. try to start standby")
            self.startstandby()

    def shutdown(self):
        self.options.force = True
        self.master_shutdownhb()
        self.slave_shutdown()
        try:
            self.master_shutdownapp()
        except ex.excError:
            pass
        self.shutdowncontainer()
        self.master_shutdownshare()
        self.master_shutdownfs()
        self.master_shutdownip()

    def command_is_scoped(self):
        """
        Return True if a resource filter has been setup through
        --rid, --subsets or --tags
        """
        if self.options.parm_rid is not None or \
           self.options.parm_tags is not None or \
           self.options.parm_subsets is not None:
            return True
        return False

    def run_task(self, rid):
        if self.skip_action(rid):
            return
        self.resources_by_id[rid].run()

    def run(self):
        self.master_run()
        self.slave_run()

    @_master_action
    def master_run(self):
        self.sub_set_action("task", "run")

    @_slave_action
    def slave_run(self):
        self.encap_cmd(['run'], verbose=True)

    def start(self):
        self.master_starthb()
        self.abort_start()
        af_svc = self.get_non_affine_svc()
        if len(af_svc) != 0:
            if self.options.ignore_affinity:
                self.log.error("force start of %s on the same node as %s "
                               "despite anti-affinity settings",
                               self.svcname, ', '.join(af_svc))
            else:
                self.log.error("refuse to start %s on the same node as %s",
                               self.svcname, ', '.join(af_svc))
                return
        self.master_startip()
        self.master_startfs()
        self.master_startshare()
        self.master_startcontainer()
        self.master_startapp()
        self.slave_start()

    @_slave_action
    def slave_start(self):
        self.encap_cmd(['start'], verbose=True)

    def rollback(self):
        self.encap_cmd(['rollback'], verbose=True)
        try:
            self.rollbackapp()
        except ex.excError:
            pass
        self.rollbackcontainer()
        self.rollbackshare()
        self.rollbackfs()
        self.rollbackip()

    def stop(self):
        self.master_stophb()
        self.slave_stop()
        try:
            self.master_stopapp()
        except ex.excError:
            pass
        self.stopcontainer()
        self.master_stopshare()
        self.master_stopfs()
        self.master_stopip()

    @_slave_action
    def slave_shutdown(self):
        self.encap_cmd(['shutdown'], verbose=True, error="continue")

    @_slave_action
    def slave_stop(self):
        self.encap_cmd(['stop'], verbose=True, error="continue")

    def cluster_mode_safety_net(self, action):
        """
        Raise excError to bar actions executed without --cluster on monitored
        services.

        Raise excAbortAction to bar actions executed with --cluster on monitored
        services with disabled hb resources (maintenance mode).

        In any case, consider an action with --rid, --tags or --subset set is
        not to be blocked, as it is a surgical operation typical of maintenance
        operations.
        """
        if action in ACTIONS_ALLOW_ON_CLUSTER:
            return

        if self.command_is_scoped():
            self.log.debug("stop: called with --rid, --tags or --subset, allow "
                           "action on ha service.")
            return

        n_hb = 0
        n_hb_enabled = 0

        for resource in self.get_resources('hb', discard_disabled=False):
            n_hb += 1
            if not resource.disabled:
                n_hb_enabled += 1

        if n_hb == 0:
            return

        if n_hb > 0 and n_hb_enabled == 0 and self.options.cluster:
            raise ex.excAbortAction("this service has heartbeat resources, "
                                    "but all disabled. this state is "
                                    "interpreted as a maintenance mode. "
                                    "actions submitted with --cluster are not "
                                    "allowed to inhibit actions triggered by "
                                    "the heartbeat daemon.")
        if n_hb_enabled == 0:
            return

        if not self.options.cluster:
            for resource in self.get_resources("hb"):
                if not resource.skip and hasattr(resource, action):
                    self.running_action = action
                    getattr(resource, action)()

            raise ex.excError("this service is managed by a clusterware, thus "
                              "direct service manipulation is disabled (%s). "
                              "the --cluster option circumvent this safety "
                              "net." % action)

    def starthb(self):
        self.master_starthb()
        self.slave_starthb()

    @_slave_action
    def slave_starthb(self):
        self.encap_cmd(['starthb'], verbose=True)

    @_master_action
    def master_starthb(self):
        self.master_hb('start')

    @_master_action
    def master_startstandbyhb(self):
        self.master_hb('startstandby')

    @_master_action
    def master_shutdownhb(self):
        self.master_hb('shutdown')

    @_master_action
    def master_stophb(self):
        self.master_hb('stop')

    def master_hb(self, action):
        self.sub_set_action("hb", action)

    def stophb(self):
        self.slave_stophb()
        self.master_stophb()

    @_slave_action
    def slave_stophb(self):
        self.encap_cmd(['stophb'], verbose=True)

    def startdrbd(self):
        self.master_startdrbd()
        self.slave_startdrbd()

    @_slave_action
    def slave_startdrbd(self):
        self.encap_cmd(['startdrbd'], verbose=True)

    @_master_action
    def master_startdrbd(self):
        self.sub_set_action("disk.drbd", "start")

    def stopdrbd(self):
        self.slave_stopdrbd()
        self.master_stopdrbd()

    @_slave_action
    def slave_stopdrbd(self):
        self.encap_cmd(['stopdrbd'], verbose=True)

    @_master_action
    def master_stopdrbd(self):
        self.sub_set_action("disk.drbd", "stop")

    def startloop(self):
        self.master_startloop()
        self.slave_startloop()

    @_slave_action
    def slave_startloop(self):
        self.encap_cmd(['startloop'], verbose=True)

    @_master_action
    def master_startloop(self):
        self.sub_set_action("disk.loop", "start")

    def stoploop(self):
        self.slave_stoploop()
        self.master_stoploop()

    @_slave_action
    def slave_stoploop(self):
        self.encap_cmd(['stoploop'], verbose=True)

    @_master_action
    def master_stoploop(self):
        self.sub_set_action("disk.loop", "stop")

    def stopvg(self):
        self.slave_stopvg()
        self.master_stopvg()

    @_slave_action
    def slave_stopvg(self):
        self.encap_cmd(['stopvg'], verbose=True)

    @_master_action
    def master_stopvg(self):
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.lock", "stop")
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    def startvg(self):
        self.master_startvg()
        self.slave_startvg()

    @_slave_action
    def slave_startvg(self):
        self.encap_cmd(['startvg'], verbose=True)

    @_master_action
    def master_startvg(self):
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action("disk.lock", "start")
        self.sub_set_action("disk.vg", "start")

    def startpool(self):
        self.master_startpool()
        self.slave_startpool()

    @_slave_action
    def slave_startpool(self):
        self.encap_cmd(['startpool'], verbose=True)

    @_master_action
    def master_startpool(self):
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action("disk.zpool", "start", xtags=set(['zone']))

    def stoppool(self):
        self.slave_stoppool()
        self.master_stoppool()

    @_slave_action
    def slave_stoppool(self):
        self.encap_cmd(['stoppool'], verbose=True)

    @_master_action
    def master_stoppool(self):
        self.sub_set_action("disk.zpool", "stop", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    def startdisk(self):
        self.master_startdisk()
        self.slave_startdisk()

    @_slave_action
    def slave_startdisk(self):
        self.encap_cmd(['startdisk'], verbose=True)

    @_master_action
    def master_startstandbydisk(self):
        self.sub_set_action("sync.netapp", "startstandby")
        self.sub_set_action("sync.dcsckpt", "startstandby")
        self.sub_set_action("sync.nexenta", "startstandby")
        self.sub_set_action("sync.symclone", "startstandby")
        self.sub_set_action("sync.symsnap", "startstandby")
        self.sub_set_action("sync.ibmdssnap", "startstandby")
        self.sub_set_action("disk.scsireserv", "startstandby", xtags=set(['zone']))
        self.sub_set_action(DISK_TYPES, "startstandby", xtags=set(['zone']))

    @_master_action
    def master_startdisk(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("sync.dcsckpt", "start")
        self.sub_set_action("sync.nexenta", "start")
        self.sub_set_action("sync.symclone", "start")
        self.sub_set_action("sync.symsnap", "start")
        self.sub_set_action("sync.symsrdfs", "start")
        self.sub_set_action("sync.hp3par", "start")
        self.sub_set_action("sync.ibmdssnap", "start")
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action(DISK_TYPES, "start", xtags=set(['zone']))

    def stopdisk(self):
        self.slave_stopdisk()
        self.master_stopdisk()

    @_slave_action
    def slave_stopdisk(self):
        self.encap_cmd(['stopdisk'], verbose=True)

    @_master_action
    def master_stopdisk(self):
        self.sub_set_action("sync.btrfssnap", "stop")
        self.sub_set_action(DISK_TYPES, "stop", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    @_master_action
    def master_shutdowndisk(self):
        self.sub_set_action("sync.btrfssnap", "shutdown")
        self.sub_set_action(DISK_TYPES, "shutdown", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "shutdown", xtags=set(['zone']))

    def rollbackdisk(self):
        self.sub_set_action(DISK_TYPES, "rollback", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "rollback", xtags=set(['zone']))

    def abort_start(self):
        """
        Give a chance to all resources concerned by the action to voice up
        their rebutal of the action before it begins.
        """
        self.abort_start_done = True
        if rcEnv.sysname == "Windows":
            parallel = False
        else:
            try:
                from multiprocessing import Process
                parallel = True
                def wrapper(func):
                    if func():
                        sys.exit(1)
            except ImportError:
                parallel = False

        procs = {}
        for resource in self.get_resources():
            if resource.skip or resource.disabled:
                continue
            if not hasattr(resource, 'abort_start'):
                continue
            if not parallel:
                if resource.abort_start():
                    raise ex.excError("start aborted due to resource %s "
                                      "conflict" % resource.rid)
            else:
                proc = Process(target=wrapper, args=[resource.abort_start])
                proc.start()
                procs[resource.rid] = proc

        if parallel:
            err = []
            for rid, proc in procs.items():
                proc.join()
                if proc.exitcode > 0:
                    err.append(rid)
            if len(err) > 0:
                raise ex.excError("start aborted due to resource %s "
                                  "conflict" % ",".join(err))

    def startip(self):
        self.master_startip()
        self.slave_startip()

    @_slave_action
    def slave_startip(self):
        self.encap_cmd(['startip'], verbose=True)

    @_master_action
    def master_startstandbyip(self):
        self.sub_set_action("ip", "startstandby", xtags=set(['zone', 'docker']))

    @_master_action
    def master_startip(self):
        self.sub_set_action("ip", "start", xtags=set(['zone', 'docker']))

    def stopip(self):
        self.slave_stopip()
        self.master_stopip()

    @_slave_action
    def slave_stopip(self):
        self.encap_cmd(['stopip'], verbose=True)

    @_master_action
    def master_stopip(self):
        self.sub_set_action("ip", "stop", xtags=set(['zone', 'docker']))

    @_master_action
    def master_shutdownip(self):
        self.sub_set_action("ip", "shutdown", xtags=set(['zone', 'docker']))

    def rollbackip(self):
        self.sub_set_action("ip", "rollback", xtags=set(['zone', 'docker']))

    def startshare(self):
        self.master_startshare()
        self.slave_startshare()

    @_master_action
    def master_startshare(self):
        self.sub_set_action("share.nfs", "start")

    @_master_action
    def master_startstandbyshare(self):
        self.sub_set_action("share", "startstandby")

    @_slave_action
    def slave_startshare(self):
        self.encap_cmd(['startshare'], verbose=True)

    def stopshare(self):
        self.slave_stopshare()
        self.master_stopshare()

    @_master_action
    def master_stopshare(self):
        self.sub_set_action("share", "stop")

    @_master_action
    def master_shutdownshare(self):
        self.sub_set_action("share", "shutdown")

    @_slave_action
    def slave_stopshare(self):
        self.encap_cmd(['stopshare'], verbose=True)

    def rollbackshare(self):
        self.sub_set_action("share", "rollback")

    def startfs(self):
        self.master_startfs()
        self.slave_startfs()

    @_master_action
    def master_startfs(self):
        self.master_startdisk()
        self.sub_set_action("fs", "start", xtags=set(['zone']))

    @_master_action
    def master_startstandbyfs(self):
        self.master_startstandbydisk()
        self.sub_set_action("fs", "startstandby", xtags=set(['zone']))

    @_slave_action
    def slave_startfs(self):
        self.encap_cmd(['startfs'], verbose=True)

    def stopfs(self):
        self.slave_stopfs()
        self.master_stopfs()

    @_master_action
    def master_stopfs(self):
        self.sub_set_action("fs", "stop", xtags=set(['zone']))
        self.master_stopdisk()

    @_master_action
    def master_shutdownfs(self):
        self.sub_set_action("fs", "shutdown", xtags=set(['zone']))
        self.master_shutdowndisk()

    @_slave_action
    def slave_stopfs(self):
        self.encap_cmd(['stopfs'], verbose=True)

    def rollbackfs(self):
        self.sub_set_action("fs", "rollback", xtags=set(['zone']))
        self.rollbackdisk()

    def startcontainer(self):
        self.abort_start()
        self.master_startcontainer()

    @_master_action
    def master_startstandbycontainer(self):
        self.sub_set_action("container", "startstandby")
        self.refresh_ip_status()

    @_master_action
    def master_startcontainer(self):
        self.sub_set_action("container", "start")
        self.refresh_ip_status()

    def refresh_ip_status(self):
        """ Used after start/stop container because the ip resource
            status change after its own start/stop
        """
        for resource in self.get_resources("ip"):
            resource.status(refresh=True, restart=False)

    @_master_action
    def shutdowncontainer(self):
        self.sub_set_action("container", "shutdown")
        self.refresh_ip_status()

    @_master_action
    def stopcontainer(self):
        self.sub_set_action("container", "stop")
        self.refresh_ip_status()

    def rollbackcontainer(self):
        self.sub_set_action("container", "rollback")
        self.refresh_ip_status()

    def unprovision(self):
        self.sub_set_action("container", "unprovision")
        self.sub_set_action("fs", "unprovision", xtags=set(['zone']))
        self.sub_set_action("disk", "unprovision", xtags=set(['zone']))
        self.sub_set_action("ip", "unprovision", xtags=set(['zone', 'docker']))

    def provision(self):
        self.sub_set_action("ip", "provision", xtags=set(['zone', 'docker']))
        self.sub_set_action("disk", "provision", xtags=set(['zone']))
        self.sub_set_action("fs", "provision", xtags=set(['zone']))
        self.sub_set_action("container", "provision")
        self.push()

    def startapp(self):
        self.master_startapp()
        self.slave_startapp()

    @_slave_action
    def slave_startapp(self):
        self.encap_cmd(['startapp'], verbose=True)

    @_master_action
    def master_startstandbyapp(self):
        self.sub_set_action("app", "startstandby")

    @_master_action
    def master_startapp(self):
        self.sub_set_action("app", "start")

    def stopapp(self):
        self.slave_stopapp()
        self.master_stopapp()

    @_slave_action
    def slave_stopapp(self):
        self.encap_cmd(['stopapp'], verbose=True)

    @_master_action
    def master_stopapp(self):
        self.sub_set_action("app", "stop")

    @_master_action
    def master_shutdownapp(self):
        self.sub_set_action("app", "shutdown")

    def rollbackapp(self):
        self.sub_set_action("app", "rollback")

    def prstop(self):
        self.slave_prstop()
        self.master_prstop()

    @_slave_action
    def slave_prstop(self):
        self.encap_cmd(['prstop'], verbose=True)

    @_master_action
    def master_prstop(self):
        self.sub_set_action("disk.scsireserv", "scsirelease")

    def prstart(self):
        self.master_prstart()
        self.slave_prstart()

    @_slave_action
    def slave_prstart(self):
        self.encap_cmd(['prstart'], verbose=True)

    @_master_action
    def master_prstart(self):
        self.sub_set_action("disk.scsireserv", "scsireserv")

    def prstatus(self):
        self.sub_set_action("disk.scsireserv", "scsicheckreserv")

    def startstandby(self):
        self.master_startstandby()
        self.slave_startstandby()

    @_master_action
    def master_startstandby(self):
        self.master_startstandbyip()
        self.master_startstandbyfs()
        self.master_startstandbyshare()
        self.master_startstandbycontainer()
        self.master_startstandbyapp()

    @_slave_action
    def slave_startstandby(self):
        cmd = ['startstandby']
        for container in self.get_resources('container'):
            if not container.is_up() and \
               rcEnv.nodename not in container.always_on:
                # no need to try to startstandby the encap service on a
                # container we not activated
                continue
            try:
                self._encap_cmd(cmd, container, verbose=True)
            except ex.excError:
                self.log.error("container %s is not joinable to execute "
                               "action '%s'", container.name, ' '.join(cmd))
                raise

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

    def remote_postsync(self):
        """ Release the svc lock at this point because the
            waitlock timeout is long and we are done touching
            local data.

            Action triggered by a remote master node after
            sync_nodes and sync_drp. Typically make use of files
            received in var/.
            use a long waitlock timeout to give a chance to
            remote syncs to finish
        """
        self.svcunlock()
        for nodename in self.need_postsync:
            self.remote_action(nodename, 'postsync', waitlock=3600)

        self.need_postsync = set()

    def remote_action(self, nodename, action, waitlock=-1, sync=False,
                      verbose=True, action_mode=True):
        if self.options.cron:
            # the scheduler action runs forked. don't use the cmdworker
            # in this context as it may hang
            sync = True

        rcmd = [os.path.join(rcEnv.pathetc, self.svcname)]
        if self.options.debug:
            rcmd += ['--debug']
        if self.options.cluster and action_mode:
            rcmd += ['--cluster']
        if self.options.cron:
            rcmd += ['--cron']
        if waitlock >= 0:
            rcmd += ['--waitlock', str(waitlock)]
        rcmd += action.split()
        cmd = rcEnv.rsh.split() + [nodename] + rcmd
        if verbose:
            self.log.info("exec '%s' on node %s", ' '.join(rcmd), nodename)
        if sync:
            out, err, ret = justcall(cmd)
            return out, err, ret
        else:
            self.node.cmdworker.enqueue(cmd)

    def presync(self):
        """ prepare files to send to slave nodes in var/.
            Each resource can prepare its own set of files.
        """
        self.need_postsync = set()
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
        for rtype in rtypes:
            self.sub_set_action(rtype, "sync_nodes")
        self.remote_postsync()

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
        for rtype in rtypes:
            self.sub_set_action(rtype, "sync_drp")
        self.remote_postsync()

    def syncswap(self):
        self.sub_set_action("sync.netapp", "syncswap")
        self.sub_set_action("sync.symsrdfs", "syncswap")
        self.sub_set_action("sync.hp3par", "syncswap")
        self.sub_set_action("sync.nexenta", "syncswap")

    def sync_revert(self):
        self.sub_set_action("sync.hp3par", "sync_revert")

    def sync_resume(self):
        self.sub_set_action("sync.netapp", "sync_resume")
        self.sub_set_action("sync.symsrdfs", "sync_resume")
        self.sub_set_action("sync.hp3par", "sync_resume")
        self.sub_set_action("sync.dcsckpt", "sync_resume")
        self.sub_set_action("sync.nexenta", "sync_resume")

    def sync_quiesce(self):
        self.sub_set_action("sync.netapp", "sync_quiesce")
        self.sub_set_action("sync.nexenta", "sync_quiesce")

    def resync(self):
        self.stop()
        self.sync_resync()
        self.start()

    def sync_resync(self):
        self.sub_set_action("sync.netapp", "sync_resync")
        self.sub_set_action("sync.nexenta", "sync_resync")
        self.sub_set_action("sync.rados", "sync_resync")
        self.sub_set_action("sync.dds", "sync_resync")
        self.sub_set_action("sync.symclone", "sync_resync")
        self.sub_set_action("sync.symsnap", "sync_resync")
        self.sub_set_action("sync.ibmdssnap", "sync_resync")
        self.sub_set_action("sync.evasnap", "sync_resync")
        self.sub_set_action("sync.necismsnap", "sync_resync")
        self.sub_set_action("sync.dcssnap", "sync_resync")

    def sync_break(self):
        self.sub_set_action("sync.netapp", "sync_break")
        self.sub_set_action("sync.nexenta", "sync_break")
        self.sub_set_action("sync.hp3par", "sync_break")
        self.sub_set_action("sync.dcsckpt", "sync_break")
        self.sub_set_action("sync.symclone", "sync_break")
        self.sub_set_action("sync.symsnap", "sync_break")

    def sync_update(self):
        self.sub_set_action("sync.netapp", "sync_update")
        self.sub_set_action("sync.hp3par", "sync_update")
        self.sub_set_action("sync.hp3parsnap", "sync_update")
        self.sub_set_action("sync.nexenta", "sync_update")
        self.sub_set_action("sync.dcsckpt", "sync_update")
        self.sub_set_action("sync.dds", "sync_update")
        self.sub_set_action("sync.btrfssnap", "sync_update")
        self.sub_set_action("sync.zfssnap", "sync_update")
        self.sub_set_action("sync.s3", "sync_update")
        self.sub_set_action("sync.symclone", "sync_update")
        self.sub_set_action("sync.symsnap", "sync_update")
        self.sub_set_action("sync.ibmdssnap", "sync_update")

    def sync_full(self):
        self.sub_set_action("sync.dds", "sync_full")
        self.sub_set_action("sync.zfs", "sync_full")
        self.sub_set_action("sync.btrfs", "sync_full")
        self.sub_set_action("sync.s3", "sync_full")

    def sync_restore(self):
        self.sub_set_action("sync.s3", "sync_restore")

    def sync_split(self):
        self.sub_set_action("sync.symsrdfs", "sync_split")

    def sync_establish(self):
        self.sub_set_action("sync.symsrdfs", "sync_establish")

    def sync_verify(self):
        self.sub_set_action("sync.dds", "sync_verify")

    def print_config(self):
        """
        The 'print config' action entry point.
        Print the service configuration in the format specified by --format.
        """
        if self.options.format is not None:
            return self.print_config_data()
        self.node._print_config(self.paths.cf)

    def make_temp_config(self):
        """
        Copy the current service configuration file to a temporary
        location for edition.
        If the temp file already exists, propose the --discard
        or --recover options.
        """
        import shutil
        path = os.path.join(rcEnv.pathtmp, self.svcname+".conf.tmp")
        if os.path.exists(path):
            if self.options.recover:
                pass
            elif self.options.discard:
                shutil.copy(self.paths.cf, path)
            else:
                raise ex.excError("%s exists: service is already being edited. "
                                  "Set --discard to edit from the current "
                                  "configuration, or --recover to open the "
                                  "unapplied config" % path)
        else:
            shutil.copy(self.paths.cf, path)
        return path

    def edit_config(self):
        """
        Execute an editor on the service configuration file.
        When the editor exits, validate the new configuration file.
        If validation pass, install the new configuration,
        else keep the previous configuration in place and offer the
        user the --recover or --discard choices for its next edit
        config action.
        """
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        from rcUtilities import which
        if not which(editor):
            print("%s not found" % editor, file=sys.stderr)
            return 1
        path = self.make_temp_config()
        os.environ["LANG"] = "en_US.UTF-8"
        os.system(' '.join((editor, path)))
        results = self._validate_config(path=path)
        if results["errors"] == 0:
            import shutil
            shutil.copy(path, self.paths.cf)
            os.unlink(path)
        else:
            print("your changes were not applied because of the errors "
                  "reported above. you can use the edit config command "
                  "with --recover to try to fix your changes or with "
                  "--discard to restart from the live config")
        return results["errors"] + results["warnings"]

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
                except ex.excError:
                    return False
                if ret:
                    return True
        self.log.debug("nothing to sync for the service for now")
        return False

    def sched_sync_all(self):
        """
        The 'sync_all' scheduler task entrypoint.
        """
        data = self.skip_action("sync_all", deferred_write_timestamp=True)
        if len(data["keep"]) == 0:
            return
        self._sched_sync_all(data["keep"])

    @scheduler_fork
    def _sched_sync_all(self, sched_options):
        """
        Call the sync_all method of each sync resources that passed the
        scheduler constraints.
        """
        self.action("sync_all", rid=[o.section for o in sched_options])
        self.sched_write_timestamp(sched_options)

    def sync_all(self):
        """
        The 'sync all' action entrypoint.
        """
        if not self.can_sync(["sync"]):
            return
        if self.options.cron:
            self.sched_delay()
        self.presync()
        self.sub_set_action("sync.rsync", "sync_nodes")
        self.sub_set_action("sync.zfs", "sync_nodes")
        self.sub_set_action("sync.btrfs", "sync_nodes")
        self.sub_set_action("sync.docker", "sync_nodes")
        self.sub_set_action("sync.dds", "sync_nodes")
        self.sub_set_action("sync.rsync", "sync_drp")
        self.sub_set_action("sync.zfs", "sync_drp")
        self.sub_set_action("sync.btrfs", "sync_drp")
        self.sub_set_action("sync.docker", "sync_drp")
        self.sub_set_action("sync.dds", "sync_drp")
        self.sync_update()
        self.remote_postsync()

    def push_service_status(self):
        """
        The 'push_service_status' scheduler task and action entrypoint.

        This method returns early if called from an encapsulated agent, as
        the master agent is responsible for pushing the encapsulated
        status.
        """
        if self.encap:
            if not self.options.cron:
                self.log.info("push service status is disabled for encapsulated services")
            return
        if self.skip_action("push_service_status"):
            return
        self.task_push_service_status()

    @scheduler_fork
    def task_push_service_status(self):
        """
        Refresh and push the service status to the collector.
        """
        if self.options.cron:
            self.sched_delay()
        import rcSvcmon
        self.options.refresh = True
        rcSvcmon.svcmon_normal([self])

    def push_resinfo(self):
        """
        The 'push_resinfo' scheduler task and action entrypoint.
        """
        if self.skip_action("push_resinfo"):
            return
        self.task_push_resinfo()

    @scheduler_fork
    def task_push_resinfo(self):
        """
        Push the per-resource key/value pairs to the collector.
        """
        if self.options.cron:
            self.sched_delay()
        self.node.collector.call('push_resinfo', [self])

    def push_config(self):
        """
        The 'push_config' scheduler task entrypoint.
        """
        if self.skip_action("push_config"):
            return
        self.push()

    def create_var_subdir(self):
        """
        Create the service-dedicated subdir in <pathvar>.
        """
        var_d = os.path.join(rcEnv.pathvar, self.svcname)
        if not os.path.exists(var_d):
            os.makedirs(var_d)

    def autopush(self):
        """
        If the configuration file has been modified since the last push
        to the collector, call the push method.
        """
        if not self.collector_outdated():
            return
        self.log.handlers[1].setLevel(logging.CRITICAL)
        try:
            self.push()
        finally:
            self.log.handlers[1].setLevel(rcEnv.loglevel)

    @scheduler_fork
    def push(self):
        """
        The 'push' action entrypoint.
        Synchronize the configuration file between encap and master agent,
        then send the configuration to the collector.
        Finally update the last push on-disk timestamp.
        This action is skipped when run by an encapsulated agent.
        """
        if self.encap:
            return
        if self.options.cron:
            self.sched_delay()
        self.push_encap_config()
        self.node.collector.call('push_all', [self])
        self.log.info("send %s to collector", self.paths.cf)
        try:
            self.create_var_subdir()
            import time
            with open(self.paths.push_flag, 'w') as ofile:
                ofile.write(str(time.time()))
            self.log.info("update %s timestamp", self.paths.push_flag)
        except (OSError, IOError):
            self.log.error("failed to update %s timestamp", self.paths.push_flag)

    def push_encap_config(self):
        """
        Verify the service has an encapsulated part, and if so, for each
        container in up state running an encapsulated part, synchronize the
        service configuration file.
        """
        if self.encap or not self.has_encap_resources:
            return

        for resource in self.get_resources('container'):
            if resource.status(ignore_nostatus=True) not in (rcStatus.STDBY_UP, rcStatus.UP):
                continue
            self._push_encap_config(resource)

    def _push_encap_config(self, container):
        """
        Compare last modification time of the master and slave service
        configuration file, and copy the most recent version over the least
        recent.
        """
        cmd = ['print', 'config', 'mtime']
        try:
            cmd_results = self._encap_cmd(cmd, container)
            out = cmd_results[0]
            ret = cmd_results[2]
        except ex.excError:
            out = None
            ret = 1

        paths = get_osvc_paths(osvc_root_path=container.osvc_root_path,
                               sysname=container.guestos)
        encap_cf = os.path.join(paths.pathetc, os.path.basename(self.paths.cf))

        if out == "":
            # this is what happens when the container is down
            return

        if ret == 0:
            encap_mtime = int(float(out.strip()))
            local_mtime = int(os.stat(self.paths.cf).st_mtime)
            if encap_mtime > local_mtime:
                if hasattr(container, 'rcp_from'):
                    cmd_results = container.rcp_from(encap_cf, rcEnv.pathetc+'/')
                else:
                    cmd = rcEnv.rcp.split() + [container.name+':'+encap_cf, rcEnv.pathetc+'/']
                    cmd_results = justcall(cmd)
                os.utime(self.paths.cf, (encap_mtime, encap_mtime))
                self.log.info("fetch %s from %s", encap_cf, container.name)
                if cmd_results[2] != 0:
                    raise ex.excError()
                return
            elif encap_mtime == local_mtime:
                return

        if hasattr(container, 'rcp'):
            cmd_results = container.rcp(self.paths.cf, encap_cf)
        else:
            cmd = rcEnv.rcp.split() + [self.paths.cf, container.name+':'+encap_cf]
            cmd_results = justcall(cmd)
        if cmd_results[2] != 0:
            raise ex.excError("failed to send %s to %s" % (self.paths.cf, container.name))
        self.log.info("send %s to %s", self.paths.cf, container.name)

        cmd = ['create', '--config', encap_cf]
        cmd_results = self._encap_cmd(cmd, container=container)
        if cmd_results[2] != 0:
            raise ex.excError("failed to create %s slave service" % container.name)
        self.log.info("create %s slave service", container.name)

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

    def setup_environ(self, action=None):
        """
        Setup envionment variables.
        Startup scripts and triggers can use them, so their code can be
        more generic.
        All resources can contribute a set of env variables through their
        own setup_environ() method.
        """
        os.environ['OPENSVC_SVCNAME'] = self.svcname
        if action:
            os.environ['OPENSVC_ACTION'] = action
        for resource in self.get_resources():
            resource.setup_environ()

    def expand_rid(self, rid):
        """
        Given a rid return a set containing either the rid itself if it is
        a known rid, or containing the rid of all resources whose prefix
        matches the name given as rid.
        """
        retained_rids = set()
        for _rid in self.resources_by_id.keys():
            if _rid is None:
                continue
            if '#' not in _rid:
                if _rid == rid:
                    retained_rids.add(_rid)
                else:
                    continue
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
            if '#' in rid:
                if rid not in self.resources_by_id:
                    continue
                retained_rids.add(rid)
                continue
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
        if len(subsets) == 0 or subsets is None:
            return
        retained_rids = set()
        for resource in self.resources_by_id.values():
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
            for resource in self.resources_by_id.values():
                if set(intersection) & resource.tags == set(intersection):
                    retained_rids.add(resource.rid)
        if len(retained_rids) > 0:
            self.log.debug("rids added from --tags %s: %s", ",".join(tags),
                           ",".join(retained_rids))
        return retained_rids

    @staticmethod
    def action_translate(action):
        """
        Return the supported action name corresponding to the specified
        action. Deprecated actions are thus translated into their supported
        action name.
        """
        if action in ACTIONS_TRANSLATIONS:
            return ACTIONS_TRANSLATIONS[action]
        return action

    def always_on_resources(self):
        """
        Return the list of resources flagged always on on this node
        """
        return [resource for resource in self.resources_by_id.values()
                if rcEnv.nodename in resource.always_on]

    def action(self, *args, **kwargs):
        """
        The service action main entrypoint.
        Handle the run file flag creation after the action is done,
        whatever its status.
        """
        try:
            return self._action(*args, **kwargs)
        finally:
            if args[0] != "scheduler":
                self.set_run_flag()

    def _action(self, *args, **kwargs):
        """
        Filter resources on which the service action must act.
        Abort if the service is frozen, or if --cluster is not set on a HA
        service.
        Set up the environment variables.
        Finally do the service action either in logged or unlogged mode.
        """
        action = args[0]
        rid = kwargs.get("rid", [])
        tags = kwargs.get("tags", [])
        subsets = kwargs.get("subsets", [])
        xtags = kwargs.get("xtags", set())
        waitlock = kwargs.get("waitlock", -1)

        if rid is None:
            rid = []
        if tags is None:
            tags = []
        if subsets is None:
            subsets = []
        if xtags is None:
            xtags = set()
        if waitlock < 0:
            waitlock = self.lock_timeout

        if len(self.resources_by_id.keys()) > 0:
            rids = set(self.resources_by_id.keys()) - set([None])

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

            rids = list(rids)
            self.log.debug("rids retained after expansions intersection: %s",
                           ";".join(rids))

            if not self.options.slaves and self.options.slave is None and \
               len(set(rid) | set(subsets) | set(tags)) > 0 and len(rids) == 0:
                self.log.error("no resource match the given --rid, --subset and "
                               "--tags specifiers")
                return 1
        else:
            # no resources certainly mean the build was done with minimal=True
            # let the action go on. 'delete', for one, takes a --rid but does
            # not need resource initialization
            rids = rid

        self.action_rid = rids
        if self.node is None:
            self.node = node.Node()
        self.action_start_date = datetime.datetime.now()
        if self.svc_env != 'PRD' and rcEnv.node_env == 'PRD':
            self.log.error("Abort action for non PRD service on PRD node")
            return 1

        action = self.action_translate(action)


        if action not in ACTIONS_ALLOW_ON_FROZEN and \
           'compliance' not in action and \
           'collector' not in action:
            if self.frozen() and not self.options.force:
                self.log.info("Abort action '%s' for frozen service. Use "
                              "--force to override.", action)
                return 1

            if action == "boot" and len(self.always_on_resources()) == 0 and \
               len(self.get_resources('hb')) > 0:
                self.log.info("end boot action on cluster node before "
                              "acquiring the action lock: no stdby resource "
                              "needs activation.")
                return 0

            try:
                self.cluster_mode_safety_net(action)
            except ex.excAbortAction as exc:
                self.log.info(str(exc))
                return 0
            except ex.excEndAction as exc:
                self.log.info(str(exc))
                return 0
            except ex.excError as exc:
                self.log.error(str(exc))
                return 1
            #
            # here we know we will run a resource state-changing action
            # purge the resource status file cache, so that we don't take
            # decision on outdated information
            #
            if not self.options.dry_run and action != "resource_monitor":
                self.log.debug("purge all resource status file caches")
                self.purge_status_last()

        self.setup_environ(action=action)
        self.setup_signal_handlers()
        self.set_skip_resources(keeprid=rids, xtags=xtags)
        if action.startswith("print_") or \
           action.startswith("collector") or \
           action.startswith("json_"):
            return self.do_print_action(action)
        if action in ACTIONS_NO_LOG or \
           action.startswith("compliance") or \
           action.startswith("docker") or \
           self.options.dry_run:
            err = self.do_action(action, waitlock=waitlock)
        else:
            err = self.do_logged_action(action, waitlock=waitlock)
        return err

    def do_print_action(self, action):
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

        if "_json_" in action:
            action = action.replace("_json_", "_")
            self.node.options.format = "json"
            self.options.format = "json"

        if self.options.cluster and self.options.format != "json":
            raise ex.excError("only the json output format is allowed with --cluster")
        if action.startswith("collector_"):
            from collector import Collector
            collector = Collector(self.options, self.node, self.svcname)
            func = getattr(collector, action)
        else:
            func = getattr(self, action)

        if not hasattr(func, "__call__"):
            raise ex.excError("%s is not callable" % action)

        psinfo = self.do_cluster_action(_action, collect=True, action_mode=False)

        try:
            data = func()
        except Exception as exc:
            data = {"error": str(exc)}

        if psinfo:
            # --cluster is set and we have remote responses
            results = self.join_cluster_action(**psinfo)
            for nodename in results:
                results[nodename] = results[nodename][0]
                if self.options.format == "json":
                    import json
                    try:
                        results[nodename] = json.loads(results[nodename])
                    except ValueError as exc:
                        results[nodename] = {"error": str(exc)}
            results[rcEnv.nodename] = data
            return results
        elif self.options.cluster:
            # no remote though --cluster is set
            results = {}
            results[rcEnv.nodename] = data
            return results

        return data

    def do_cluster_action(self, action, waitlock=60, collect=False, action_mode=True):
        """
        Execute an action on remote nodes if --cluster is set and the
        service is a flex, and this node is flex primary.

        edit config, validate config, and sync* are never executed through
        this method.

        If possible execute in parallel running subprocess. Aggregate and
        return results.
        """
        if not self.options.cluster:
            return

        if action in ("edit_config", "validate_config") or "sync" in action:
            return

        if action_mode and "flex" not in self.clustertype:
            return

        if "flex" in self.clustertype:
            if rcEnv.nodename == self.drp_flex_primary:
                peers = set(self.drpnodes) - set([rcEnv.nodename])
            elif rcEnv.nodename == self.flex_primary:
                peers = set(self.nodes) - set([rcEnv.nodename])
            else:
                return
        elif not action_mode:
            if rcEnv.nodename in self.nodes:
                peers = set(self.nodes) | set(self.drpnodes)
            else:
                peers = set(self.drpnodes)
            peers -= set([rcEnv.nodename])

        args = [arg for arg in sys.argv[1:] if arg != "--cluster"]
        if self.options.docker_argv and len(self.options.docker_argv) > 0:
            args += self.options.docker_argv

        def wrapper(queue, **kwargs):
            """
            Execute the remote action and enqueue or print results.
            """
            collect = kwargs["collect"]
            del kwargs["collect"]
            out, err, ret = self.remote_action(**kwargs)
            if collect:
                queue.put([out, err, ret])
            else:
                if len(out):
                    print(out)
                if len(err):
                    print(err)
            return out, err, ret

        if rcEnv.sysname == "Windows":
            parallel = False
        else:
            try:
                from multiprocessing import Process, Queue
                parallel = True
                results = None
                procs = {}
                queues = {}
            except ImportError:
                parallel = False
                results = {}
                procs = None
                queues = None

        for nodename in peers:
            kwargs = {
                "nodename": nodename,
                "action": " ".join(args),
                "waitlock": waitlock,
                "verbose": False,
                "sync": True,
                "action_mode": action_mode,
                "collect": collect,
            }
            if parallel:
                queues[nodename] = Queue()
                proc = Process(target=wrapper, args=(queues[nodename],), kwargs=kwargs)
                proc.start()
                procs[nodename] = proc
            else:
                results[nodename] = wrapper(**kwargs)
        return {"procs": procs, "queues": queues, "results": results}

    @staticmethod
    def join_cluster_action(procs=None, queues=None, results=None):
        """
        Wait for subprocess to finish, aggregate and return results.
        """
        if procs is None or queues is None:
            return results
        results = {}
        joined = []
        while len(joined) < len(procs):
            for nodename, proc in procs.items():
                proc.join(0.1)
                if not proc.is_alive():
                    joined.append(nodename)
                queue = queues[nodename]
                if not queue.empty():
                    results[nodename] = queue.get()
        return results

    def do_action(self, action, waitlock=60):
        """
        Acquire the service action lock, call the service action method,
        handles its errors, and finally release the lock.

        If --cluster is set, and the service is a flex, and we are
        flex_primary run the action on all remote nodes.
        """

        err = 0
        try:
            self.svclock(action, timeout=waitlock)
        except ex.excError as exc:
            self.log.error(str(exc))
            return 1

        psinfo = self.do_cluster_action(action, waitlock=waitlock)

        try:
            if action.startswith("compliance_"):
                from compliance import Compliance
                compliance = Compliance(self)
                err = getattr(compliance, action)()
            elif hasattr(self, action):
                self.running_action = action
                err = getattr(self, action)()
                if err is None:
                    err = 0
            else:
                self.log.error("unsupported action %s", action)
                err = 1
        except ex.excEndAction as exc:
            msg = "'%s' action ended by last resource" % action
            if len(str(exc)) > 0:
                msg += ": %s" % str(exc)
            self.log.info(msg)
            err = 0
        except ex.excAbortAction as exc:
            msg = "'%s' action aborted by last resource" % action
            if len(str(exc)) > 0:
                msg += ": %s" % str(exc)
            self.log.info(msg)
            err = 0
        except ex.excError as exc:
            msg = "'%s' action stopped on execution error" % action
            if len(str(exc)) > 0:
                msg += ": %s" % str(exc)
            self.log.error(msg)
            err = 1
            self.rollback_handler(action)
        except ex.excSignal:
            self.log.error("interrupted by signal")
            err = 1
        except ex.MonitorAction:
            self.svcunlock()
            raise
        except:
            err = 1
            self.save_exc()
        finally:
            self.running_action = None

        self.svcunlock()

        if action == "start" and self.options.cluster and self.ha:
            # This situation is typical of a hb-initiated service start.
            # While the hb starts the service, its resource status is warn from
            # opensvc point of view. So after a successful startup, the hb res
            # status would stay warn until the next svcmon.
            # To avoid this drawback we can force from here the hb status.
            if err == 0:
                for resource in self.get_resources(['hb']):
                    if resource.disabled:
                        continue
                    resource.force_status(rcStatus.UP)

        if psinfo:
            self.join_cluster_action(**psinfo)

        return err

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
        rids = [r.rid for r in self.get_resources() if r.can_rollback and not r.always_on]
        if len(rids) == 0:
            self.log.info("skip rollback %s: no resource activated", action)
            return
        self.log.info("trying to rollback %s on %s", action, ', '.join(rids))
        try:
            self.rollback()
        except ex.excError:
            self.log.error("rollback %s failed", action)

    def do_logged_action(self, action, waitlock=60):
        """
        Setup action logging to a machine-readable temp logfile, in preparation
        to the collector feeding.
        Do the action.
        Finally, feed the log to the collector.
        """
        import tempfile
        begin = datetime.datetime.now()

        # Provision a database entry to store action log later
        if action in ('postsync', 'shutdown'):
            # don't loose the action log on node shutdown
            # no background dblogger for remotely triggered postsync
            self.sync_dblogger = True
        self.node.collector.call('begin_action', self, action, begin,
                                 sync=self.sync_dblogger)

        # Per action logfile to push to database at the end of the action
        tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.pathtmp,
                                              prefix=self.svcname+'.'+action)
        actionlogfile = tmpfile.name
        tmpfile.close()
        log = logging.getLogger()
        fmt = "%(asctime)s;;%(name)s;;%(levelname)s;;%(message)s;;%(process)d;;EOL"
        actionlogformatter = logging.Formatter(fmt)
        actionlogfilehandler = logging.FileHandler(actionlogfile)
        actionlogfilehandler.setFormatter(actionlogformatter)
        actionlogfilehandler.setLevel(logging.INFO)
        log.addHandler(actionlogfilehandler)
        if "/svcmgr.py" in sys.argv:
            self.log.info(" ".join(sys.argv))

        err = self.do_action(action, waitlock=waitlock)

        # Push result and logs to database
        actionlogfilehandler.close()
        log.removeHandler(actionlogfilehandler)
        end = datetime.datetime.now()
        self.dblogger(action, begin, end, actionlogfile)
        return err

    def restart(self):
        """
        The 'restart' action entrypoint.
        This action translates into 'stop' followed by 'start'
        """
        self.stop()
        self.start()

    def _migrate(self):
        """
        Call the migrate action on all relevant resources.
        """
        self.sub_set_action("container.ovm", "_migrate")
        self.sub_set_action("container.hpvm", "_migrate")
        self.sub_set_action("container.esx", "_migrate")

    def destination_node_sanity_checks(self):
        """
        Raise an excError if
        * the destination node --to arg not set
        * the specified destination is the current node
        * the specified destination is not a service candidate node
        """
        if self.options.destination_node is None:
            raise ex.excError("a destination node must be provided this action")
        if self.options.destination_node == rcEnv.nodename:
            raise ex.excError("the destination is the source node")
        if self.options.destination_node not in self.nodes:
            raise ex.excError("the destination node %s is not in the service "
                              "nodes list" % self.options.destination_node)

    @_master_action
    def migrate(self):
        """
        Service online migration.
        """
        self.destination_node_sanity_checks()
        self.master_prstop()
        try:
            self.remote_action(nodename=self.options.destination_node, action='startfs --master')
            self._migrate()
        except:
            if self.has_resourceset(['disk.scsireserv']):
                self.log.error("scsi reservations were dropped. you have to "
                               "acquire them now using the 'prstart' action "
                               "either on source node or destination node, "
                               "depending on your problem analysis.")
            raise
        self.master_stopfs()
        self.remote_action(nodename=self.options.destination_node, action='prstart --master')

    def switch(self):
        """
        Service move to another node.
        """
        self.destination_node_sanity_checks()
        self.sub_set_action("hb", "switch")
        self.stop()
        self.remote_action(nodename=self.options.destination_node, action='start')

    def collector_outdated(self):
        """
        Return True if the configuration file has changed since last push.
        """
        if self.encap:
            return False

        if not os.path.exists(self.paths.push_flag):
            self.log.debug("no last push timestamp found")
            return True
        try:
            mtime = os.stat(self.paths.cf).st_mtime
            with open(self.paths.push_flag) as flag:
                last_push = float(flag.read())
        except (ValueError, IOError, OSError):
            self.log.error("can not read timestamp from %s or %s",
                           self.paths.cf, self.paths.push_flag)
            return True
        if mtime > last_push:
            self.log.debug("configuration file changed since last push")
            return True
        return False

    def write_config(self):
        """
        Rewrite the service configuration file, using the current parser
        object in self.config write method.
        Also reset the file mode to 644.
        """
        import tempfile
        import shutil
        try:
            tmpfile = tempfile.NamedTemporaryFile()
            fname = tmpfile.name
            tmpfile.close()
            with open(fname, "w") as tmpfile:
                self.config.write(tmpfile)
            shutil.move(fname, self.paths.cf)
        except (OSError, IOError) as exc:
            print("failed to write new %s (%s)" % (self.paths.cf, str(exc)),
                  file=sys.stderr)
            raise ex.excError()
        try:
            os.chmod(self.paths.cf, 0o0644)
        except (OSError, IOError) as exc:
            self.log.debug("failed to set %s mode: %s", self.paths.cf, str(exc))

    def load_config(self):
        """
        Initialize the service configuration parser object. Using an
        OrderDict type to preserve the options and sections ordering,
        if possible.

        The parser object is a opensvc-specified class derived from
        optparse.RawConfigParser.
        """
        try:
            from collections import OrderedDict
            self.config = RawConfigParser(dict_type=OrderedDict)
        except ImportError:
            self.config = RawConfigParser()
        self.config.read(self.paths.cf)

    def unset(self):
        """
        The 'unset' action entrypoint.
        Verifies the --param and --value are set, set DEFAULT as section
        if no section was specified, and finally call the _unset internal
        method.
        """
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = elements
        try:
            self._unset(section, option)
            return 0
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            return 1

    def _unset(self, section, option):
        """
        Delete an option in the service configuration file specified section.
        """
        section = "[%s]" % section
        lines = self._read_cf().splitlines()

        need_write = False
        in_section = False
        for i, line in enumerate(lines):
            sline = line.strip()
            if sline == section:
                in_section = True
            elif in_section:
                if sline.startswith("["):
                    break
                elif "=" in sline:
                    elements = sline.split("=")
                    _option = elements[0].strip()
                    if option != _option:
                        continue
                    del lines[i]
                    need_write = True
                    while i < len(lines) and "=" not in lines[i] and \
                          not lines[i].strip().startswith("[") and \
                          lines[i].strip() != "":
                        del lines[i]

        if not in_section:
            raise ex.excError("section %s not found" % section)

        if not need_write:
            raise ex.excError("option '%s' not found in section %s" % (option, section))

        buff = "\n".join(lines)

        try:
            self._write_cf(buff)
        except (IOError, OSError) as exc:
            raise ex.excError(str(exc))

    def get(self):
        """
        The 'get' action entrypoint.
        Verifies the --param and --value are set, set DEFAULT as section
        if no section was specified, and finally,
        * print the raw value if --eval is not set
        * print the dereferenced and evaluated value if --eval is set
        """
        self.load_config()
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = elements
        if section != 'DEFAULT' and not self.config.has_section(section):
            print("section [%s] not found"%section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section [%s]"%(option, section), file=sys.stderr)
            return 1
        if self.options.eval:
            from svcBuilder import conf_get
            print(conf_get(self, self.config, section, option, "string", scope=True))
        else:
            print(self.config.get(section, option))
        return 0

    def set(self):
        """
        The 'set' action entrypoint.
        Verifies the --param and --value are set, set DEFAULT as section
        if no section was specified, and set the value using the internal
        _set() method.
        """
        self.load_config()
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        if self.options.value is None:
            print("no value. set --value", file=sys.stderr)
            return 1
        elements = self.options.param.split('.')
        if len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        try:
            self._set(elements[0], elements[1], self.options.value)
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            return 1
        return 0

    def setenv(self, args, interactive=False):
        """
        For each option in the 'env' section of the configuration file,
        * rewrite the value using the value specified in a corresponding
          --env <option>=<value> commandline arg
        * or prompt for the value if --interactive is set, and rewrite
        * or leave the value as is, considering the default is accepted
        """
        explicit_options = []

        for arg in args:
            idx = arg.index("=")
            option = arg[:idx]
            value = arg[idx+1:]
            self._set("env", option, value)
            explicit_options.append(option)

        if not interactive:
            return

        if not os.isatty(0):
            raise ex.excError("--interactive is set but input fd is not a tty")

        for key, default_val in self.env_section_keys().items():
            if key.endswith(".comment"):
                continue
            if key in explicit_options:
                continue
            if self.config.has_option("env", key+".comment"):
                print(self.config.get("env", key+".comment"))
            newval = raw_input("%s [%s] > " % (key, str(default_val)))
            if newval != "":
                self._set("env", key, newval)

    def _set(self, section, option, value):
        """
        Set <option> to <value> in <section> of the configuration file.
        """
        section = "[%s]" % section
        lines = self._read_cf().splitlines()
        done = False
        in_section = False

        for idx, line in enumerate(lines):
            sline = line.strip()
            if sline == section:
                in_section = True
            elif in_section:
                if sline.startswith("[") and not done:
                    # section found and parsed and no option => add option
                    section_idx = idx
                    while section_idx > 0 and lines[section_idx-1].strip() == "":
                        section_idx -= 1
                    lines.insert(section_idx, "%s = %s" % (option, value))
                    done = True
                    break
                elif "=" in sline:
                    elements = sline.split("=")
                    _option = elements[0].strip()

                    if option != _option:
                        continue

                    if done:
                        # option already set : remove dup
                        del lines[idx]
                        while idx < len(lines) and "=" not in lines[idx] and \
                              not lines[idx].strip().startswith("[") and \
                              lines[idx].strip() != "":
                            del lines[idx]
                        continue

                    _value = elements[1].strip()
                    section_idx = idx

                    while section_idx < len(lines)-1 and  \
                          "=" not in lines[section_idx+1] and \
                          not lines[section_idx+1].strip().startswith("["):
                        section_idx += 1
                        if lines[section_idx].strip() == "":
                            continue
                        _value += " %s" % lines[section_idx].strip()

                    if value.replace("\n", " ") == _value:
                        return

                    lines[idx] = "%s = %s" % (option, value)
                    section_idx = idx

                    while section_idx < len(lines)-1 and \
                          "=" not in lines[section_idx+1] and \
                          not lines[section_idx+1].strip().startswith("[") and \
                          lines[section_idx+1].strip() != "":
                        del lines[section_idx+1]

                    done = True

        if not done:
            while lines[-1].strip() == "":
                lines.pop()
            if not in_section:
                # section in last position and no option => add section
                lines.append("")
                lines.append(section)
            lines.append("%s = %s" % (option, value))

        buff = "\n".join(lines)

        try:
            self._write_cf(buff)
        except (IOError, OSError) as exc:
            raise ex.excError(str(exc))

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

        for rid in rids:
            if rid != 'DEFAULT' and not self.config.has_section(rid):
                self.log.error("service %s has no resource %s", self.svcname, rid)
                continue
            self.log.info("set %s.disable = %s", rid, str(disable))
            self.config.set(rid, "disable", str(disable).lower())

        #
        # if we set DEFAULT.disable = True,
        # we don't want res#n.disable = False
        #
        if rids == ["DEFAULT"] and disable:
            for section in self.config.sections():
                if self.config.has_option(section, "disable") and \
                   not self.config.getboolean(section, "disable"):
                    self.log.info("remove %s.disable = false", section)
                    self.config.remove_option(section, "disable")

        try:
            self.write_config()
        except (IOError, OSError) as exc:
            self.log.error(str(exc))
            return 1

        return 0

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
        if len(self.action_rid) in (0, len(self.resources_by_id.keys())):
            import shutil
            dpaths = [
                os.path.join(rcEnv.pathetc, self.svcname+".dir"),
                os.path.join(rcEnv.pathetc, self.svcname+".d"),
            ]
            fpaths = [
                self.paths.cf,
                os.path.join(rcEnv.pathetc, self.svcname),
                os.path.join(rcEnv.pathetc, self.svcname+".d"),
                os.path.join(rcEnv.pathetc, self.svcname+".cluster"),
                os.path.join(rcEnv.pathetc, self.svcname+".stonith"),
            ]
            for fpath in fpaths:
                if os.path.exists(fpath) and \
                   (os.path.islink(fpath) or os.path.isfile(fpath)):
                    self.log.info("remove %s", fpath)
                    os.unlink(fpath)
            for dpath in dpaths:
                if os.path.exists(dpath):
                    self.log.info("remove %s", dpath)
                    shutil.rmtree(dpath)
            return 0
        lines = self._read_cf().splitlines()
        need_write = False

        for rid in self.action_rid:
            section = "[%s]" % rid
            in_section = False
            for i, line in enumerate(lines):
                sline = line.strip()
                if sline == section:
                    in_section = True
                    need_write = True
                    del lines[i]
                    while i < len(lines) and not lines[i].strip().startswith("["):
                        del lines[i]

            if not in_section:
                print("service", self.svcname, "has no resource", rid, file=sys.stderr)

        if not need_write:
            return 0

        buff = "\n".join(lines)

        try:
            self._write_cf(buff)
        except (IOError, OSError):
            print("failed to rewrite", self.paths.cf, file=sys.stderr)
            return 1
        return 0

    def docker(self):
        """
        The 'docker' action entry point.
        Parse the docker argv and substitute known patterns before relaying
        the argv to the docker command.
        Set the socket to point the service-private docker daemon if
        the service has such a daemon.
        """
        import subprocess
        containers = self.get_resources('container')
        if self.options.docker_argv is None:
            print("no docker command arguments supplied", file=sys.stderr)
            return 1

        def subst(argv):
            """
            Parse the docker argv and substitute known patterns.
            """
            import re
            for idx, arg in enumerate(argv):
                if arg in ("%instances%", "{instances}"):
                    del argv[idx]
                    instances = [resource.container_name for resource in containers
                                 if not resource.skip and not resource.disabled]
                    for instance in instances:
                        argv.insert(idx, instance)
            for idx, arg in enumerate(argv):
                if arg in ("%images%", "{images}"):
                    del argv[idx]
                    images = list(set([resource.run_image for resource in containers
                                       if not resource.skip and not resource.disabled]))
                    for image in images:
                        argv.insert(idx, image)
            for idx, arg in enumerate(argv):
                if arg in ("%as_service%", "{as_service}"):
                    del argv[idx]
                    argv[idx:idx] = ["-u", self.svcname+"@"+rcEnv.nodename]
                    argv[idx:idx] = ["-p", self.node.config.get("node", "uuid")]
                    if len(containers) > 0:
                        if containers[0].docker_min_version("1.12"):
                            pass
                        elif containers[0].docker_min_version("1.11"):
                            argv[idx:idx] = ["--email", ""]
            for idx, arg in enumerate(argv):
                if re.match(r'\{container#\w+\}', arg):
                    container_name = self.svcname + "." + arg.strip("{}").replace("#", ".")
                    del argv[idx]
                    argv.insert(idx, container_name)
            return argv

        for container in containers:
            if hasattr(container, "docker_cmd"):
                container.docker_start(verbose=False)
                cmd = container.docker_cmd + subst(self.options.docker_argv)
                proc = subprocess.Popen(cmd)
                proc.communicate()
                return proc.returncode
        print("this service has no docker resource", file=sys.stderr)
        return 1

    def freeze(self):
        """
        Call the freeze method of hb resources, then set the frozen flag.
        """
        for resource in self.get_resources("hb"):
            resource.freeze()
        self.freezer.freeze()

    def thaw(self):
        """
        Call the thaw method of hb resources, then unset the frozen flag.
        """
        for resource in self.get_resources("hb"):
            resource.thaw()
        self.freezer.thaw()

    def frozen(self):
        """
        Return True if the service is frozen.
        """
        return self.freezer.frozen()

    def pull(self):
        """
        Pull a service configuration from the collector, installs it and
        create the svcmgr link.
        """
        self.node.pull_service(self.svcname)

    def validate_config(self, path=None):
        """
        The validate config action entrypoint.
        """
        ret = self._validate_config(path=path)
        return ret["warnings"] + ret["errors"]

    def _validate_config(self, path=None):
        """
        The validate config core method.
        Returns a dict with the list of syntax warnings and errors.
        """
        from svcDict import KeyDict, deprecated_sections
        from svcBuilder import build, handle_references
        from rcUtilities import convert_size

        data = KeyDict(provision=True)
        ret = {
            "errors": 0,
            "warnings": 0,
        }

        if path is None:
            config = self.config
        else:
            config = RawConfigParser()
            config.read(path)

        def check_scoping(key, section, option):
            """
            Verify the specified option scoping is allowed.
            """
            if not key.at and "@" in option:
                self.log.error("option %s.%s does not support scoping", section, option)
                return 1
            return 0

        def check_references(section, option):
            """
            Verify the specified option references.
            """
            value = config.get(section, option)
            try:
                value = handle_references(self, config, value, scope=True)
            except ex.excError as exc:
                if not option.startswith("pre_") and \
                   not option.startswith("post_") and \
                   not option.startswith("blocking_"):
                    self.log.error(str(exc))
                    return 1
            except Exception as exc:
                self.log.error(str(exc))
                return 1
            return 0

        def get_val(key, section, option):
            """
            Fetch the value and convert it to expected type.
            """
            value = config.get(section, option)
            if isinstance(key.default, bool):
                return bool(value)
            elif isinstance(key.default, int):
                try:
                    return int(value)
                except ValueError:
                    # might be a size string like 11mib
                    return convert_size(value)
            return value

        def check_candidates(key, section, option, value):
            """
            Verify the specified option value is in allowed candidates.
            """
            if key.strict_candidates and key.candidates and value not in key.candidates:
                if isinstance(key.candidates, (set, list, tuple)):
                    candidates = ", ".join(key.candidates)
                else:
                    candidates = str(key.candidates)
                self.log.error("option %s.%s value %s is not in valid candidates: %s",
                               section, option, str(value), candidates)
                return 1
            return 0

        def check_known_option(key, section, option):
            """
            Verify the specified option scoping, references and that the value
            is in allowed candidates.
            """
            err = 0
            err += check_scoping(key, section, option)
            if check_references(section, option) != 0:
                err += 1
                return err
            value = get_val(key, section, option)
            err += check_candidates(key, section, option, value)
            return err

        def validate_default_options(config, data, ret):
            """
            Validate DEFAULT section options.
            """
            for option in config.defaults():
                key = data.sections["DEFAULT"].getkey(option)
                if key is None:
                    found = False
                    # the option can be set in the DEFAULT section for the
                    # benefit of a resource section
                    for section in config.sections():
                        family = section.split("#")[0]
                        if family not in list(data.sections.keys()) + \
                           list(deprecated_sections.keys()):
                            continue
                        if family in deprecated_sections:
                            results = deprecated_sections[family]
                            family = results[0]
                        if data.sections[family].getkey(option) is not None:
                            found = True
                            break
                    if not found:
                        self.log.warning("ignored option DEFAULT.%s", option)
                        ret["warnings"] += 1
                else:
                    # here we know its a native DEFAULT option
                    ret["errors"] += check_known_option(key, "DEFAULT", option)
            return ret

        def validate_resources_options(config, data, ret):
            """
            Validate resource sections options.
            """
            for section in config.sections():
                if section == "env":
                    # the "env" section is not handled by a resource driver, and is
                    # unknown to the svcDict. Just ignore it.
                    continue
                family = section.split("#")[0]
                if config.has_option(section, "type"):
                    rtype = config.get(section, "type")
                else:
                    rtype = None
                if family not in list(data.sections.keys()) + list(deprecated_sections.keys()):
                    self.log.warning("ignored section %s", section)
                    ret["warnings"] += 1
                    continue
                if family in deprecated_sections:
                    self.log.warning("deprecated section prefix %s", family)
                    ret["warnings"] += 1
                    family, rtype = deprecated_sections[family]
                for option in config.options(section):
                    if option in config.defaults():
                        continue
                    key = data.sections[family].getkey(option, rtype=rtype)
                    if key is None:
                        key = data.sections[family].getkey(option)
                    if key is None:
                        self.log.warning("ignored option %s.%s, driver %s", section,
                                         option, rtype if rtype else "generic")
                        ret["warnings"] += 1
                    else:
                        ret["errors"] += check_known_option(key, section, option)
            return ret

        def validate_build(path, ret):
            """
            Try a service build to catch errors missed in other tests.
            """
            try:
                build(self.svcname, svcconf=path)
            except Exception as exc:
                self.log.error("the new configuration causes the following "
                               "build error: %s", str(exc))
                ret["errors"] += 1
            return ret

        ret = validate_default_options(config, data, ret)
        ret = validate_resources_options(config, data, ret)
        ret = validate_build(path, ret)

        return ret

    def has_run_flag(self):
        """
        Return True if the run flag is set or if the run flag dir does not
        exist.
        """
        flag_d = os.path.dirname(self.paths.run_flag)
        if not os.path.exists(flag_d):
            return True
        if os.path.exists(self.paths.run_flag):
            return True
        return False

    def set_run_flag(self):
        """
        Create the /var/run/opensvc.<svcname> flag if and /var/run exists,
        and if the flag does not exist yet.

        This flag absence inhibit the service scheduler.

        A known issue with scheduled tasks during init is the 'monitor vs
        boot' lock contention.
        """
        flag_d = os.path.dirname(self.paths.run_flag)
        if not os.path.exists(flag_d):
            self.log.debug("%s does not exists", flag_d)
            return
        if os.path.exists(self.paths.run_flag):
            self.log.debug("%s already exists", self.paths.run_flag)
            return
        self.log.debug("create %s", self.paths.run_flag)
        try:
            with open(self.paths.run_flag, "w"):
                pass
        except (IOError, OSError) as exc:
            self.log.error("failed to create %s: %s",
                           self.paths.run_flag, str(exc))

    def save_exc(self):
        """
        A helper method to save stacks in the service log.
        """
        self.log.error("unexpected error. stack saved in the service debug log")
        self.log.debug("", exc_info=True)

    def vcall(self, *args, **kwargs):
        """
        Wrap vcall, setting the service logger
        """
        kwargs["log"] = self.log
        return vcall(*args, **kwargs)

    def _read_cf(self):
        """
        Return the service config file content.
        """
        import codecs
        with codecs.open(self.paths.cf, "r", "utf8") as ofile:
            buff = ofile.read()
        return buff

    def _write_cf(self, buff):
        """
        Truncate the service config file and write buff.
        """
        import codecs
        import tempfile
        import shutil
        ofile = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.pathtmp, prefix=self.svcname)
        fpath = ofile.name
        os.chmod(fpath, 0o0644)
        ofile.close()
        with codecs.open(fpath, "w", "utf8") as ofile:
            ofile.write(buff)
        shutil.move(fpath, self.paths.cf)

