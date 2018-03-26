"""
The module defining the Svc class.
"""
from __future__ import print_function
from __future__ import unicode_literals

import sys
import os
import signal
import logging
import datetime
import time
import lock
import json
import re
import hashlib

from resources import Resource
from resourceset import ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv, get_osvc_paths, Storage
from rcUtilities import justcall, lazy, unset_lazy, vcall, lcall, is_string, \
                        try_decode, action_triggers, read_cf, \
                        drop_option, fcache
from converters import *
import rcExceptions as ex
import rcLogger
import node
from rcScheduler import Scheduler, SchedOpts, sched_action
from comm import Crypt
from extconfig import ExtConfig

if sys.version_info[0] < 3:
    BrokenPipeError = IOError
else:
    raw_input = input


def signal_handler(*args):
    """
    A signal handler raising the excSignal exception.
    Args can be signum and frame, but we don't use them.
    """
    raise ex.excSignal

DEFAULT_WAITLOCK = 60

ACTION_NO_ASYNC = [
    "logs",
]

ACTION_ASYNC = {
    "abort": {
        "target": "aborted",
        "progress": "aborting",
    },
    "delete": {
        "target": "deleted",
        "progress": "deleting",
    },
    "freeze": {
        "target": "frozen",
        "progress": "freezing",
    },
    "giveback": {
        "target": "placed",
        "progress": "placing",
    },
    "provision": {
        "target": "provisioned",
        "progress": "provisioning",
    },
    "purge": {
        "target": "purged",
        "progress": "purging",
    },
    "shutdown": {
        "target": "shutdown",
        "progress": "shutting",
    },
    "start": {
        "target": "started",
        "progress": "starting",
    },
    "stop": {
        "target": "stopped",
        "progress": "stopping",
    },
    "toc": {
        "progress": "tocing",
    },
    "thaw": {
        "target": "thawed",
        "progress": "thawing",
    },
    "unprovision": {
        "target": "unprovisioned",
        "progress": "unprovisioning",
    },
}

TOP_STATUS_GROUPS = [
    "overall",
    "avail",
    "optional",
]

DEFAULT_STATUS_GROUPS = [
    "ip",
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
    "docker",
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
    "push_config",
    "push_encap_config",
    "prstatus",
    "resource_monitor",
    "status",
    "validate_config",
]

ACTIONS_ALLOW_ON_INVALID_NODE = [
    "abort",
    "clear",
    "delete",
    "edit_config",
    "frozen",
    "get",
    "logs",
    "print_config",
    "print_status",
    "set",
    "unset",
    "update",
    "validate_config",
]

ACTIONS_NO_LOG = [
    "delete",
    "edit_config",
    "get",
    "group_status",
    "logs",
    "push_resinfo",
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
    "delete",
    "dns_update",
    "enable",
    "disable",
    "status",
    "pg_freeze",
    "pg_thaw",
    "pg_kill",
    "logs",
    "edit_config",
    "push_resinfo",
    "push_config",
    "push_encap_config",
    "group_status",
    "presync",
    "postsync",
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
    "docker",
    "edit_config",
    "freeze",
    "frozen",
    "get",
    "logs",
    "push_resinfo",
    "push_config",
    "push_encap_config",
    "run",
    "status",
    "set_provisioned",
    "set_unprovisioned",
    "thaw",
    "toc",
    "validate_config",
]

START_GROUPS = [
    "ip",
    "sync.netapp",
    "sync.dcsckpt",
    "sync.nexenta",
    "sync.symclone",
    "sync.symsnap",
    "sync.symsrdfs",
    "sync.hp3par",
    "sync.ibmdssnap",
    "disk",
    "fs",
    "share",
    "container",
    "app",
]

STOP_GROUPS = [
    "app",
    "container",
    "share",
    "fs",
    "sync.btrfssnap",
    "disk",
    "ip",
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
    "container.lxd",
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
    "disk.veritas",
    "disk.zpool",
    "fs",
    "fs.dir",
    "fs.docker",
    "ip",
    "ip.docker",
    "ip.cni",
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
    "sync.radossnap",
    "sync.radosclone",
    "sync.rsync",
    "sync.symclone",
    "sync.symsnap",
    "sync.symsrdfs",
    "sync.s3",
    "sync.zfs",
    "sync.zfssnap",
    "task",
]

ACTIONS_DO_MASTER = [
    "freeze",
    "set_provisioned",
    "set_unprovisioned",
    "run",
    "thaw",
    "toc",
]

ACTIONS_DO_MASTER_AND_SLAVE = [
    "migrate",
    "prstart",
    "prstop",
    "restart",
    "shutdown",
    "start",
    "startstandby",
    "stop",
    "switch",
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
]

os.environ['LANG'] = 'C'

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
        raise ex.excError("specify either --master, --slave(s) or both (%s)" % func.__name__)

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
           (not self.options.master and not self.options.slaves and self.options.slave is None and \
           self.running_action in ACTIONS_DO_MASTER_AND_SLAVE):
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
        if self.running_action in ACTIONS_DO_MASTER_AND_SLAVE + ACTIONS_DO_MASTER:
            return
        if self.options.master or self.options.slaves or self.options.slave is not None:
            return
        raise ex.excError("specify either --master, --slave(s) or both (%s)" % func.__name__)

    def _func(self):
        need_specifier(self)
        if self.options.master or \
           (not self.options.master and not self.options.slaves and self.options.slave is None and \
           self.running_action in ACTIONS_DO_MASTER_AND_SLAVE + ACTIONS_DO_MASTER):
            func(self)
    return _func

class Svc(Crypt, ExtConfig):
    """
    A OpenSVC service class.
    A service is a collection of resources.
    It exposes operations methods like provision, unprovision, stop, start,
    and sync.
    """
    # for ExtConfig
    default_status_groups = DEFAULT_STATUS_GROUPS

    def __init__(self, svcname=None, node=None, cf=None):
        self.type = "hosted"
        self.svcname = svcname
        self.node = node
        self.hostid = rcEnv.nodename
        self.paths = Storage(
            exe=os.path.join(rcEnv.paths.pathetc, self.svcname),
            cf=os.path.join(rcEnv.paths.pathetc, self.svcname+'.conf'),
            initd=os.path.join(rcEnv.paths.pathetc, self.svcname+'.d'),
            alt_initd=os.path.join(rcEnv.paths.pathetc, self.svcname+'.dir'),
            tmp_cf=os.path.join(rcEnv.paths.pathtmp, self.svcname+".conf.tmp")
        )
        if cf:
            self.paths.cf = cf
        self.resources_by_id = {}
        self.encap_resources = {}
        self.resourcesets_by_id = {}

        self.ref_cache = {}
        self.encap_json_status_cache = {}
        self.rset_status_cache = None
        self.lockfd = None
        self.abort_start_done = False
        self.action_start_date = datetime.datetime.now()
        self.has_encap_resources = False
        self.encap = False
        self.action_rid = []
        self.action_rid_before_depends = []
        self.action_rid_depends = []
        self.dependencies = {}
        self.running_action = None
        self.need_postsync = set()

        # set by the builder
        self.conf = os.path.join(rcEnv.paths.pathetc, svcname+".conf")
        self.comment = ""
        self.orchestrate = "ha"
        self.topology = "failover"
        self.placement = "nodes order"
        self.stonith = False
        self.parents = []
        self.show_disabled = False
        self.svc_env = rcEnv.node_env
        self.nodes = set([rcEnv.nodename])
        self.ordered_nodes = [rcEnv.nodename]
        self.drpnodes = set()
        self.ordered_drpnodes = []
        self.drpnode = ""
        self.encapnodes = set()
        self.flex_primary = ""
        self.drp_flex_primary = ""
        self.create_pg = False
        self.disable_rollback = False
        self.presync_done = False
        self.presnap_trigger = None
        self.postsnap_trigger = None
        self.monitor_action = None
        self.pre_monitor_action = None
        self.lock_timeout = DEFAULT_WAITLOCK

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
            waitlock=DEFAULT_WAITLOCK,
            wait=False,
        )

    @lazy
    def kwdict(self):
        return __import__("svcdict")

    @lazy
    def var_d(self):
        var_d = os.path.join(rcEnv.paths.pathvar, "services", self.svcname)
        if not os.path.exists(var_d):
            os.makedirs(var_d, 0o755)
        return var_d

    @lazy
    def log(self):
        return rcLogger.initLogger(rcEnv.nodename+"."+self.svcname)

    @lazy
    def sched(self):
        """
        Lazy init of the service scheduler.
        """
        return Scheduler(
            name=self.svcname,
            config_defaults=CONFIG_DEFAULTS,
            options=self.options,
            config=self.config,
            log=self.log,
            svc=self,
            scheduler_actions={
                "compliance_auto": SchedOpts(
                    "DEFAULT",
                    fname="last_comp_check",
                    schedule_option="comp_schedule"
                ),
                "push_resinfo": SchedOpts(
                    "DEFAULT",
                    fname="last_push_resinfo",
                    schedule_option="resinfo_schedule"
                ),
            },
        )

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
    def peers(self):
        if rcEnv.nodename in self.nodes:
            return self.nodes
        elif rcEnv.nodename in self.drpnodes:
            return self.drpnodes
        else:
            return []

    @lazy
    def ordered_peers(self):
        if rcEnv.nodename in self.nodes:
            return self.ordered_nodes
        elif rcEnv.nodename in self.drpnodes:
            return self.ordered_drpnodes
        else:
            return []

    @lazy
    def dockerlib(self):
        """
        Lazy allocator for the dockerlib object.
        """
        import rcDocker
        return rcDocker.DockerLib(self)

    @lazy
    def freezer(self):
        """
        Lazy allocator for the freezer object.
        """
        return Freezer(self.svcname)

    @lazy
    def compliance(self):
        from compliance import Compliance
        comp = Compliance(self)
        return comp

    @lazy
    def disabled(self):
        try:
            return self.conf_get("DEFAULT", "disable")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def children(self):
        try:
            children = self.conf_get('DEFAULT', "children")
        except ex.OptNotFound as exc:
            children = exc.default
        return children

    @lazy
    def slaves(self):
        try:
            slaves = self.conf_get('DEFAULT', "slaves")
        except ex.OptNotFound as exc:
            slaves = exc.default
        return slaves

    @lazy
    def children_and_slaves(self):
        data = self.children + self.slaves
        if self.scaler is not None:
            data += self.scaler.slaves
        return data

    @lazy
    def scaler_slave(self):
        try:
            scaler_slave = self.conf_get('DEFAULT', "scaler_slave")
        except ex.OptNotFound as exc:
            scaler_slave = exc.default
        return scaler_slave

    @lazy
    def scale_target(self):
        try:
            val = self.conf_get("DEFAULT", "scale")
            if isinstance(val, int) and val < 0:
                val = 0
        except ex.OptNotFound as exc:
            val = exc.default
        return val

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
            name = str(idx) + "." + self.svcname
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
        except ex.excError:
            return True

    @lazy
    def hard_affinity(self):
        try:
            return self.conf_get("DEFAULT", "hard_affinity")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def hard_anti_affinity(self):
        try:
            return self.conf_get("DEFAULT", "hard_anti_affinity")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def soft_affinity(self):
        try:
            return self.conf_get("DEFAULT", "soft_affinity")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def soft_anti_affinity(self):
        try:
            return self.conf_get("DEFAULT", "soft_anti_affinity")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def flex_min_nodes(self):
        try:
           val = self.conf_get('DEFAULT', 'flex_min_nodes')
        except ex.OptNotFound as exc:
           return exc.default
        if val < 0:
           val = 0
        nb_nodes = len(self.nodes|self.drpnodes)
        if val > nb_nodes:
           val = nb_nodes
        return val

    @lazy
    def flex_max_nodes(self):
        nb_nodes = len(self.peers)
        try:
           val = self.conf_get('DEFAULT', 'flex_max_nodes')
        except ex.OptNotFound:
           return nb_nodes
        if val > nb_nodes:
           val = nb_nodes
        if val < self.flex_min_nodes:
           val = self.flex_min_nodes
        return val

    @lazy
    def flex_cpu_low_threshold(self):
        try:
            val = self.conf_get('DEFAULT', 'flex_cpu_low_threshold')
        except ex.OptNotFound as exc:
            return exc.default
        if val < 0:
            return 0
        if val > 100:
            return 100
        return val

    @lazy
    def flex_cpu_high_threshold(self):
        try:
            val = self.conf_get('DEFAULT', 'flex_cpu_high_threshold')
        except ex.OptNotFound as exc:
            return exc.default
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
        try:
            return self.conf_get("DEFAULT", "app")
        except ex.OptNotFound as exc:
            return exc.default

    def get_node(self):
        if self.node is None:
            self.node = node.Node()
        return self.node

    def __lt__(self, other):
        """
        Order by service name
        """
        return self.svcname < other.svcname

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
        elif action in ("shutdown", "unprovision", "stop"):
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
        elif action in ("shutdown", "unprovision", "stop"):
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

    def resource_handling_dir(self, path):
        mntpts = {}
        for res in self.get_resources(["fs"]):
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
        for res in self.get_resources():
            if res.monitor and not res.is_disabled():
                return True
        return False

    def post_build(self):
        """
        A method run after the service is done building.
        Add resource-dependent tasks to the scheduler.
        """
        try:
            monitor_schedule = self.conf_get('DEFAULT', 'monitor_schedule')
        except ex.OptNotFound:
            monitor_schedule = None

        if not self.encap:
            self.sched.scheduler_actions["status"] = SchedOpts(
                    "DEFAULT",
                    fname="last_status",
                    schedule_option="status_schedule"
            )
            if self.has_monitored_resources() or monitor_schedule is not None:
                self.sched.scheduler_actions["resource_monitor"] = SchedOpts(
                    "DEFAULT",
                    fname="last_resource_monitor",
                    schedule_option="monitor_schedule"
                )

        syncs = []
        for resource in self.get_resources("sync"):
            schedule_option = "sync_schedule" if resource.rid != "sync#i0" \
                              else "sync#i0_schedule"
            syncs += [SchedOpts(
                resource.rid,
                fname="last_syncall_"+resource.rid,
                schedule_option=schedule_option
            )]
        if len(syncs) > 0:
            self.sched.scheduler_actions["sync_all"] = syncs

        tasks = []
        for resource in self.get_resources("task"):
            tasks += [SchedOpts(
                resource.rid,
                fname="last_"+resource.rid,
                schedule_option="no_schedule"
            )]
        if len(tasks) > 0:
            self.sched.scheduler_actions["run"] = tasks


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
        if os.path.exists(self.status_data_dump):
            os.unlink(self.status_data_dump)

    def purge_status_last(self):
        """
        Purge all service resources on-disk status caches.
        """
        for rset in self.resourcesets_by_id.values():
            rset.purge_status_last()

    def get_subset_parallel(self, rtype):
        """
        Return True if the resources of a resourceset can run an action in
        parallel executing per-resource workers.
        """
        rtype = rtype.split(".")[0]
        subset_section = 'subset#' + rtype
        if not self.config.has_section(subset_section):
            return False
        try:
            return self.conf_get(subset_section, "parallel")
        except ex.OptNotFound:
            return False

    def get_scsireserv(self, rid):
        """
        Get the 'scsireserv' config keyword value for rid.
        """
        try:
            return self.conf_get(rid, 'scsireserv')
        except ex.OptNotFound as exc:
            return exc.default

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
            sr = __import__('resScsiReserv'+rcEnv.sysname)
        except ImportError:
            sr = __import__('resScsiReserv')

        kwargs = {}
        pr_rid = resource.rid+"pr"

        try:
            kwargs["prkey"] = self.conf_get(resource.rid, 'prkey')
        except ex.OptNotFound as exc:
            kwargs["prkey"] = exc.default

        try:
            kwargs['no_preempt_abort'] = self.conf_get(resource.rid, 'no_preempt_abort')
        except ex.OptNotFound as exc:
            kwargs['no_preempt_abort'] = exc.default

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
            kwargs['standby'] = resource.standby

        kwargs['rid'] = resource.rid
        kwargs['peer_resource'] = resource

        r = sr.ScsiReserv(**kwargs)
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
        if resource.type == "task":
            actions += [
                'run',
            ]
        for action in actions:
            try:
                s = self.conf_get(resource.rid, action+'_requires')
            except ex.OptNotFound:
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
            if other.rid in self.resourcesets_by_id:
                self.resourcesets_by_id[other.rid] += other
            else:
                # new ResourceSet or ResourceSet-derived class
                self.resourcesets_by_id[other.rid] = other
                other.svc = self
            return self

        base_type = other.type.split(".")[0]
        if other.subset is not None:
            # the resource wants to be added to a specific resourceset
            # for action grouping, parallel execution or sub-resource
            # triggers
            rset_id = "%s:%s" % (base_type, other.subset)
        else:
            rset_id = base_type

        if rset_id in self.resourcesets_by_id:
            # the resource set already exists. add resource or resourceset.
            self.resourcesets_by_id[rset_id] += other
        elif isinstance(other, Resource):
            parallel = self.get_subset_parallel(rset_id)
            rset = ResourceSet(rset_id, resources=[other], parallel=parallel)
            rset.svc = self
            rset.pg_settings = self.get_pg_settings("subset#"+rset_id)
            self.__iadd__(rset)
        else:
            self.log.debug("unexpected object addition to the service: %s",
                           str(other))

        other.svc = self

        if isinstance(other, Resource) and other.rid and "#" in other.rid:
            other.pg_settings = self.get_pg_settings(other.rid)
            self.add_scsireserv(other)
            self.add_requires(other)
            self.resources_by_id[other.rid] = other

        if not other.is_disabled() and hasattr(other, "on_add"):
            other.on_add()

        return self

    def dblogger(self, action, begin, end, actionlogfile):
        """
        Send to the collector the service status after an action, and
        the action log.
        """
        self.node.daemon_collector_xmlrpc('end_action', self.svcname, action,
                                          begin, end, self.options.cron,
                                          actionlogfile)
        try:
            logging.shutdown()
        except:
            pass

    def validate_mon_action(self, action):
        if action in ("freeze", "abort"):
            return
        if self.options.local:
            return
        data = self.get_smon_data()
        if data is None:
            return
        for nodename, _data in data["instances"].items():
            status = _data.get("status", "unknown")
            if status != "idle":
                raise ex.excError("instance on node %s in %s state"
                                  "" % (nodename, status))
        global_expect = data.get("service", {}).get("global_expect")
        if global_expect is not None:
            raise ex.excError("service has already been asked to reach the "
                              "%s global state" % global_expect)

        data = self.node._daemon_status()
        if self.svcname not in data["monitor"]["services"]:
            return
        if action in ("start", "stop"):
            avail = data["monitor"]["services"][self.svcname]["avail"]
            if action == "start" and avail == "up":
                raise ex.excError("the service is already started.")
            elif action == "stop" and avail in ("down", "stdby down"):
                raise ex.excError("the service is already stopped.")
            if avail in ("n/a", "undef"):
                raise ex.excError("the service is in '%s' aggregated avail "
                                  "status. the daemons won't honor this "
                                  "request, so don't submit it." % avail)
            avails = set([data["monitor"]["nodes"][node]["services"]["status"].get(self.svcname, {}).get("avail") for node in data["monitor"]["nodes"]])
            if action == "start" and len(avails & set(["warn"])) > 0:
                raise ex.excError("the service has instances in 'warn' avail "
                                  "status. the daemons won't honor this request,"
                                  " so don't submit it.")

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

    def svclock(self, action=None, timeout=30, delay=1):
        """
        Acquire the service action lock.
        """
        suffix = None
        if (action not in ACTION_NO_ASYNC and self.options.node is not None and self.options.node != "") or \
           action in ACTIONS_NO_LOCK or \
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

        lockfile = os.path.join(rcEnv.paths.pathlock, self.svcname)
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))

        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire service lock %s", details)

        # try an immmediate lock acquire and see if the running action is
        # compatible
        if action in ACTIONS_LOCK_COMPAT:
            try:
                lockfd = lock.lock(
                    timeout=0,
                    delay=delay,
                    lockfile=lockfile,
                    intent=action
                )
                if lockfd is not None:
                    self.lockfd = lockfd
                return
            except lock.LockTimeout as exc:
                if exc.intent in ACTIONS_LOCK_COMPAT[action]:
                    return
                # not compatible, continue with the normal acquire
            except Exception:
                pass

        try:
            lockfd = lock.lock(
                timeout=timeout,
                delay=delay,
                lockfile=lockfile,
                intent=action
            )
        except lock.LockTimeout as exc:
            raise ex.excError("timed out waiting for lock %s: %s" % (details, str(exc)))
        except lock.LockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.LockCreateError:
            raise ex.excError("can not create lock file %s" % details)
        except lock.LockAcquire as exc:
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

    def get_resource(self, rid):
        """
        Return a resource object by id.
        Return None if the rid is not found.
        """
        if rid not in self.resources_by_id:
            return
        return self.resources_by_id[rid]

    def get_resources(self, _type=None, discard_disabled=True):
        """
        Return the list of resources matching criteria.

        <_type> can be:
          None: all resources are returned
        """
        if _type is None:
            return self.resources_by_id.values()
        if not isinstance(_type, (list, tuple)):
            _types = [_type]
        else:
            _types = _type

        resources = []
        for resource in self.resources_by_id.values():
            if not self.encap and resource.encap:
                continue
            if discard_disabled and resource.is_disabled():
                continue
            for t in _types:
                if "." in t and resource.type == t or \
                   "." not in t and t == resource.type.split(".")[0]:
                    resources.append(resource)
        return resources

    def get_resourcesets(self, _type, strict=False):
        """
        Return the list of resourceset matching the specified types.
        """
        if not isinstance(_type, (set, list, tuple)):
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
                self.type.startswith("sync"):
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
                   rcEnv.nodename in self.nodes:
                    return True
        return False

    def action_triggers(self, trigger, action, **kwargs):
        """
        Executes a resource trigger. Guess if the shell mode is needed from
        the trigger syntax.
        """
        action_triggers(self, trigger, action, **kwargs)

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
            * excError, stop looping over the resources and propagate up
              to the caller.
            * any other exception, save the traceback in the debug log
              and stop looping over the resources and raise an excError
            """
            aborted = []
            for rset in rsets:
                if action in ACTIONS_NO_TRIGGER or rset.all_skip(action):
                    break
                try:
                    rset.log.debug("start %s %s_action", rset.rid, when)
                    aborted += getattr(rset, when + "_action")(action, types=_type, tags=tags, xtags=xtags)
                except ex.excError:
                    raise
                except:
                    self.save_exc()
                    raise ex.excError
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
                raise ex.excError(results[2])

        need_snap = self.need_snap_trigger(rsets, action)

        # snapshots are created in pre_action and destroyed in post_action
        # place presnap and postsnap triggers around pre_action
        do_snap_trigger("pre")
        aborted = do_trigger("pre")
        do_snap_trigger("post")

        last = None
        for rset in rsets:
            # upto / downto break
            current = rset.rid.split(":")[0]
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
        output = self.svcname
        for rset in self.resourcesets_by_id.values():
            output += "  [%s]" % str(rset)
        return output

    def prstatus(self):
        status = rcStatus.Status()
        for resource in self.get_resources("disk.scsireserv"):
            status += resource.status()
        return int(status)

    def status(self):
        """
        Return the aggregate status a service.
        """
        refresh = self.options.refresh or (not self.encap and self.options.cron)
        data = self.print_status_data(mon_data=False, refresh=refresh)
        return rcStatus.Status(data["overall"]).value()

    @fcache
    def get_mon_data(self):
        data = self.node._daemon_status(silent=True)
        if data is not None:
            return data["monitor"]
        return {}

    def get_smon_data(self):
        data = {}
        try:
            mon_data = self.get_mon_data()
            data["compat"] = mon_data["compat"]
            data["service"] = mon_data["services"][self.svcname]
            data["instances"] = {}
            for nodename in mon_data["nodes"]:
                 try:
                     data["instances"][nodename] = mon_data["nodes"][nodename]["services"]["status"][self.svcname]["monitor"]
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

    def print_status_data(self, from_resource_status_cache=False, mon_data=False, refresh=False):
        """
        Return a structure containing hierarchical status of
        the service and monitor information. Fetch CRM status from cache if
        possible and allowed by kwargs.
        """
        lockfile = os.path.join(rcEnv.paths.pathlock, self.svcname + ".status")
        try:
            with lock.cmlock(timeout=30, delay=1, lockfile=lockfile):
                if not from_resource_status_cache and \
                   not refresh and \
                   os.path.exists(self.status_data_dump) and \
                   not self.status_data_dump_outdated():
                    try:
                        with open(self.status_data_dump, 'r') as filep:
                            data = json.load(filep)
                    except ValueError:
                        data = self.print_status_data_eval(refresh=refresh)
                else:
                    data = self.print_status_data_eval(refresh=refresh)
        except lock.LOCK_EXCEPTIONS as exc:
            raise ex.excAbortAction(str(exc))

        if mon_data:
            mon_data = self.get_smon_data()
            try:
                data["cluster"] = {
                    "compat": mon_data["compat"],
                    "avail": mon_data["service"]["avail"],
                    "overall": mon_data["service"]["overall"],
                    "placement": mon_data["service"]["placement"],
                }
                data["monitor"] = mon_data["instances"][rcEnv.nodename]
            except:
                pass

        if not refresh:
            data["running"] = self.get_running()
        return data

    def get_running(self):
        lockfile = os.path.join(rcEnv.paths.pathlock, self.svcname)
        running = []
        running += [self._get_running(lockfile).get("rid")]
        lockfile = os.path.join(rcEnv.paths.pathlock, self.svcname+".sync")
        running += [self._get_running(lockfile).get("rid")]
        for task in self.get_resources("task"):
            lockfile = os.path.join(task.var_d, "run.lock")
            running += [self._get_running(lockfile).get("rid")]
        return [rid for rid in running if rid]

    def _get_running(self, lockfile):
        try:
            with lock.cmlock(lockfile=lockfile, timeout=0):
                return {}
        except Exception as exc:
            # failing to open "w+", action in progress
            try:
                with open(lockfile, "r") as ofile:
                    #ofile.seek(0)
                    lock_data = json.load(ofile)
                    return lock_data.get("progress", {})
            except Exception:
                pass
            return {}
        return {}

    def print_status_data_eval(self, refresh=False):
        """
        Return a structure containing hierarchical status of
        the service.
        """
        now = time.time()
        group_status = self.group_status(refresh=refresh)

        data = {
            "updated": datetime.datetime.utcfromtimestamp(now).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "mtime": now,
            "app": self.app,
            "placement": self.placement,
            "topology": self.topology,
            "provisioned": True,
            "resources": {},
        }
        data["running"] = self.get_running()
        if self.topology == "flex":
            data.update({
                "flex_min_nodes": self.flex_min_nodes,
                "flex_max_nodes": self.flex_max_nodes,
            })
        frozen = self.frozen()
        if frozen:
            data["frozen"] = frozen
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

        containers = self.get_resources('container')
        if len(containers) > 0:
            data['encap'] = {}
            for container in containers:
                if not self.has_encap_resources:
                    continue
                if container.name is None or len(container.name) == 0:
                    # docker case
                    continue
                try:
                    data['encap'][container.rid] = self.encap_json_status(container, refresh=refresh)
                    # merge container overall status, so we propagate encap alerts
                    # up to instance and service level.
                    group_status["overall"] += rcStatus.Status(data["encap"][container.rid]["overall"] if "overall" in data["encap"][container.rid] else "n/a")
                    group_status["avail"] += rcStatus.Status(data["encap"][container.rid]["avail"] if "avail" in data["encap"][container.rid] else "n/a")
                except:
                    data["encap"][container.rid] = {"resources": {}}
                if hasattr(container, "vm_hostname"):
                    data["encap"][container.rid]["hostname"] = container.vm_hostname
        elif self.encap:
            data['encap'] = True

        for rset in self.get_resourcesets(STATUS_TYPES, strict=True):
            for resource in rset.resources:
                status = rcStatus.Status(resource.status(verbose=True))
                log = resource.status_logs_strlist()
                info = resource.status_info()
                tags = sorted(list(resource.tags))
                disable = resource.is_disabled()
                _data = {
                    "status": str(status),
                    "type": resource.type,
                    "label": resource.label,
                }
                _data["provisioned"] = resource.provisioned_data()
                if disable:
                    _data["disable"] = disable
                if resource.standby:
                    _data["standby"] = resource.standby
                if resource.encap:
                    _data["encap"] = resource.encap
                if resource.optional:
                    _data["optional"] = resource.optional
                if resource.monitor:
                    _data["monitor"] = resource.monitor
                if len(log) > 0:
                    _data["log"] = log
                if len(info) > 0:
                    _data["info"] = info
                if len(tags) > 0:
                    _data["tags"] = tags
                if _data["provisioned"]["state"] is False:
                    data["provisioned"] = False
                if resource.subset:
                    _data["subset"] = resource.subset
                data["resources"][resource.rid] = _data
        for group in TOP_STATUS_GROUPS:
            group_status[group] = str(group_status[group])
        for group in group_status["status_group"]:
            group_status["status_group"][group] = str(group_status["status_group"][group])
        data.update(group_status)
        if self.stonith and self.topology == "failover" and data["avail"] == "up":
            data["stonith"] = True
        self.write_status_data(data)
        return data

    def csum_status_data(self, data):
        h = hashlib.md5()
        def fn(h, val):
            if type(val) == dict:
                for key, _val in val.items():
                    if key in ("status_updated", "updated", "mtime"):
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
        data["csum"] = self.csum_status_data(data)
        try:
            with open(self.status_data_dump, "w") as filep:
                json.dump(data, filep)
                filep.flush()
            os.utime(self.status_data_dump, (-1, data["mtime"]))
            self.wake_monitor()
        except Exception as exc:
            self.log.warning("failed to update %s: %s",
                             self.status_data_dump, str(exc))
        return data

    def update_status_data(self):
        if self.options.minimal:
            return
        self.log.debug("update status dump")
        data = self.print_status_data(from_resource_status_cache=True)
        self.write_status_data(data)

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
            print(rcStatus.colorize_status(str(resource.status(refresh=self.options.refresh))))
        return 0

    def print_status(self):
        """
        Display in human-readable format the hierarchical service status.
        """
        data = self.print_status_data(mon_data=True, refresh=self.options.refresh)
        if self.options.format is not None:
            return data

        from rcColor import color, colorize, STATUS_COLOR
        from forest import Forest

        def fmt_flags(resource, running):
            """
            Format resource flags as a vector of character.

            R  Running
            M  Monitored
            D  Disabled
            O  Optional
            E  Encap
            P  Provisioned
            S  Standby
            """
            flags = ''
            flags += "R" if resource["rid"] in running else '.'
            flags += 'M' if resource.get("monitor") else '.'
            flags += 'D' if resource.get("disable") else '.'
            flags += 'O' if resource.get("optional") else '.'
            flags += 'E' if resource.get("encap") else '.'
            provisioned = resource.get("provisioned", {}).get("state")
            if provisioned is True:
                flags += '.'
            elif provisioned is False:
                flags += 'P'
            else:
                flags += '/'
            flags += 'S' if resource.get("standby") else '.'
            return flags

        def dispatch_resources(data):
            """
            Sorted resources.
            Honor the --discard-disabled arg.
            """
            subsets = {}
            for group in DEFAULT_STATUS_GROUPS:
                subsets[group] = {}

            for rid, resource in data["resources"].items():
                if discard_disabled and resource.get("disable", False):
                    continue
                group = resource["type"].split(".", 1)[0]
                if "subset" in resource:
                    subset = group + ":" + resource["subset"]
                else:
                    subset = group
                if subset not in subsets[group]:
                    subsets[group][subset] = []
                resource["rid"] = rid
                subsets[group][subset].append(resource)

            return subsets

        def add_subsets(subsets, node_nodename, running):
            for group in DEFAULT_STATUS_GROUPS:
                subset_names = sorted(subsets[group])
                for subset in subset_names:
                    if subset != group:
                        node_subset = node_nodename.add_node()
                        node_subset.add_column(subset)
                        try:
                            parallel = self.conf_get("subset#"+subset, "parallel")
                        except ex.OptNotFound as exc:
                            parallel = exc.default
                        if parallel:
                            node_subset.add_column()
                            node_subset.add_column()
                            node_subset.add_column("//")
                    else:
                        node_subset = node_nodename
                    for resource in sorted(subsets[group][subset], key=lambda x: x["rid"]):
                        add_res_node(resource, node_subset, running=running)

        # discard disabled resources ?
        if self.options.show_disabled is not None:
            discard_disabled = not self.options.show_disabled
        else:
            discard_disabled = not self.show_disabled

        subsets = dispatch_resources(data)

        # service-level notices
        svc_notice = []
        if "cluster" in data:
            if data["cluster"]["overall"] == "warn":
                svc_notice.append(colorize(data["cluster"]["overall"], STATUS_COLOR[data["cluster"]["overall"]]))
            if data["cluster"]["placement"] not in ("optimal", "n/a"):
                svc_notice.append(colorize(data["cluster"]["placement"] + " placement", color.RED))
            if self.ha and not data["cluster"]["compat"]:
                svc_notice.append(colorize("incompatible versions", color.RED))
        svc_notice = ", ".join(svc_notice)

        # instance-level notices
        def instance_notice(overall=None, frozen=None, node_frozen=None, constraints=None,
                            provisioned=None, monitor=None):
            notice = []
            if overall == "warn":
                notice.append(colorize(overall, STATUS_COLOR[overall]))
            if frozen:
                notice.append(colorize("frozen", color.BLUE))
            if node_frozen:
                notice.append(colorize("node frozen", color.BLUE))
            if not constraints:
                notice.append("constraints violation")
            if provisioned is False:
                notice.append(colorize("not provisioned", color.RED))
            if monitor:
                mon_status = monitor.get("status", "unknown")
                if monitor["status"] == "idle":
                    notice.append(colorize(mon_status, color.LIGHTBLUE))
                else:
                    notice.append(colorize(mon_status, color.RED))
                if monitor.get("local_expect") not in ("", None):
                    notice.append(colorize(monitor.get("local_expect", ""), color.LIGHTBLUE))
                if monitor.get("global_expect") not in ("", None):
                    notice.append(colorize(">"+monitor.get("global_expect", ""), color.LIGHTBLUE))
            else:
                notice.append(colorize("daemon down", color.RED))
            return ", ".join(notice)

        notice = instance_notice(overall=data["overall"],
                                 frozen=self.frozen(),
                                 node_frozen=self.node.frozen(),
                                 constraints=data.get("constraints", True),
                                 provisioned=data.get("provisioned"),
                                 monitor=data.get("monitor"))

        # encap resources
        ers = {}
        for container in self.get_resources('container'):
            if container.type == "container.docker":
                continue
            try:
                ejs = data["encap"][container.rid]
                ers[container.rid] = dispatch_resources(ejs)
            except KeyError:
                continue
            except ex.excNotAvailable:
                ers[container.rid] = {}
            except Exception as exc:
                print(exc)
                ers[container.rid] = {}

        def add_res_node(resource, parent, rid=None, running=None):
            if discard_disabled and resource.get("disable", False):
                return
            if rid is None:
                rid = resource["rid"]
            node_res = parent.add_node()
            node_res.add_column(rid)
            node_res.add_column(fmt_flags(resource, running))
            node_res.add_column(resource["status"],
                                STATUS_COLOR[resource["status"]])
            col = node_res.add_column(resource["label"])
            if rid in ers and resource["status"] in ("up", "stdby up", "n/a"):
                edata = Storage(data["encap"].get(rid))
                encap_notice = instance_notice(
                    overall=edata.overall,
                    frozen=edata.frozen,
                    constraints=edata.get("constraints", True),
                    provisioned=edata.provisioned,
                    monitor=edata.monitor,
                )
                col.add_text(encap_notice, color.LIGHTBLUE)
            for line in resource.get("log", []):
                if line.startswith("warn:"):
                    scolor = STATUS_COLOR["warn"]
                elif line.startswith("err:"):
                    scolor = STATUS_COLOR["err"]
                else:
                    scolor = None
                col.add_text(line, scolor)

            if rid not in ers or resource["status"] not in ("up", "stdby up", "n/a"):
                return

            add_subsets(ers[rid], node_res, running)

        def add_instances(node):
            if len(self.peers) < 1:
                return
            for nodename in self.peers:
                if nodename == rcEnv.nodename:
                    continue
                add_instance(nodename, node)

        def add_instance(nodename, node):
            node_child = node.add_node()
            node_child.add_column(nodename, color.BOLD)
            node_child.add_column()
            try:
                mon_data = self.get_mon_data()
                data = mon_data["nodes"][nodename]["services"]["status"][self.svcname]
                avail = data["avail"]
                node_frozen = mon_data["nodes"][nodename].get("frozen")
            except KeyError as exc:
                avail = "undef"
                node_frozen = False
                data = Storage()
            node_child.add_column(avail, STATUS_COLOR[avail])
            notice = instance_notice(overall=data["overall"],
                                     frozen=data.get("frozen"),
                                     node_frozen=node_frozen,
                                     constraints=data.get("constraints", True),
                                     provisioned=data.get("provisioned"),
                                     monitor=data.get("monitor"))
            node_child.add_column(notice, color.LIGHTBLUE)

        def add_parents(node):
            if len(self.parents) == 0:
                return
            node_parents = node.add_node()
            node_parents.add_column("parents")
            for parent in self.parents:
                add_parent(parent, node_parents)

        def add_parent(svcname, node):
            node_parent = node.add_node()
            node_parent.add_column(svcname, color.BOLD)
            node_parent.add_column()
            try:
                mon_data = self.get_mon_data()
                avail = mon_data["services"][svcname]["avail"]
            except KeyError:
                avail = "undef"
            node_parent.add_column(avail, STATUS_COLOR[avail])

        def add_children(node):
            if len(self.children) == 0:
                return
            node_children = node.add_node()
            node_children.add_column("children")
            for child in self.children:
                add_child(child, node_children)

        def add_slaves(node):
            if len(self.slaves) == 0:
                return
            node_slaves = node.add_node()
            node_slaves.add_column("slaves")
            for child in self.slaves:
                add_child(child, node_slaves)

        def add_scaler_slaves(node):
            if self.scaler is None:
                return
            if self.scaler.slaves_count == 0:
                return
            node_slaves = node.add_node()
            node_slaves.add_column("scaler")
            for child in self.scaler.slaves:
                add_child(child, node_slaves)

        def add_child(svcname, node):
            node_child = node.add_node()
            node_child.add_column(svcname, color.BOLD)
            node_child.add_column()
            try:
                mon_data = self.get_mon_data()
                avail = mon_data["services"][svcname]["avail"]
            except KeyError:
                avail = "undef"
            node_child.add_column(avail, STATUS_COLOR[avail])

        tree = Forest(
            separator=" ",
            widths=(
                (14, None),
                None,
                10,
                None,
            ),
        )
        node_svcname = tree.add_node()
        node_svcname.add_column(self.svcname, color.BOLD)
        node_svcname.add_column()
        if "cluster" in data:
            node_svcname.add_column(data["cluster"]["avail"], STATUS_COLOR[data["cluster"]["avail"]])
        else:
            node_svcname.add_column()
        node_svcname.add_column(svc_notice)
        node_instances = node_svcname.add_node()
        node_instances.add_column("instances")
        add_instances(node_instances)
        node_nodename = node_instances.add_node()
        node_nodename.add_column(rcEnv.nodename, color.BOLD)
        node_nodename.add_column()
        node_nodename.add_column(data['avail'], STATUS_COLOR[data['avail']])
        node_nodename.add_column(notice, color.LIGHTBLUE)
        add_subsets(subsets, node_nodename, data["running"])
        add_parents(node_svcname)
        add_children(node_svcname)
        add_scaler_slaves(node_svcname)
        add_slaves(node_svcname)

        print(tree)

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

    def resource_monitor(self):
        """
        The resource monitor action. Refresh important resources at a different
        schedule.
        """
        for resource in self.get_resources():
            if resource.monitor:
                resource.status(refresh=True)

    def reboot(self):
        """
        A method wrapper the node reboot method.
        """
        self.node.system.reboot()

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
            self.sub_set_action(["app", "container"], "_pg_freeze")
        else:
            self._pg_freeze()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

    def pg_thaw(self):
        """
        Thaw all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action(["app", "container"], "_pg_thaw")
        else:
            self._pg_thaw()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

    def pg_kill(self):
        """
        Kill all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action(["app", "container"], "_pg_kill")
        else:
            self._pg_kill()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

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
            self.log.info("refuse to toc from a frozen service")
            return
        self.do_pre_monitor_action()
        if self.monitor_action is None:
            return
        if not hasattr(self, self.monitor_action):
            self.log.error("invalid monitor action '%s'", self.monitor_action)
            return
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

    def _encap_cmd(self, cmd, container, verbose=False, push_config=True):
        """
        Execute a command in a service container.
        """
        if container.pg_frozen():
            raise ex.excError("can't join a frozen container. abort encap "
                              "command.")
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
           (container.name in self.options.slave or \
            container.rid in self.options.slave):
            # no need to run encap cmd (container not specified in --slave)
            return '', '', 0

        if cmd == ['start'] and not self.command_is_scoped():
            return '', '', 0

        # make sure the container has an up-to-date service config
        if push_config:
            try:
                self._push_encap_config(container)
            except ex.excError:
                pass

        # now we known we'll execute a command in the slave, so purge the
        # encap cache
        self.purge_cache_encap_json_status(container.rid)

        # wait for the container multi-user state
        if cmd[0] in ["start"] and hasattr(container, "wait_multi_user"):
            container.wait_multi_user()

        options = ['--daemon']
        if self.options.dry_run:
            options.append('--dry-run')
        if self.options.force:
            options.append('--force')
        if self.options.local:
            options.append('--local')
        if self.options.disable_rollback:
            options.append('--disable-rollback')
        if self.options.rid:
            options.append('--rid')
            options.append(self.options.rid)
        if self.options.tags:
            options.append('--tags')
            options.append(self.options.tags)
        if self.options.subsets:
            options.append('--subsets')
            options.append(self.options.subsets)

        paths = get_osvc_paths(osvc_root_path=container.osvc_root_path,
                               sysname=container.guestos)
        cmd = [paths.svcmgr, '-s', self.svcname] + options + cmd

        if container is not None and hasattr(container, "rcmd"):
            out, err, ret = container.rcmd(cmd)
        elif hasattr(container, "runmethod"):
            cmd = container.runmethod + cmd
            out, err, ret = justcall(cmd, stdin=self.node.devnull)
        else:
            raise ex.excEncapUnjoignable("undefined rcmd/runmethod in "
                                         "resource %s"%container.rid)

        if verbose:
            self.log.info('logs from %s child service:', container.name)
            print(out)
            if len(err) > 0:
                print(err)
        if ret == 255:
            raise ex.excEncapUnjoignable
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
        self.encap_json_status_cache[rid] = data
        path = self.get_encap_json_status_path(rid)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with open(path, 'w') as ofile:
                ofile.write(json.dumps(data))
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
        from svcdict import DEPRECATED_SECTIONS
        egroups = set()
        for rid in self.encap_resources:
            egroup = rid.split('#')[0]
            if egroup in DEPRECATED_SECTIONS:
                egroup = DEPRECATED_SECTIONS[egroup][0]
            egroups.add(egroup)
        return egroups

    def encap_json_status(self, container, refresh=False):
        """
        Return the status data from the agent runnning the encapsulated part
        of the service.
        """
        if container.guestos == 'windows':
            raise ex.excNotAvailable

        if container.status(ignore_nostatus=True, refresh=refresh) == rcStatus.DOWN:
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
                group = resource.type.split('.')[0]
                if group not in groups:
                    continue
                if not self.encap and resource.encap:
                    group_status['resources'][resource.rid] = {'status': 'down'}

            groups = set(["app", "sync"])
            for group in groups:
                group_status[group] = 'n/a'
            for resource in self.get_resources(groups):
                group = resource.type.split('.')[0]
                if group not in groups:
                    continue
                if not self.encap and resource.encap:
                    group_status['resources'][resource.rid] = {'status': 'n/a'}

            return group_status

        if not refresh:
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

        cmd = ['print', 'status', '--format', 'json']
        if refresh:
            cmd.append('--refresh')
        try:
            results = self._encap_cmd(cmd, container)
        except ex.excError:
            return group_status
        except Exception as exc:
            print(exc)
            return group_status

        try:
            group_status = json.loads(results[0])
        except:
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
            groups = set(DEFAULT_STATUS_GROUPS)

        status = {
            "status_group": {},
        }
        groups = groups - excluded_groups
        self.get_rset_status(groups, refresh=refresh)

        # initialise status of each group
        for group in TOP_STATUS_GROUPS:
            status[group] = rcStatus.Status(rcStatus.NA)
        for group in groups:
            status["status_group"][group] = rcStatus.Status(rcStatus.NA)

        for driver in STATUS_TYPES:
            if driver in excluded_groups:
                continue
            group = driver.split('.')[0]
            if group not in groups:
                continue
            for resource in self.get_resources(driver):
                rstatus = resource.status()
                if resource.type.startswith("sync"):
                    if rstatus == rcStatus.UP:
                        rstatus = rcStatus.NA
                    elif rstatus == rcStatus.DOWN:
                        rstatus = rcStatus.WARN
                status["status_group"][group] += rstatus
                if resource.is_optional():
                    status["optional"] += rstatus
                else:
                    status["avail"] += rstatus
                if resource.status_logs_count(levels=["warn", "error"]) > 0:
                    status["overall"] += rcStatus.WARN

        if status["avail"].status == rcStatus.STDBY_UP_WITH_UP:
            # now we know the avail status we can promote
            # stdbyup to up
            status["avail"].status = rcStatus.UP
            for group in status:
                if status[group] == rcStatus.STDBY_UP:
                    status[group].status = rcStatus.UP
        elif status["avail"].status == rcStatus.STDBY_UP_WITH_DOWN:
            status["avail"].status = rcStatus.STDBY_UP

        if status["optional"].status == rcStatus.STDBY_UP_WITH_UP:
            status["optional"].status = rcStatus.UP
        elif status["optional"].status == rcStatus.STDBY_UP_WITH_DOWN:
            status["optional"].status = rcStatus.STDBY_UP

        status["overall"] += rcStatus.Status(status["avail"])
        status["overall"] += rcStatus.Status(status["optional"])

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
        if self.options.format is not None:
            return data
        from forest import Forest
        from rcColor import color
        tree = Forest()
        node1 = tree.add_node()
        node1.add_column(self.svcname, color.BOLD)
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
        print(tree)

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
                devs = sorted(list(getattr(resource, cat+"_devs")()))
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

    def print_config_mtime(self):
        """
        Print the service configuration file last modified timestamp. Used by
        remote agents to determine which agent holds the most recent version.
        """
        mtime = os.stat(self.paths.cf).st_mtime
        print(mtime)

    def prepare_async_cmd(self):
        cmd = sys.argv[1:]
        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("-s", cmd, drop_value=True)
        cmd = drop_option("--service", cmd, drop_value=True)
        return cmd

    def async_action(self, action, wait=None, timeout=None):
        if action in ACTION_NO_ASYNC:
            return
        if self.options.node is not None and self.options.node != "":
            cmd = self.prepare_async_cmd()
            ret = self.daemon_service_action(cmd)
            if ret == 0:
                raise ex.excAbortAction()
            else:
                raise ex.excError()
        if self.options.local or self.options.slave or self.options.slaves or \
           self.options.master:
            return
        if action not in ACTION_ASYNC:
            return
        if "target" not in ACTION_ASYNC[action]:
            return
        if self.command_is_scoped():
            return
        self.daemon_mon_action(action, wait=wait, timeout=timeout)
        raise ex.excAbortAction()

    def daemon_mon_action(self, action, wait=None, timeout=None, svcname=None):
        if svcname is None:
            svcname = self.svcname
        self.validate_mon_action(action)
        global_expect = ACTION_ASYNC[action]["target"]
        if action == "delete" and self.options.unprovision:
            global_expect = "purged"
            action = "purge"
        self.set_service_monitor(global_expect=global_expect, svcname=svcname)
        if svcname == self.svcname:
            self.log.info("%s action requested", action)
        else:
            self.log.info("%s action requested on service %s", action, svcname)
        if wait is None:
            wait = self.options.wait
        if timeout is None:
            timeout = self.options.time
        if timeout is not None:
            timeout = convert_duration(timeout)
        if not wait:
            return

        # poll global service status
        prev_global_expect_set = set()
        for _ in range(timeout):
            data = self.node._daemon_status(refresh=True)
            if data is None or "monitor" not in data:
                # interrupted, daemon died
                time.sleep(1)
                continue
            global_expect_set = set()
            inprogress = []
            for nodename in data["monitor"]["nodes"]:
                try:
                    _data = data["monitor"]["nodes"][nodename]["services"]["status"][svcname]
                except (KeyError, TypeError) as exc:
                    continue
                if _data["monitor"].get("global_expect") is not None:
                    inprogress.append(nodename)
                if _data["monitor"].get("global_expect") in (global_expect, "n/a"):
                    global_expect_set.add(nodename)
            if prev_global_expect_set != global_expect_set:
                for nodename in global_expect_set - prev_global_expect_set:
                    self.log.info(" work starting on %s", nodename)
                for nodename in prev_global_expect_set - global_expect_set:
                    self.log.info(" work over on %s", nodename)
            if not inprogress and not global_expect_set:
                if svcname not in data["monitor"]["services"]:
                    return
                self.log.info("final status: avail=%s overall=%s frozen=%s",
                              data["monitor"]["services"][svcname]["avail"],
                              data["monitor"]["services"][svcname]["overall"],
                              data["monitor"]["services"][svcname].get("frozen", False))
                return
            prev_global_expect_set = set(global_expect_set)
            time.sleep(1)
        raise ex.excError("wait timeout exceeded")

    def current_node(self):
        data = self.node._daemon_status()
        for nodename, _data in data["monitor"]["nodes"].items():
            try:
                __data = _data["services"]["status"][self.svcname]
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
        if (options.rid is not None and options.rid != "") or \
           (options.tags is not None and options.tags != "") or \
           (options.subsets is not None and options.subsets != "") or \
           options.upto or options.downto:
            return True
        return False

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
        self.sub_set_action(START_GROUPS, "startstandby", xtags=set(["zone", "docker"]))

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
        self.sub_set_action(START_GROUPS, "start", xtags=set(["zone", "docker"]))

    @_slave_action
    def slave_start(self):
        cmd = self.prepare_async_cmd()
        self.encap_cmd(cmd, verbose=True)

    def rollback(self):
        self.sub_set_action(STOP_GROUPS, "rollback", xtags=set(["zone", "docker"]))

    def stop(self):
        self.slave_stop()
        self.master_stop()

    @_master_action
    def master_stop(self):
        self.sub_set_action(STOP_GROUPS, "stop", xtags=set(["zone", "docker"]))

    @_slave_action
    def slave_stop(self):
        self.encap_cmd(['stop'], verbose=True, error="continue")

    def shutdown(self):
        self.options.force = True
        self.slave_shutdown()
        self.master_shutdown()

    @_master_action
    def master_shutdown(self):
        self.sub_set_action(STOP_GROUPS, "shutdown", xtags=set(["zone", "docker"]))

    @_slave_action
    def slave_shutdown(self):
        self.encap_cmd(['shutdown'], verbose=True, error="continue")

    def unprovision(self):
        self.sub_set_action(STOP_GROUPS, "unprovision", xtags=set(["zone", "docker"]))

    def provision(self):
        self.sub_set_action(START_GROUPS, "provision", xtags=set(["zone", "docker"]))

        if not self.options.disable_rollback:
            # set by the daemon on the placement leaders.
            # return the service to standby if not a placement leader
            self.rollback()

    def set_provisioned(self):
        self.sub_set_action(START_GROUPS, "set_provisioned", xtags=set(["zone", "docker"]))

    def set_unprovisioned(self):
        self.sub_set_action(START_GROUPS, "set_unprovisioned", xtags=set(["zone", "docker"]))

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
            if resource.skip or resource.is_disabled():
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
            self.daemon_service_action(['postsync', '--waitlock=3600'],
                                       nodename=nodename, sync=False,
                                       collect=False)

        self.need_postsync = set()

    def remote_action(self, nodename, action, waitlock=DEFAULT_WAITLOCK,
                      sync=False, verbose=True, action_mode=True, collect=True):
        rcmd = []
        if self.options.debug:
            rcmd += ['--debug']
        if self.options.dry_run:
            rcmd += ['--dry-run']
        if self.options.local and action_mode:
            rcmd += ['--local']
        if self.options.cron:
            rcmd += ['--cron']
        if self.options.waitlock != DEFAULT_WAITLOCK:
            rcmd += ['--waitlock', str(waitlock)]
        rcmd += action.split()
        if verbose:
            self.log.info("exec '%s' on node %s", ' '.join(rcmd), nodename)
        return self.daemon_service_action(rcmd, nodename=nodename, sync=sync,
                                          collect=collect, action_mode=action_mode)

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
        self.sub_set_action(rtypes, "sync_nodes")
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
        self.sub_set_action(rtypes, "sync_drp")
        self.remote_postsync()

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
            "sync.dcsckpt",
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
            "sync.dcssnap",
            "disk.md",
        ]
        self.sub_set_action(rtypes, "sync_resync")

    def sync_break(self):
        rtypes = [
            "sync.netapp",
            "sync.nexenta",
            "sync.hp3par",
            "sync.dcsckpt",
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
            "sync.dcsckpt",
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

    def print_config(self):
        """
        The 'print config' action entry point.
        Print the service configuration in the format specified by --format.
        """
        if self.options.format is not None:
            return self.print_config_data(evaluate=self.options.eval,
                                          impersonate=self.options.impersonate)
        from rcColor import print_color_config
        print_color_config(self.paths.cf)

    def make_temp_config(self):
        """
        Copy the current service configuration file to a temporary
        location for edition.
        If the temp file already exists, propose the --discard
        or --recover options.
        """
        import shutil
        if os.path.exists(self.paths.tmp_cf):
            if self.options.recover:
                pass
            elif self.options.discard:
                shutil.copy(self.paths.cf, self.paths.tmp_cf)
            else:
                self.edit_config_diff()
                print("%s exists: service is already being edited. Set "
                      "--discard to edit from the current configuration, "
                      "or --recover to open the unapplied config" % \
                      self.paths.tmp_cf, file=sys.stderr)
                raise ex.excError
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
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        from rcUtilities import which, fsum
        if not which(editor):
            print("%s not found" % editor, file=sys.stderr)
            return 1
        path = self.make_temp_config()
        os.environ["LANG"] = "en_US.UTF-8"
        os.system(' '.join((editor, path)))
        if fsum(path) == fsum(self.paths.cf):
            os.unlink(path)
            return 0
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
                    self.log.debug("resource %s can sync: %s" % (resource.rid, str(ret)))
                except ex.excError as exc:
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
        self.sub_set_action(rtypes, "sync_nodes")
        self.sub_set_action(rtypes, "sync_drp")
        self.remote_postsync()

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
                print(exc, file=sys.stderr)
            for __data in _data:
                rid = __data[-3]
                if rid not in data:
                    data[rid] = []
                data[rid].append(__data)
        if not self.config.has_section("env"):
            return data
        for key in self.config.options("env"):
            try:
                val = self.conf_get("env", key)
            except ex.OptNotFound as exc:
                continue
            if "env" not in data:
                data["env"] = []
            data["env"].append([
                self.svcname,
                rcEnv.nodename,
                self.topology,
                "env",
                key if key is not None else "",
                val if val is not None else "",
            ])
        return data

    def print_resinfo(self):
        if self.options.format is None:
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
        from forest import Forest
        from rcColor import color
        tree = Forest()
        node1 = tree.add_node()
        node1.add_column(self.svcname, color.BOLD)
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
        print(tree)

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
        data = []
        for _data in self.resinfo().values():
            data += _data
        self.node.collector.call('push_resinfo', data)

    def push_encap_config(self):
        """
        Synchronize the configuration file between encap and master agent,
        This action is skipped when run by an encapsulated agent.

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
        if len(self.log.handlers) > 1:
            self.log.handlers[1].setLevel(logging.CRITICAL)
        try:
            self.__push_encap_config(container)
        finally:
            if len(self.log.handlers) > 1:
                self.log.handlers[1].setLevel(rcEnv.loglevel)

    def __push_encap_config(self, container):
        """
        Compare last modification time of the master and slave service
        configuration file, and copy the most recent version over the least
        recent.
        """
        cmd = ['print', 'config', 'mtime']
        try:
            cmd_results = self._encap_cmd(cmd, container, push_config=False)
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
                    cmd_results = container.rcp_from(encap_cf, rcEnv.paths.pathetc+'/')
                else:
                    cmd = rcEnv.rcp.split() + [container.name+':'+encap_cf, rcEnv.paths.pathetc+'/']
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
        cmd_results = self._encap_cmd(cmd, container=container, push_config=False)
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

    def all_rids(self):
        return [rid for rid in self.resources_by_id if rid is not None] + \
               list(self.encap_resources.keys())

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
        for resource in self.resources_by_id.values() + self.encap_resources.values():
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
            for resource in self.resources_by_id.values() + self.encap_resources.values():
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
                if resource.standby]

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
                    raise ex.excError("invalid json in resource definition: "
                                      "%s" % options.resource[idx])

        self.options.update(options)
        options = self.options

        return options

    def options_to_rids(self, options, action):
        """
        Return the list of rids to apply an action to, from the command
        line options passed as <options>.
        """
        rid = options.get("rid", None)
        tags = options.get("tags", None)
        subsets = options.get("subsets", None)
        xtags = options.get("xtags", None)

        if rid is None:
            rid = []
        elif is_string(rid):
            rid = rid.split(',')

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
                raise ex.excAbortAction("no resource match the given --rid, --subset "
                                        "and --tags specifiers")
        else:
            # no resources certainly mean the build was done with minimal=True
            # let the action go on. 'delete', for one, takes a --rid but does
            # not need resource initialization
            rids = rid

        return rids

    def action(self, action, options=None):
        self.allow_on_this_node(action)
        try:
            options = self.prepare_options(action, options)
            self.async_action(action)
        except ex.excError as exc:
            self.log.error(exc)
            return 1
        except ex.excAbortAction:
            return 0
        return self._action(action, options=options)

    @sched_action
    def _action(self, action, options=None):
        """
        Filter resources on which the service action must act.
        Abort if the service is frozen, or if --cluster is not set on a HA
        service.
        Set up the environment variables.
        Finally do the service action either in logged or unlogged mode.
        """

        try:
            self.action_rid_before_depends = self.options_to_rids(options, action)
        except ex.excAbortAction as exc:
            self.log.error(exc)
            return 1

        depends = set()
        for rid in self.action_rid_before_depends:
            depends |= self.action_rid_dependencies(action, rid) - set(self.action_rid_before_depends)

        if len(depends) > 0:
            self.log.info("add rid %s to satisfy dependencies" % ", ".join(depends))
            self.action_rid = list(set(self.action_rid_before_depends) | depends)
        else:
            self.action_rid = list(self.action_rid_before_depends)

        self.action_rid_depends = list(depends)

        self.action_start_date = datetime.datetime.now()

        if self.node is None:
            self.node = node.Node()

        if action not in ACTIONS_NO_STATUS_CHANGE and \
           'compliance' not in action and \
           'collector' not in action and \
            not options.dry_run and \
            not action.startswith("docker"):
            #
            # here we know we will run a resource state-changing action
            # purge the resource status file cache, so that we don't take
            # decision on outdated information
            #
            self.log.debug("purge all resource status file caches before "
                           "action %s", action)
            self.purge_status_caches()

        self.setup_environ(action=action)
        self.setup_signal_handlers()
        self.set_skip_resources(keeprid=self.action_rid, xtags=options.xtags)
        if action.startswith("print_") or \
           action.startswith("collector") or \
           action.startswith("json_"):
            return self.do_print_action(action, options)
        if action in ACTIONS_NO_LOG or \
           action.startswith("compliance") or \
           action.startswith("docker") or \
           options.dry_run:
            err = self.do_action(action, options)
        else:
            err = self.do_logged_action(action, options)
        return err

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

        if options.cluster and options.format != "json":
            raise ex.excError("only the json output format is allowed with --cluster")

        if action.startswith("collector_"):
            from collector import Collector
            collector = Collector(options, self.node, self.svcname)
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
                if options.format == "json":
                    try:
                        results[nodename] = json.loads(results[nodename])
                    except (TypeError, ValueError) as exc:
                        results[nodename] = {"error": str(exc)}
            results[rcEnv.nodename] = data
            return results
        elif options.cluster:
            # no remote though --cluster is set
            results = {}
            results[rcEnv.nodename] = data
            return results

        return data

    def do_cluster_action(self, action, options=None, waitlock=60, collect=False, action_mode=True):
        """
        Execute an action on remote nodes if --cluster is set and the
        service is a flex, and this node is flex primary.

        edit config, validate config, and sync* are never executed through
        this method.

        If possible execute in parallel running subprocess. Aggregate and
        return results.
        """
        if options is None:
            options = self.options
        if not options.cluster:
            return

        if action in ("edit_config", "validate_config") or "sync" in action:
            return

        if self.topology == "flex":
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
        else:
            return

        args = [arg for arg in sys.argv[1:] if arg not in ("-c", "--cluster")]
        if options.docker_argv and len(options.docker_argv) > 0:
            args += options.docker_argv

        def wrapper(queue, **kwargs):
            """
            Execute the remote action and enqueue or print results.
            """
            collect = kwargs["collect"]
            ret, out, err = self.remote_action(**kwargs)
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
                "collect": True,
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

    def do_action(self, action, options):
        """
        Acquire the service action lock, call the service action method,
        handles its errors, and finally release the lock.

        If --cluster is set, and the service is a flex, and we are
        flex_primary run the action on all remote nodes.
        """

        if action not in ACTIONS_NO_LOCK and self.topology not in TOPOLOGIES:
            raise ex.excError("invalid cluster type '%s'. allowed: %s" % (
                self.topology,
                ', '.join(TOPOLOGIES),
            ))

        err = 0
        waitlock = convert_duration(options.waitlock)
        if waitlock < 0:
            waitlock = self.lock_timeout

        if action == "sync_all" and self.command_is_scoped():
            for rid in self.action_rid:
                resource = self.resources_by_id[rid]
                if not resource.type.startswith("sync"):
                    continue
                try:
                    resource.reslock(action=action, suffix="sync")
                except ex.excError as exc:
                    self.log.error(str(exc))
                    return 1
        else:
            try:
                self.svclock(action, timeout=waitlock)
            except ex.excError as exc:
                self.log.error(str(exc))
                return 1

        psinfo = self.do_cluster_action(action, options=options)

        def call_action(action):
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
                self.notify_action(action)
                err = call_action(action)
                if err is None:
                    err = 0
            else:
                self.log.error("unsupported action %s", action)
                err = 1
        except ex.excEndAction as exc:
            self.log.info(exc)
            err = 0
        except ex.excAbortAction as exc:
            msg = "'%s' action aborted by last resource" % action
            if len(str(exc)) > 0:
                msg += ": %s" % str(exc)
            self.log.info(msg)
            err = 0
        except ex.excError as exc:
            msg = "'%s' action stopped on execution error" % action
            self.log.debug(msg)
            msg = str(exc)
            if len(msg) > 0:
                self.log.error(msg)
            err = 1
            self.rollback_handler(action)
        except ex.excSignal:
            self.log.error("interrupted by signal")
            err = 1
        except:
            err = 1
            self.save_exc()
        finally:
            self.running_action = None
            if not (action == "delete" and not self.command_is_scoped()):
                self.update_status_data()
            self.clear_action(action, err)
            self.svcunlock()
            if action == "sync_all" and self.command_is_scoped():
                for rid in self.action_rid:
                    resource = self.resources_by_id[rid]
                    if not resource.type.startswith("sync"):
                        continue
                    resource.resunlock()

        if psinfo:
            self.join_cluster_action(**psinfo)

        return err

    def action_progress(self, action):
        progress = ACTION_ASYNC.get(action, {}).get("progress")
        if progress is None:
            return
        if action.startswith("sync"):
            progress = "syncing"
        return progress

    def notify_action(self, action):
        if os.environ.get("OSVC_ACTION_ORIGIN") == "daemon":
            return
        progress = self.action_progress(action)
        if progress is None:
            return
        local_expect = None
        if action in ("stop", "unprovision", "delete", "shutdown", "rollback") and not self.command_is_scoped():
            local_expect = "unset"
            if self.orchestrate == "ha":
                self.freeze()
        try:
            self.set_service_monitor(local_expect=local_expect, status=progress)
            self.log.debug("daemon notified of action '%s' begin" % action)
        except Exception as exc:
            self.log.warning("failed to notify action begin to the daemon: %s", str(exc))

    def clear_action(self, action, err):
        if os.environ.get("OSVC_ACTION_ORIGIN") == "daemon":
            return
        progress = self.action_progress(action)
        local_expect = None
        if progress is None:
            return
        if err:
            status = action + " failed"
        else:
            status = "idle"
            if action == "start":
                local_expect == "started"
        try:
            self.set_service_monitor(local_expect=local_expect, status=status)
            self.log.debug("daemon notified of action '%s' end" % action)
        except Exception as exc:
            self.log.warning("failed to notify action end to the daemon: %s", str(exc))

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
        rids = [r.rid for r in self.get_resources() if r.can_rollback and not r.standby]
        if len(rids) == 0:
            self.log.info("skip rollback %s: no resource activated", action)
            return
        self.log.info("trying to rollback %s on %s", action, ', '.join(rids))
        try:
            self.rollback()
        except ex.excError:
            self.log.error("rollback %s failed", action)

    def do_logged_action(self, action, options):
        """
        Setup action logging to a machine-readable temp logfile, in preparation
        to the collector feeding.
        Do the action.
        Finally, feed the log to the collector.
        """
        import tempfile
        begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Provision a database entry to store action log later
        self.node.daemon_collector_xmlrpc("begin_action", self.svcname,
                                          action, self.node.agent_version(),
                                          begin, self.options.cron)

        # Per action logfile to push to database at the end of the action
        tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.paths.pathtmp,
                                              prefix=self.svcname+'.'+action)
        actionlogfile = tmpfile.name
        tmpfile.close()
        fmt = "%(asctime)s;;%(name)s;;%(levelname)s;;%(message)s;;%(process)d;;EOL"
        actionlogformatter = logging.Formatter(fmt)
        actionlogfilehandler = logging.FileHandler(actionlogfile)
        actionlogfilehandler.setFormatter(actionlogformatter)
        actionlogfilehandler.setLevel(logging.INFO)
        self.log.addHandler(actionlogfilehandler)
        if "/svcmgr.py" in sys.argv:
            self.log.info(" ".join(sys.argv))

        err = self.do_action(action, options)

        # Push result and logs to database
        actionlogfilehandler.close()
        self.log.removeHandler(actionlogfilehandler)
        end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        rtypes = [
            "container.ovm",
            "container.hpvm",
            "container.esx",
            "container.lxd",
        ]
        self.sub_set_action(rtypes, "_migrate")

    def destination_node_sanity_checks(self, destination_node=None):
        """
        Raise an excError if
        * the destination node --to arg not set
        * the specified destination is the current node
        * the specified destination is not a service candidate node

        If the destination node is not specified and the cluster has
        only 2 nodes, consider the destination node is our peer.

        Return the validated destination node name.
        """
        if self.topology != "failover":
            raise ex.excError("this service topology is not 'failover'")
        if destination_node is None:
            destination_node = self.options.destination_node
        if destination_node is None and len(self.peers) == 2:
            nodenames = self.started_on()
            candidates = list(set(self.peers) - set(nodenames))
            if len(candidates) == 1:
                destination_node = candidates[0]
        if destination_node is None:
            raise ex.excError("a destination node must be provided this action")
        if destination_node == self.current_node():
            raise ex.excError("the destination is the source node")
        if destination_node not in self.nodes:
            raise ex.excError("the destination node %s is not in the service "
                              "nodes list" % destination_node)
        return destination_node

    @_master_action
    def migrate(self):
        """
        Service online migration.
        """
        dst = self.destination_node_sanity_checks()
        self.svcunlock()
        self._clear(nodename=rcEnv.nodename)
        self._clear(nodename=dst)
        self.daemon_mon_action("freeze", wait=True)
        src_node = self.current_node()
        self.daemon_service_action(["prstop"], nodename=src_node)
        try:
            self.daemon_service_action(["startfs", "--master"], nodename=dst)
            self._migrate()
        except:
            if len(self.get_resources('disk.scsireserv')) > 0:
                self.log.error("scsi reservations were dropped. you have to "
                               "acquire them now using the 'prstart' action "
                               "either on source node or destination node, "
                               "depending on your problem analysis.")
            raise
        self.daemon_service_action(["stop"], nodename=src_node)
        self.daemon_service_action(["prstart", "--master"], nodename=dst)

    def takeover(self):
        """
        Service move to local node.
        """
        self.destination_node_sanity_checks(rcEnv.nodename)
        self.svcunlock()
        self._clear(nodename=rcEnv.nodename)
        self._clear(nodename=self.options.destination_node)
        self.daemon_mon_action("stop", wait=True)
        self.daemon_service_action(["start"], nodename=rcEnv.nodename)
        self.daemon_mon_action("thaw", wait=True)

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
            raise ex.excError("can't scale: not a scaler")
        try:
            value = int(self.options.destination_node)
            assert value >= 0
        except Exception:
            raise ex.excError("invalid scale target: set '--to <n>' where n>=0")
        self._set("DEFAULT", "scale", str(value))
        self.set_service_monitor()

    def switch(self):
        """
        Service move to another node.
        """
        dst = self.destination_node_sanity_checks()
        self.svcunlock()
        self._clear(nodename=rcEnv.nodename)
        self._clear(nodename=dst)
        self.daemon_mon_action("stop", wait=True)
        self.daemon_service_action(["start"], nodename=dst, timeout=self.options.time)
        self.daemon_mon_action("thaw", wait=True)

    def collector_rest_get(self, *args, **kwargs):
        kwargs["svcname"] = self.svcname
        return self.node.collector_rest_get(*args, **kwargs)

    def collector_rest_post(self, *args, **kwargs):
        kwargs["svcname"] = self.svcname
        return self.node.collector_rest_post(*args, **kwargs)

    def collector_rest_put(self, *args, **kwargs):
        kwargs["svcname"] = self.svcname
        return self.node.collector_rest_put(*args, **kwargs)

    def collector_rest_delete(self, *args, **kwargs):
        kwargs["svcname"] = self.svcname
        return self.node.collector_rest_delete(*args, **kwargs)

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
                tmpfile.flush()
            shutil.move(fname, self.paths.cf)
        except (OSError, IOError) as exc:
            print("failed to write new %s (%s)" % (self.paths.cf, str(exc)),
                  file=sys.stderr)
            raise ex.excError()
        try:
            os.chmod(self.paths.cf, 0o0644)
        except (OSError, IOError) as exc:
            self.log.debug("failed to set %s mode: %s", self.paths.cf, str(exc))

    @lazy
    def config(self):
        """
        Initialize the service configuration parser object. Using an
        OrderDict type to preserve the options and sections ordering,
        if possible.

        The parser object is a opensvc-specified class derived from
        optparse.RawConfigParser.
        """
        return read_cf(self.paths.cf)

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

        def get_href(ref):
            ref = ref.strip("[]")
            try:
                response = node.urlopen(ref)
                return response.read()
            except:
                return ""

        def print_comment(comment):
            """
            Print a env keyword comment. For use in the interactive service
            create codepath.
            """
            import re
            comment = re.sub("(\[.+://.+])", lambda m: get_href(m.group(1)), comment)
            print(comment)

        for key, default_val in self.env_section_keys().items():
            if key.endswith(".comment"):
                continue
            if key in explicit_options:
                continue
            if self.config.has_option("env", key+".comment"):
                print_comment(self.config.get("env", key+".comment"))
            newval = raw_input("%s [%s] > " % (key, str(default_val)))
            if newval != "":
                self._set("env", key, newval)

    def set_disable(self, rids=None, disable=True):
        """
        Set the disable to <disable> (True|False) in the configuration file,
        * at DEFAULT level if no resources were specified
        * in each resource section if resources were specified
        """
        lines = self._read_cf().splitlines()

        if rids is None:
            rids = []

        if not self.command_is_scoped() and \
           (len(rids) == 0 or len(rids) == len(self.resources_by_id)):
            rids = ['DEFAULT']

        for rid in rids:
            if rid != 'DEFAULT' and not self.config.has_section(rid):
                self.log.error("service %s has no resource %s", self.svcname, rid)
                continue

            if disable:
                self.log.info("set %s.disable = true", rid)
                lines = self.set_line(lines, rid, "disable", "true")
            elif self.config.has_option(rid, "disable"):
                self.log.info("remove %s.disable", rid)
                lines = self.unset_line(lines, rid, "disable")

            #
            # if we set <section>.disable = <bool>,
            # remove <section>.disable@<scope> = <not bool>
            #
            if rid == "DEFAULT":
                items = self.config.defaults().items()
            else:
                items = self.config.items(rid)
            for option, value in items:
                if not option.startswith("disable@"):
                    continue
                if value == True:
                    continue
                self.log.info("remove %s.%s = false", rid, option)
                lines = self.unset_line(lines, rid, option)


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

        try:
            self._write_cf(lines)
        except (IOError, OSError) as exc:
            raise ex.excError(str(exc))
        self.unset_lazy("config")

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

    def delete_service_logs(self):
        """
        Delete the service configuration logs
        """
        import glob
        patterns = [
            os.path.join(rcEnv.paths.pathlog, self.svcname+".log*"),
            os.path.join(rcEnv.paths.pathlog, self.svcname+".debug.log*"),
            os.path.join(rcEnv.paths.pathlog, '.'+self.svcname+".log*"),
            os.path.join(rcEnv.paths.pathlog, '.'+self.svcname+".debug.log*"),
            os.path.join(self.var_d, "frozen"),
        ]
        for pattern in patterns:
            for fpath in glob.glob(pattern):
                self.log.info("remove %s", fpath)
                os.unlink(fpath)

    def delete_service_conf(self):
        """
        Delete the service configuration files
        """
        import shutil
        dpaths = [
            os.path.join(rcEnv.paths.pathetc, self.svcname+".dir"),
            os.path.join(rcEnv.paths.pathetc, self.svcname+".d"),
            os.path.join(self.var_d),
        ]
        fpaths = [
            self.paths.cf,
            os.path.join(rcEnv.paths.pathetc, self.svcname),
            os.path.join(rcEnv.paths.pathetc, self.svcname+".d"),
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
        lines = self._read_cf().splitlines()
        need_write = False

        for rid in rids:
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
            return

        buff = "\n".join(lines)

        try:
            self._write_cf(buff)
        except (IOError, OSError):
            raise ex.excError("failed to rewrite %s" % self.paths.cf)

        self.unset_lazy("config")

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
                if rcEnv.nodename in self.nodes:
                    self.set_multi([
                       "nodes="+rcEnv.nodename,
                       "drpnodes=",
                    ])
                elif rcEnv.nodename in self.drpnodes:
                    self.set_multi([
                       "drpnodes="+rcEnv.nodename,
                       "nodes=",
                    ])
                self.svcunlock()
                for peer in self.peers:
                    if peer == rcEnv.nodename:
                        continue
                    self.daemon_service_action([
                        "set",
                        "--kw", "nodes-=" + rcEnv.nodename,
                        "--kw", "drpnodes-=" + rcEnv.nodename,
                    ], nodename=peer, sync=False)
            self.delete_service_conf()
            self.delete_service_logs()
        else:
            self.delete_resources()

    def docker(self):
        """
        The 'docker' action entry point.
        Parse the docker argv and substitute known patterns before relaying
        the argv to the docker command.
        Set the socket to point the service-private docker daemon if
        the service has such a daemon.
        """
        import subprocess
        containers = self.get_resources('container.docker')
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
                                 if not resource.skip and not resource.is_disabled()]
                    for instance in instances:
                        argv.insert(idx, instance)
            for idx, arg in enumerate(argv):
                if arg in ("%images%", "{images}"):
                    del argv[idx]
                    images = list(set([resource.run_image for resource in containers
                                       if not resource.skip and not resource.is_disabled()]))
                    for image in images:
                        argv.insert(idx, image)
            for idx, arg in enumerate(argv):
                if arg in ("%as_service%", "{as_service}"):
                    del argv[idx]
                    argv[idx:idx] = ["-u", self.svcname+"@"+rcEnv.nodename]
                    argv[idx:idx] = ["-p", self.node.config.get("node", "uuid")]
                    if self.dockerlib.docker_min_version("1.12"):
                        pass
                    elif self.dockerlib.docker_min_version("1.10"):
                        argv[idx:idx] = ["--email", self.svcname+"@"+rcEnv.nodename]
            for idx, arg in enumerate(argv):
                if re.match(r'\{container#\w+\}', arg):
                    container_rid = arg.strip("{}")
                    if container_rid not in self.resources_by_id:
                        continue
                    container = self.resources_by_id[container_rid]
                    if container.docker_service:
                        name = container.service_name
                    else:
                        name = container.container_name
                    del argv[idx]
                    argv.insert(idx, name)
            return argv

        if len(containers) == 0:
            print("this service has no docker resource", file=sys.stderr)
            return 1

        self.dockerlib.docker_start(verbose=False)
        cmd = self.dockerlib.docker_cmd + subst(self.options.docker_argv)
        proc = subprocess.Popen(cmd)
        proc.communicate()
        return proc.returncode

    def freezestop(self):
        """
        The freezestop monitor action.
        """
        self.freeze()
        self.stop()

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
        Pull a service configuration from the collector, installs it and
        create the svcmgr link.
        """
        data = self.node.collector_rest_get("/services/"+self.svcname+"?props=svc_config&meta=0")
        if "error" in data:
            raise ex.excError(data["error"])
        if len(data["data"]) == 0:
            raise ex.excError("service not found on the collector")
        if data["data"][0]["svc_config"] is None:
            raise ex.excError("service has an empty configuration on the collector")
        if len(data["data"][0]["svc_config"]) == 0:
            raise ex.excError("service has an empty configuration on the collector")
        buff = data["data"][0]["svc_config"].replace("\\n", "\n").replace("\\t", "\t")
        import codecs
        with codecs.open(self.paths.cf, "w", "utf8") as ofile:
            ofile.write(buff)
            ofile.flush()
        self.log.info("%s pulled", self.paths.cf)
        self.node.install_service_files(self.svcname)

        if self.options.provision:
            self.action("provision")

    def save_exc(self):
        """
        A helper method to save stacks in the service log.
        """
        self.log.error("unexpected error. stack saved in the service debug log")
        import traceback
        buff = traceback.format_exc()
        for line in buff.splitlines():
            self.log.debug(line)

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
        """
        The 'update' action entry point.
        Add resources to the service configuration, and provision them if
        instructed to do so.
        """
        from keywords import MissKeyNoDefault, KeyInvalidValue

        sections = {}
        rtypes = {}
        defaults = self.config.defaults()
        for section in self.config.sections():
            sections[section] = {}
            elements = section.split('#')
            if len(elements) == 2:
                rtype = elements[0]
                ridx = elements[1]
                if rtype not in rtypes:
                    rtypes[rtype] = set()
                rtypes[rtype].add(ridx)
            for option, value in self.config.items(section):
                if option in list(defaults.keys()) + ['rtype']:
                    continue
                sections[section][option] = value

        import svcBuilder

        rid = []

        for data in self.options.resource:
            is_resource = False
            if 'rid' in data:
                section = data['rid']
                if '#' not in section:
                    raise ex.excError("%s must be formatted as 'rtype#n'" % section)
                elements = section.split('#')
                if len(elements) != 2:
                    raise ex.excError("%s must be formatted as 'rtype#n'" % section)
                del data['rid']
                if section in sections:
                    sections[section].update(data)
                else:
                    sections[section] = data
                is_resource = True
            elif 'rtype' in data and data["rtype"] == "env":
                del data["rtype"]
                if "env" in sections:
                    sections["env"].update(data)
                else:
                    sections["env"] = data
            elif 'rtype' in data and data["rtype"] != "DEFAULT":
                section = self.allocate_rid(data['rtype'], sections)
                self.log.info("allocated rid %s" % section)
                del data['rtype']
                sections[section] = data
                is_resource = True
            else:
                if "rtype" in data:
                    del data["rtype"]
                defaults.update(data)

            if is_resource:
                try:
                    sections[section].update(self.kwdict.KEYS.update(section, data))
                except (MissKeyNoDefault, KeyInvalidValue) as exc:
                    if not self.options.interactive:
                        raise ex.excError(str(exc))
                rid.append(section)

        for section, data in sections.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            for key, val in data.items():
                self.config.set(section, key, val)

        self.write_config()

        for section in rid:
            group = section.split("#")[0]
            svcBuilder.add_resource(self, group, section)

        if self.options.provision and len(rid) > 0:
            options = Storage(self.options)
            options.rid = rid
            self.action("provision", options)

    def allow_on_this_node(self, action):
        """
        Raise excError if the service is not allowed to run on this node.
        In other words, the nodename is not a service node or drpnode, nor the
        service mode is cloud proxy.
        """
        if action in ACTIONS_ALLOW_ON_INVALID_NODE:
            return
        if self.svc_env != 'PRD' and rcEnv.node_env == 'PRD':
            raise ex.excError('not allowed to run on this node (svc env=%s node env=%s)' % (self.svc_env, rcEnv.node_env))
        if self.type in rcEnv.vt_cloud:
            return
        if rcEnv.nodename in self.nodes:
            return
        if rcEnv.nodename in self.drpnodes:
            return
        raise ex.excError("action '%s' aborted because this node's hostname "
                          "'%s' is not a member of DEFAULT.nodes, "
                          "DEFAULT.drpnode nor DEFAULT.drpnodes" % \
                          (action, rcEnv.nodename))

    def logs(self, nodename=None):
        try:
            self._logs(nodename=nodename)
        except ex.excSignal:
            return
        except (OSError, IOError) as exc:
            if exc.errno == 32:
                # broken pipe
                return

    def _logs(self, nodename=None):
        if nodename is None:
            nodename = self.options.node
        if self.options.local:
            nodes = [rcEnv.nodename]
        elif self.options.node:
            nodes = [self.options.node]
        else:
            nodes = self.peers
        from rcColor import colorize_log_line
        lines = []
        for nodename in nodes:
            lines += self.daemon_backlogs(nodename)
            for line in sorted(lines):
                line = colorize_log_line(line)
                if line:
                    print(line)
        if not self.options.follow:
            return
        for line in self.daemon_logs(nodes):
            line = colorize_log_line(line)
            if line:
                print(line)

    def placement_optimal(self, data=None):
        if data is None:
            data = self.node._daemon_status()
        placement = data.get("monitor").get("services").get(self.svcname).get("placement")
        if placement == "optimal":
            return True
        return False

    #########################################################################
    #
    # daemon communications
    #
    #########################################################################
    def daemon_backlogs(self, nodename):
        options = {
            "svcname": self.svcname,
            "backlog": self.options.backlog,
            "debug": self.options.debug,
        }
        for lines in self.daemon_get_stream(
            {"action": "service_logs", "options": options},
            nodename=nodename,
        ):
            if lines is None:
                break
            for line in lines:
                yield line

    def daemon_logs(self, nodes=None):
        options = {
            "svcname": self.svcname,
            "backlog": 0,
        }
        for lines in self.daemon_get_streams(
            {"action": "service_logs", "options": options},
            nodenames=nodes,
        ):
            if lines is None:
                break
            for line in lines:
                yield line

    def abort(self):
        pass

    def clear(self):
        if self.options.local:
           self._clear()
        elif self.options.node:
           self._clear(self.options.node)
        else:
           cleared = 0
           for nodename in self.peers:
               try:
                   self._clear(nodename)
               except ex.excError as exc:
                   self.log.warning(exc)
                   continue
               cleared += 1
           if cleared < len(self.peers):
               raise ex.excError("cleared on %d/%d nodes" % (cleared, len(self.peers)))

    def _clear(self, nodename=None):
        options = {
            "svcname": self.svcname,
        }
        data = self.daemon_send(
            {"action": "clear", "options": options},
            nodename=nodename,
        )
        if data is None or data["status"] != 0:
            raise ex.excError("clear on node %s failed" % nodename)

    def wake_monitor(self):
        options = {
            "svcname": self.svcname,
        }
        try:
            data = self.daemon_send(
                {"action": "wake_monitor", "options": options},
                nodename=self.options.node,
                silent=True,
            )
            if data and data["status"] != 0:
                self.log.warning("wake monitor failed")
        except Exception as exc:
            self.log.warning("wake monitor failed: %s", str(exc))

    def set_service_monitor(self, status=None, local_expect=None, global_expect=None, stonith=None, svcname=None):
        if svcname is None:
            svcname = self.svcname
        options = {
            "svcname": svcname,
            "status": status,
            "local_expect": local_expect,
            "global_expect": global_expect,
            "stonith": stonith,
        }
        try:
            data = self.daemon_send(
                {"action": "set_service_monitor", "options": options},
                nodename=self.options.node,
                silent=True,
            )
            if data and data["status"] != 0:
                self.log.warning("set monitor status failed")
        except Exception as exc:
            self.log.warning("set monitor status failed: %s", str(exc))

    def daemon_service_action(self, cmd, nodename=None, sync=True, timeout=0, collect=False, action_mode=True):
        """
        Execute a service action on a peer node.
        If sync is set, wait for the action result.
        """
        if timeout is not None:
            timeout = convert_duration(timeout)
        if nodename is None:
            nodename = self.options.node
        if nodename not in self.node.cluster_nodes:
            try:
                secret = self.node.conf_get("cluster", "secret", impersonate=nodename)
            except:
                raise ex.excError("unknown cluster secret to communicate with node %s" % nodename)
            try:
                cluster_name = self.node.conf_get("cluster", "name", impersonate=nodename)
            except:
                raise ex.excError("unknown cluster name to communicate with node %s" % nodename)
        else:
            secret = self.cluster_key
            cluster_name = None


        options = {
            "svcname": self.svcname,
            "cmd": cmd,
            "sync": sync,
            "action_mode": action_mode,
        }
        if action_mode:
            self.log.info("request action '%s' on node %s", " ".join(cmd), nodename)
        try:
            data = self.daemon_send(
                {"action": "service_action", "options": options},
                nodename=nodename,
                silent=True,
                timeout=timeout,
                secret=secret,
                cluster_name=cluster_name,
            )
        except Exception as exc:
            self.log.error("post service action on node %s failed: %s",
                           nodename, exc)
            return 1
        if data is None or data["status"] != 0:
            self.log.error("post service action on node %s failed",
                           nodename)
            return 1
        if "data" not in data:
            return 0
        data = data["data"]
        if collect:
            return data["ret"], data.get("out", ""), data.get("err", "")
        else:
            if data.get("out") and len(data["out"]) > 0:
                for line in data["out"].splitlines():
                   print(line)
            if data.get("err") and len(data["err"]) > 0:
                for line in data["err"].splitlines():
                   print(line, file=sys.stderr)
            return data["ret"]

    def get_pg_settings(self, s):
        d = {}
        options = (
            "cpus",
            "cpu_shares",
            "cpu_quota",
            "mems",
            "mem_oom_control",
            "mem_limit",
            "mem_swappiness",
            "vmem_limit",
            "blkio_weight",
        )

        for option in options:
            try:
                d[option] = self.conf_get(s, "pg_"+option)
            except ex.OptNotFound as exc:
                pass

        return d

    @lazy
    def pg_settings(self):
        return self.get_pg_settings("DEFAULT")

    @lazy
    def slave_num(self):
        try:
            return int(self.svcname.split(".")[0])
        except ValueError:
            return 0

    def unset_lazy(self, prop):
        """
        Expose the unset_lazy(self, ...) utility function as a method,
        so Node() users don't have to import it from rcUtilities.
        """
        unset_lazy(self, prop)

    def snooze(self):
        """
        Snooze notifications on the service.
        """
        if self.options.duration is None:
            print("set --duration", file=sys.stderr)
            raise ex.excError
        try:
            data = self.collector_rest_post("/services/self/snooze", {
                "duration": self.options.duration,
            })
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])
        print(data.get("info", ""))

    def unsnooze(self):
        """
        Unsnooze notifications on the service.
        """
        try:
            data = self.collector_rest_post("/services/self/snooze")
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(data["error"])
        print(data.get("info", ""))


