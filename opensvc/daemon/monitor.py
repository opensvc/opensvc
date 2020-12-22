"""
Monitor Thread
"""
import codecs
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from itertools import chain

import daemon.shared as shared
from core.freezer import Freezer
from env import Env
from foreign.six.moves import queue
from utilities.naming import (factory, fmt_path, list_services,
                              resolve_path, split_path, svc_pathcf,
                              svc_pathvar)
from utilities.files import makedirs, fsum
from utilities.cache import purge_cache
from utilities.storage import Storage

STARTED_STATES = [
    "n/a",
    "up",
]
STOPPED_STATES = [
    "n/a",
    "down",
    "stdby up",
    "stdby down",
    "unknown",     # slavers with deleted slaves
    None,          # base-kind services
]
SHUTDOWN_STATES = [
    "n/a",
    "down",
    "stdby down",
    "unknown",     # slavers with deleted slaves
    None,          # base-kind services
]
ORCHESTRATE_STATES = (
    "ready",
    "idle",
    "wait children",
    "wait parents",
    "wait sync",
    "wait leader",
    "wait non-leader",
)
ABORT_STATES = (
    ("thaw failed", "thawed"),
    ("freeze failed", "frozen"),
    ("start failed", "started"),
    ("stop failed", "stopped"),
    ("stop failed", "purged"),
    ("stop failed", "unprovisioned"),
    ("shutdown failed", "shutdown"),
    ("delete failed", "deleted"),
    ("unprovision failed", "unprovisioned"),
    ("provision failed", "provisioned"),
    ("place failed", "placed"),
    ("purge failed", "purged"),
)
NON_LEADER_ABORT_STATES = (
    ("stop failed", "placed@"),
    ("stop failed", "placed"),
)
LEADER_ABORT_STATES = (
    ("start failed", "placed"),
    ("start failed", "placed@"),
)

ETC_NS_SKIP = len(os.path.join(Env.paths.pathetcns, ""))

#import cProfile
#import pstats
#pr = cProfile.Profile()

#def start_profile():
#    pr.enable()

#def stop_profile():
#    pr.disable()
#    ps = pstats.Stats(pr).sort_stats(2)
#    ps.print_stats()

class Defer(Exception):
    """
    Raised from orchestration routines to signal the condition to
    start the next step are not satisfied yet.
    """
    pass

class MonitorObjectOrchestratorManualMixin(object):
    def object_orchestrator_manual(self, svc, smon, status):
        """
        Take actions to meet global expect target, set by user or by
        object_orchestrator_auto()
        """
        if smon.global_expect is None:
            return
        instance = self.get_service_instance(svc.path, Env.nodename)
        kwargs = dict(svc=svc, smon=smon, status=status, instance=instance)
        try:
            base_global_expect, target = smon.global_expect.split("@", 1)
            fnname = "_oom_{ge}_at".format(ge=base_global_expect)
            kwargs["target"] = target
        except Exception:
            fnname = "_oom_{ge}".format(ge=smon.global_expect)
        try:
            fn = getattr(self, fnname)
        except AttributeError:
            self.log.warning("unsupported global expect on %s: %s",
                             svc.path, smon.global_expect)
            self.set_smon(global_expect="unset")
            return
        try:
            fn(**kwargs)
        except Defer as exc:
            self.log.debug("%s %s defer %s", svc.path, fnname, exc.args[0])

    def _oom_frozen(self, svc=None, smon=None, status=None, instance=None):
        if not self.instance_frozen(svc.path):
            self.event("instance_freeze", {
                "reason": "target",
                "path": svc.path,
                "monitor": smon,
            })
            self.service_freeze(svc.path)

    def _oom_thawed(self, svc=None, smon=None, status=None, instance=None):
        if self.instance_frozen(svc.path):
            self.event("instance_thaw", {
                "reason": "target",
                "path": svc.path,
                "monitor": smon,
            })
            self.service_thaw(svc.path)

    def _oom_shutdown(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_children():
            if not self.children_down(svc):
                self.set_smon(svc.path, status="wait children")
                raise Defer("wait: children are not stopped yet")
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")
        
        def step_freeze():
            if self.instance_frozen(svc.path):
                return
            self.event("instance_freeze", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_freeze(svc.path)
            raise Defer("freeze: action started")

        def step_shutdown():
            if self.is_instance_shutdown(instance):
                return
            thawed_on = self.service_instances_thawed(svc.path)
            if thawed_on:
                self.duplog("info", "service %(path)s still has thawed "
                            "instances on nodes %(thawed_on)s, delay "
                            "shutdown",
                            path=svc.path,
                            thawed_on=", ".join(thawed_on))
            else:
                self.service_shutdown(svc.path)

        step_wait_children()
        step_freeze()
        step_shutdown()

    def _oom_stopped(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_children():
            if not self.children_down(svc):
                self.set_smon(svc.path, status="wait children")
                raise Defer("wait: children are not stopped yet")
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")
        
        def step_freeze():
            if self.instance_frozen(svc.path):
                return
            self.log.info("freeze service %s", svc.path)
            self.event("instance_freeze", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_freeze(svc.path)
            raise Defer("freeze: action started")

        def step_stop():
            if instance.avail in STOPPED_STATES:
                return
            thawed_on = self.service_instances_thawed(svc.path)
            if thawed_on:
                self.duplog("info", "service %(path)s still has thawed instances "
                            "on nodes %(thawed_on)s, delay stop",
                            path=svc.path,
                            thawed_on=", ".join(thawed_on))
            else:
                self.event("instance_stop", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_stop(svc.path)

        step_wait_children()
        step_freeze()
        step_stop()

    def _oom_started(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_parents():
            if not self.parents_available(svc):
                self.set_smon(svc.path, status="wait parents")
                raise Defer("wait: parents are not started yet")
            elif smon.status == "wait parents":
                self.set_smon(svc.path, status="idle")

        def step_thaw():
            if not self.instance_frozen(svc.path):
                return
            self.event("instance_thaw", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_thaw(svc.path)
            raise Defer("thaw: action started")

        def step_start():
            if status in STARTED_STATES:
                return
            agg = self.get_service_agg(svc.path)
            if agg.frozen != "thawed":
                raise Defer("start: instance is not thawed yet")
            self.object_orchestrator_auto(svc, smon, status)
            raise Defer("start: action started")

        step_thaw()
        step_wait_parents()
        step_start()

    def _oom_unprovisioned(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_status():
            if smon.status not in ("unprovisioning", "stopping"):
                return
            raise Defer("wait: incompatible monitor status: %s" % smon.status)

        def step_wait_done():
            if svc.path in shared.SERVICES and not self.instance_unprovisioned(instance):
                return
            if smon.status != "idle":
                self.set_smon(svc.path, status="idle")
            raise Defer("wait: instance does not exist or already unprovisioned")

        def step_set_wait_children():
            if self.children_unprovisioned(svc):
                return
            self.set_smon(svc.path, status="wait children")
            raise Defer("wait: set wait children")

        def step_stop():
            if instance.avail in STOPPED_STATES:
                return
            self.event("instance_stop", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_stop(svc.path, force=True)
            raise Defer("stop: action started")

        def step_wait_stopped():
            agg = self.get_service_agg(svc.path)
            if agg.avail in STOPPED_STATES:
                return
            raise Defer("wait: local instance not stopped yet")

        def step_wait_children():
            if smon.status != "wait children":
                return
            if self.children_unprovisioned(svc):
                return
            raise Defer("wait: children are not unprovisioned yet")

        def step_wait_non_leader():
            if smon.status != "wait non-leader":
                return
            if self.leader_last(svc, provisioned=False, silent=True):
                return
            self.log.info("service %s still waiting non leaders", svc.path)
            raise Defer("wait: non-leader instances are not unprovisioned yet")

        def step_set_wait_non_leader():
            leader = self.leader_last(svc, provisioned=False)
            if leader:
                return leader
            self.set_smon(svc.path, status="wait non-leader")
            raise Defer("wait: set wait non-leader")

        def step_unprovision():
            self.event("instance_unprovision", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_unprovision(svc, leader)

        step_wait_status()
        step_wait_done()
        step_set_wait_children()
        step_stop()
        step_wait_stopped()
        step_wait_children()
        leader = step_wait_non_leader() # pylint: disable=assignment-from-none
        step_set_wait_non_leader()
        step_unprovision()

    def _oom_provisioned(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_parents():
            if smon.status == "wait parents":
                if not self.parents_available(svc):
                    raise Defer("wait: parents not available")

        def step_wait_leader():
            if smon.status == "wait leader":
                if not self.leader_first(svc, provisioned=True, silent=True):
                    raise Defer("wait: leader not provisioned yet")

        def step_wait_sync():
            if smon.status == "wait sync":
                if not self.min_instances_reached(svc):
                    raise Defer("wait: the object configuration is not synced yet")

        def step_provision():
            if self.instance_provisioned(instance):
                self.set_smon(svc.path, status="idle")
                raise Defer("provision: instance already provisioned")
            if not self.min_instances_reached(svc):
                self.set_smon(svc.path, status="wait sync")
                raise Defer("provision: set wait sync")
            if self.is_natural_leader(svc) and not self.parents_available(svc):
                self.set_smon(svc.path, status="wait parents")
                raise Defer("provision: set wait parents")
            if not self.leader_first(svc, provisioned=True):
                self.set_smon(svc.path, status="wait leader")
                raise Defer("provision: set wait leader")
            self.event("instance_provision", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_provision(svc)
            raise Defer("provision: action started")

        step_wait_parents()
        step_wait_leader()
        step_wait_sync()
        step_provision()

    def _oom_deleted(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_children():
            if self.children_down(svc):
                if smon.status == "wait children":
                    self.set_smon(svc.path, status="idle")
                return
            self.set_smon(svc.path, status="wait children")
            raise Defer("wait: children are not down yet")

        def step_delete():
            if svc.path not in shared.SERVICES:
                raise Defer("delete: object does not exist")
            self.event("instance_delete", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_delete(svc.path)
            raise Defer("delete: action started")

        step_wait_children()
        step_delete()

    def _oom_purged(self, svc=None, smon=None, status=None, instance=None):
        def step_wait_status():
            if smon.status not in ("purging", "deleting", "stopping"):
                return
            raise Defer("wait: incompatible monitor status: %s" % smon.status)

        def step_wait_children():
            if self.children_unprovisioned(svc):
                return
            self.set_smon(svc.path, status="wait children")
            raise Defer("wait: children are not unprovisioned yet")

        def step_stop():
            if instance.avail in STOPPED_STATES:
                return
            self.event("instance_stop", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_stop(svc.path, force=True)
            raise Defer("stop: action started")

        def step_purge():
            agg = self.get_service_agg(svc.path)
            if agg.avail not in STOPPED_STATES:
                raise Defer("purge: object is not stopped")
            if smon.status == "wait children":
                if not self.children_unprovisioned(svc):
                    raise Defer("purge: waiting children")
            elif smon.status == "wait non-leader":
                if not self.leader_last(svc, provisioned=False, silent=True, deleted=True):
                    raise Defer("purge: waiting non-leader")
            leader = self.leader_last(svc, provisioned=False, deleted=True)
            if not leader:
                self.set_smon(svc.path, status="wait non-leader")
                raise Defer("purge: wait non-leader")
            if svc.path in shared.SERVICES and svc.kind not in ("vol", "svc"):
                # base services do not implement the purge action
                self.event("instance_delete", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_delete(svc.path)
                raise Defer("purge: delete action started")
            if svc.path not in shared.SERVICES or not instance:
                if smon.status != "idle":
                    self.set_smon(svc.path, status="idle")
                raise Defer("purge: object or instance does not exist")
            self.event("instance_purge", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_purge(svc, leader)
            raise Defer("purge: action started")

        step_wait_status()
        step_wait_children()
        step_stop()
        step_purge()

    def _oom_aborted(self, svc=None, smon=None, status=None, instance=None):
        if smon.local_expect in (None, "started"):
            return
        self.event("instance_abort", {
            "reason": "target",
            "path": svc.path,
        })
        self.set_smon(svc.path, local_expect="unset")

    def _oom_placed(self, svc=None, smon=None, status=None, instance=None):
        """
        Flex start new instances before stopping the old.
        Failover stop old instance before stopping the new.
        """
        # refresh smon for placement attr change caused by a clear
        smon = self.get_service_monitor(svc.path)

        def step_thaw():
            if not self.instance_frozen(svc.path):
                return
            self.event("instance_thaw", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_thaw(svc.path)
            raise Defer("thaw: action started")

        def step_stop():
            if smon.placement == "leader":
                return
            if not self.has_leader(svc):
                raise Defer("stop: no peer node can takeover")
            if instance.avail in STOPPED_STATES:
                raise Defer("stop: local instance already stopped")
            if svc.topology == "flex" and not self.leaders_started(svc.path):
                raise Defer("start: flex leader instances not started")
            self.event("instance_stop", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_stop(svc.path)
            raise Defer("stop: action started")

        def step_start():
            if instance.avail in STARTED_STATES:
                return
            if svc.topology == "failover" and not self.non_leaders_stopped(svc.path):
                raise Defer("start: failover non-leader instances not stopped")
            agg = self.get_service_agg(svc.path)
            if agg.placement in ("optimal", "n/a") and agg.avail == "up":
                raise Defer("start: aggregate placement is optimal and avail up")
            self.object_orchestrator_auto(svc, smon, status)
            raise Defer("start: action started")

        step_thaw()
        step_stop()
        step_start()

    def _oom_placed_at(self, svc=None, smon=None, status=None, instance=None, target=None):
        """
        Flex start new instances before stopping the old.
        Failover stop old instance before stopping the new.
        """
        target = target.split(",")
        candidates = self.placement_candidates(
            svc, discard_frozen=False,
            discard_overloaded=False,
            discard_unprovisioned=False,
            discard_constraints_violation=False,
            discard_start_failed=False,
            discard_affinities=False,
        )
        def step_thaw():
            if not self.instance_frozen(svc.path):
                return
            self.event("instance_thaw", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_thaw(svc.path)
            raise Defer("thaw: action started")

        def step_stop():
            if Env.nodename in target:
                return
            if instance.avail in STOPPED_STATES:
                return
            if svc.topology == "flex" and not self.instances_started(svc.path, set(svc.peers) & set(target)):
                raise Defer("stop: flex destination instances not started")
            if smon.status == "stop failed":
                raise Defer("stop: local instance blocking mon status (%s)" % smon.status)
            if not (set(target) & set(candidates)):
                raise Defer("stop: no candidate to takeover")
            self.event("instance_stop", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_stop(svc.path)
            raise Defer("stop: action started")

        def step_wait_parents():
            dep = self.parents_available(svc)
            if not dep and instance.avail not in STARTED_STATES:
                self.set_smon(svc.path, status="wait parents")
                raise Defer("wait: parents are not started yet")
            if smon.status == "wait parents":
                if dep or instance.avail in STARTED_STATES:
                    self.set_smon(svc.path, status="idle")

        def step_start():
            if Env.nodename not in target:
                return
            if instance.avail in STARTED_STATES:
                return
            if svc.topology == "failover" and not self.instances_stopped(svc.path, set(svc.peers) - set(target)):
                raise Defer("start: failover source instances not stopped")
            if Env.nodename not in target:
                raise Defer("start: not a target")
            if instance.avail not in STOPPED_STATES + ["warn"]:
                raise Defer("start: local instance not stopped or warn")
            if smon.status in ("start failed", "place failed"):
                raise Defer("start: local instance blocking mon status (%s)" % smon.status)
            self.event("instance_start", {
                "reason": "target",
                "path": svc.path,
            })
            err_status="place failed" if smon.global_expect == "placed" else "start failed"
            self.service_start(svc.path, err_status=err_status)
            raise Defer("start: action started")

        step_thaw()
        step_stop()
        step_wait_parents()
        step_start()

class Monitor(shared.OsvcThread, MonitorObjectOrchestratorManualMixin):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
    name = "monitor"
    monitor_period = 0.5
    arbitrators_check_period = 60
    max_shortloops = 30
    default_stdby_nb_restart = 2
    arbitrators_data = None
    last_arbitrator_ping = 0

    def __init__(self):
        shared.OsvcThread.__init__(self)
        self._shutdown = False
        self.compat = True
        self.last_node_data = None
        self.init_steps = set()

    def init(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.monitor"), {"node": Env.nodename, "component": self.name})
        self.event("monitor_started")
        self.startup = time.time()
        self.rejoin_grace_period_expired = False
        self.shortloops = 0
        self.unfreeze_when_all_nodes_joined = False
        self.node_frozen = self.freezer.node_frozen()
        self.init_data()

        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            if not self.node_frozen:
                self.event("node_freeze", data={"reason": "upgrade"})
                self.unfreeze_when_all_nodes_joined = True
                self.freezer.node_freeze()
                self.node_frozen = self.freezer.node_frozen()

        last_boot_id = shared.NODE.last_boot_id()
        boot_id = shared.NODE.asset.get_boot_id()
        try:
            # align float precision (py2/3 use different precision for mtime)
            last_boot_id = "%.02f" % float(last_boot_id)
            boot_id = "%.02f" % float(boot_id)
        except (TypeError, ValueError):
            pass
        self.log.info("boot id %s, last %s", boot_id, last_boot_id)
        self.wait_listener()
        if last_boot_id in (None, boot_id):
            self.services_init_status()
        else:
            self.kern_freeze()
            self.services_init_boot()
        shared.NODE.write_boot_id()
        self.dump_nodes_info()

        # send a first message without service status, so the peers know
        # we are in init state.
        self.update_hb_data()

    def wait_listener(self):
        while True:
            lsnr = shared.THREADS.get("listener")
            if lsnr and lsnr.stage == "ready":
                break
            time.sleep(0.2)

    def init_data(self):
        self._update_cluster_data()
        shared.GEN = 0
        initial_data = {
            "compat": shared.COMPAT_VERSION,
            "api": shared.API_VERSION,
            "agent": shared.NODE.agent_version,
            "monitor": {
                "status": "init",
                "status_updated": time.time(),
            },
            "labels": shared.NODE.labels,
            "targets": shared.NODE.targets,
            "services": {
                "status": {},
                "config": {},
            },
            "gen": {
            },
            "config": {
                "csum": shared.NODE.nodeconf_csum(),
            },
        }
        self.node_data.set([], initial_data)
        for nodename in self.cluster_nodes:
            self.nodes_data.setnx([nodename], {})

    def run(self):
        try:
            self.init()
        except Exception as exc:
            self.log.exception(exc)
            raise
        try:
            while True:
                self.do()
                if self.stopped():
                    self.join_threads()
                    self.kill_procs()
                    sys.exit(0)
        except Exception as exc:
            self.log.exception(exc)

    def transition_count(self):
        count = 0
        for path, smon in self.iter_local_services_monitors():
            if smon.status and smon.status != "scaling" and smon.status.endswith("ing"):
                count += 1
        return count

    def set_next(self, timeout):
        """
        Set the shortloop counter to the value meaning the long loop will
        be run at most in <timeout> seconds.
        """
        target = self.max_shortloops - (timeout // self.monitor_period)
        if target < 0:
            target = 0
        elif target < self.shortloops:
            return
        self.shortloops = target

    def add_service(self, path):
        name, namespace, kind = split_path(path)
        svc = factory(kind)(name, namespace, node=shared.NODE, log_handlers=[])
        svc.configure_scheduler()
        shared.SERVICES[path] = svc
        return svc

    def reconfigure(self):
        """
        The node config references may have changed, update the services objects.
        """
        self._update_cluster_data()
        shared.NODE.unset_lazy("labels")
        self.node_data.set(["labels"], shared.NODE.labels)
        self.node_data.set(["config"], {"csum": shared.NODE.nodeconf_csum()})
        self.on_nodes_info_change()
        for path in [p for p in shared.SERVICES]:
            try:
                self.add_service(path)
            except Exception as exc:
                continue

    def do(self):
        terminated = self.janitor_procs() + self.janitor_threads()
        changed = self.merge_rx()
        changed |= self.mon_changed()
        if self.get_node_monitor().status == "init" and self.services_have_init_status():
            self.set_nmon(status="rejoin")
            self.rejoin_grace_period_expired = False
        if terminated == 0 and not changed and self.shortloops < self.max_shortloops:
            self.shortloops += 1
            if self.shortloops == self.max_shortloops:
                # we're very idle, take time to ...
                self.update_completions()
            with shared.MON_TICKER:
                shared.MON_TICKER.wait(self.monitor_period)
            return
        self.node_frozen = self.freezer.node_frozen()
        if changed:
            with shared.MON_TICKER:
                #self.log.debug("woken for:")
                #for idx, reason in enumerate(shared.MON_CHANGED):
                #    self.log.debug("%d. %s", idx, reason)
                self.unset_mon_changed()
        self.shortloops = 0
        self.reload_config()
        if self._shutdown:
            if len(self.procs) == 0:
                self.stop()
        else:
            self.update_cluster_data()
            self.orchestrator()
        self.update_hb_data()
        shared.wake_collector()

    #########################################################################
    #
    # Service config exchange
    #
    #########################################################################
    def sync_services_conf(self):
        """
        For each service, decide if we have an outdated configuration file
        and fetch the most recent one if needed.
        """
        confs = self.get_services_configs()
        for path, data in confs.items():
            try:
                shared.SERVICES[path]
                new_service = False
            except KeyError:
                new_service = True
            if self.has_instance_with(path, global_expect=["purged", "deleted"]):
                continue
            if Env.nodename not in data:
                # need to check if we should have this config ?
                new_service = True
            if new_service:
                ref_conf = Storage({
                    "csum": "",
                    "updated": 0,
                })
                ref_nodename = Env.nodename
            else:
                ref_conf = data[Env.nodename]
                ref_nodename = Env.nodename
            for nodename, conf in data.items():
                if Env.nodename == nodename:
                    continue
                if new_service and Env.nodename not in conf.get("scope", []):
                    # we are not a service node
                    continue
                if conf.csum != ref_conf.csum and \
                   conf.updated > ref_conf.updated:
                    ref_conf = conf
                    ref_nodename = nodename
            if not new_service and ref_conf.scope and Env.nodename not in ref_conf.scope:
                smon = self.get_service_monitor(path)
                if not smon or smon.status == "deleting":
                    continue
                self.log.info("node %s has the most recent %s config, "
                              "which no longer defines %s as a node.",
                              ref_nodename, path, Env.nodename)
                #self.event("instance_stop", {
                #    "reason": "relayout",
                #    "path": svc.path,
                #})
                #self.service_stop(path, force=True)
                self.service_delete(path)
                continue
            if ref_nodename == Env.nodename:
                # we already have the most recent version
                continue
            try:
                svc = shared.SERVICES[path]
                if Env.nodename in svc.nodes and ref_nodename in svc.drpnodes:
                    # don't fetch drp config from prd nodes
                    continue
            except KeyError:
                pass
            self.log.info("node %s has the most recent %s config",
                          ref_nodename, path)
            self.fetch_service_config(path, ref_nodename)
            if new_service:
                self.init_new_service(path)
            else:
                self.service_status_fallback(path)

    def init_new_service(self, path):
        try:
            self.add_service(path)
        except Exception as exc:
            self.log.error("unbuildable service %s fetched: %s", path, exc)
            return

        try:
            svc = shared.SERVICES[path]
            if svc.kind == "svc":
                self.event("instance_freeze", {
                    "reason": "install",
                    "path": svc.path,
                })
                Freezer(path).freeze()
            if not os.path.exists(svc.paths.cf):
                return
            self.service_status_fallback(svc.path)
        except Exception:
            # can happen when deleting the service
            pass

    def fetch_service_config(self, path, nodename):
        """
        Fetch and install the most recent service configuration file, using
        the remote node listener.
        """
        req = {
            "action": "object_config",
            "options": {
                "path": path,
            },
        }
        resp = self.daemon_get(req, server=nodename)
        if resp is None:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", path, nodename, resp)
            return
        status = resp.get("status", 1)
        if status == 2:
            # peer is deleting this service
            self.log.info(resp.get("error", ""))
            return
        elif status != 0:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", path, nodename, resp)
            return
        with tempfile.NamedTemporaryFile(dir=Env.paths.pathtmp, delete=False) as filep:
            tmpfpath = filep.name
        try:
            with codecs.open(tmpfpath, "w", "utf-8") as filep:
                filep.write(resp["data"])
            with shared.SERVICES_LOCK:
                if path in shared.SERVICES:
                    svc = shared.SERVICES[path]
                else:
                    svc = None
            if svc:
                try:
                    results = svc._validate_config(path=filep.name)
                except Exception as exc:
                    self.log.error("service %s fetched config validation "
                                   "error: %s", path, exc)
                    return
            else:
                results = {"errors": 0}
            if results["errors"] == 0:
                dst = svc_pathcf(path)
                makedirs(os.path.dirname(dst))
                shutil.copy(filep.name, dst)
                mtime = resp.get("mtime")
                if mtime:
                    os.utime(dst, (mtime, mtime))
            else:
                self.log.error("the service %s config fetched from node %s is "
                               "not valid", path, nodename)
                return
            try:
                svc = self.add_service(path)
                svc.postinstall()
            except Exception as exc:
                self.log.error("service %s postinstall failed: %s", path, exc)
        finally:
            os.unlink(tmpfpath)

        self.event("service_config_installed", {
            "path": path,
            "from": nodename
        })

    #########################################################################
    #
    # Node and Service Commands
    #
    #########################################################################
    def generic_callback(self, path, **kwargs):
        self.set_smon(path, **kwargs)
        self.update_hb_data()

    def node_stonith(self, node):
        proc = self.node_command(["stonith", "--node", node])

        # make sure we won't redo the stonith for another service
        for path, smon in self.iter_local_services_monitors():
             if smon.stonith == node:
                 self.set_smon(path, stonith="unset")

        # wait for 10sec before giving up
        for step in range(10):
            ret = proc.poll()
            if ret is not None:
                return ret
            time.sleep(1)

        # timeout, free the caller
        self.push_proc(proc=proc)

    def service_startstandby_resources(self, path, rids, slave=None):
        self.set_smon(path, "restarting")
        cmd = ["startstandby", "--rid", ",".join(rids)]
        if slave:
            cmd += ["--slave", slave]
        proc = self.service_command(path, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[path, rids],
            on_success_kwargs={"slave": slave},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources(self, path, rids, slave=None):
        self.set_smon(path, "restarting")
        cmd = ["start", "--rid", ",".join(rids)]
        if slave:
            cmd += ["--slave", slave]
        proc = self.service_command(path, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[path, rids],
            on_success_kwargs={"slave": slave},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources_on_success(self, path, rids, slave=None):
        self.set_smon(path, status="idle")
        self.update_hb_data()
        changed = False
        for rid in rids:
            instance = self.get_service_instance(path, Env.nodename)
            if instance is None:
                self.reset_smon_retries(path, rid)
                changed = True
                continue
            if slave is None:
               res = instance.get("resources", {}).get(rid, {})
               if res.get("status") not in ("up", "stdby up"):
                   self.log.error("%s start returned success but resource is "
                                  "still not up", rid)
                   continue
            else:
               res = instance.get("encap", {}).get(slave, {}).get("resources", {}).get(rid, {})
               if res.get("status") not in ("up", "stdby up"):
                   self.log.error("%s start in container %s returned success "
                                  "but resource is still not up", rid, slave)
                   continue
            changed = True
            self.reset_smon_retries(path, rid)
        if changed:
            self.update_hb_data()

    def service_status(self, path):
        if Env.nodename not in shared.SERVICES[path].nodes:
            self.log.info("skip status refresh on %s (foreign)", path)
            return
        smon = self.get_service_monitor(path)
        if smon.status and smon.status.endswith("ing"):
            # no need to run status, the running action will refresh the status earlier
            return
        cmd = ["status", "--refresh", "--waitlock=0"]
        if self.has_proc([path] +  cmd):
            # no need to run status twice
            return
        proc = self.service_command(path, cmd, local=False)
        self.push_proc(
            proc=proc,
            cmd=[path] + cmd,
        )

    def service_toc(self, path):
        self.set_smon(path, "tocing")
        proc = self.service_command(path, ["toc"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle", "local_expect": "unset", "expected_status": "tocing"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "toc failed"},
        )

    def service_start(self, path, err_status="start failed"):
        self.set_smon(path, "starting")
        proc = self.service_command(path, ["start"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle", "local_expect": "started"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": err_status},
        )

    def service_stop(self, path, force=False):
        self.set_smon(path, "stopping")
        cmd = ["stop"]
        if force:
            cmd.append("--force")
        proc = self.service_command(path, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "stop failed"},
        )

    def service_shutdown(self, path):
        self.set_smon(path, "shutdown")
        proc = self.service_command(path, ["shutdown"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "shutdown failed", "local_expect": "unset"},
        )

    def service_delete(self, path):
        self.set_smon(path, "deleting", local_expect="unset")
        proc = self.service_command(path, ["delete", "--purge-collector"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "delete failed"},
        )

    def service_purge(self, svc, leader=None):
        self.set_smon(svc.path, "unprovisioning")
        if leader is None:
            leader = self.is_natural_leader(svc)
        else:
            leader = Env.nodename == leader
        cmd = ["unprovision"]
        if leader:
            cmd += ["--leader"]
        proc = self.service_command(svc.path, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_purge_on_success",
            on_success_args=[svc.path],
            on_error="generic_callback",
            on_error_args=[svc.path],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_purge_on_success(self, path):
        self.set_smon(path, "deleting", local_expect="unset")
        proc = self.service_command(path, ["delete", "--purge-collector"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_provision(self, svc):
        self.set_smon(svc.path, "provisioning")
        leader = self.is_natural_leader(svc)
        cmd = ["provision"]
        if leader:
            cmd += ["--leader", "--disable-rollback"]
        proc = self.service_command(svc.path, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_thaw",
            on_success_args=[svc.path],
            on_success_kwargs={"slaves": True},
            on_error="generic_callback",
            on_error_args=[svc.path],
            on_error_kwargs={"status": "provision failed"},
        )

    def is_natural_leader(self, svc):
        candidates = self.placement_candidates(
            svc,
            discard_frozen=False,
            discard_na=False,
            discard_overloaded=False,
            discard_unprovisioned=False,
            discard_affinities=False,
            discard_start_failed=False,
            discard_constraints_violation=False
        )
        return self.placement_leader(svc, candidates)

    def service_unprovision(self, svc, leader=None):
        self.set_smon(svc.path, "unprovisioning", local_expect="unset")
        if leader is None:
            leader = self.is_natural_leader(svc)
        else:
            leader = Env.nodename == leader
        cmd = ["unprovision"]
        if leader:
            cmd += ["--leader"]
        proc = self.service_command(svc.path, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svc.path],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svc.path],
            on_error_kwargs={"status": "unprovision failed"},
        )

    def wait_global_expect_change(self, path, ref, timeout):
        for step in range(timeout):
            try:
                global_expect = self.get_service_monitor(path).global_expect
            except (TypeError, KeyError):
                global_expect = None
            if global_expect != ref:
                return True
            time.sleep(1)
        return False

    def service_set_flex_instances(self, path, instances):
        cmd = [
            "set",
            "--kw", "flex_min=%d" % instances,
            "--kw", "flex_max=%d" % instances,
            "--kw", "flex_target=%d" % instances,
        ]
        proc = self.service_command(path, cmd)
        out, err = proc.communicate()
        return proc.returncode

    def service_create_scaler_slave(self, path, svc, data, instances=None):
        data["DEFAULT"]["scaler_slave"] = "true"
        if svc.topology == "flex" and instances is not None:
            data["DEFAULT"]["flex_target"] = instances
        try:
            del data["metadata"]
        except KeyError:
            pass
        for kw in ("scale", "id"):
            try:
                del data["DEFAULT"][kw]
            except KeyError:
                pass
        try:
            newsvc = factory("svc")(path, svc.namespace, node=shared.NODE, cd=data)
            newsvc.commit()
            del newsvc
            self.service_status_fallback(path)
        except Exception as exc:
            self.log.error("create %s failed: %s", path, exc)
            self.set_smon(path, "create failed")
            return

        try:
            ret = self.wait_service_config_consensus(path, svc.peers)
        except Exception as exc:
            self.log.exception(exc)
            return

        self.set_smon(path, global_expect="thawed")
        self.wait_global_expect_change(path, "thawed", 600)

        self.set_smon(path, global_expect="provisioned")
        self.wait_global_expect_change(path, "provisioned", 600)

    def service_freeze(self, path):
        self.set_smon(path, "freezing")
        proc = self.service_command(path, ["freeze"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "idle"},
        )

    def service_thaw(self, path, slaves=False):
        self.set_smon(path, "thawing")
        cmd = ["thaw"]
        if slaves:
            cmd += ["--master", "--slaves"]
        proc = self.service_command(path, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[path],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[path],
            on_error_kwargs={"status": "idle"},
        )

    def services_have_init_status(self):
        need_log = (time.time() - self.startup) > 60
        for path in list_services():
            try:
                svc = shared.SERVICES[path]
            except KeyError:
                if need_log:
                    self.duplog("info", "init waiting for %(path)s daemon object allocation", path=path)
                return False
            if Env.nodename not in svc.nodes | svc.drpnodes:
                # a configuration file is present, but foreign: don't wait for a status.json
                continue
            fpath = os.path.join(svc.var_d, "status.json")
            try:
                mtime = os.path.getmtime(fpath)
            except Exception:
                if need_log:
                    self.duplog("info", "init waiting for %(path)s status to exist", path=path)
                return False
            if self.startup > mtime:
                if need_log:
                    self.duplog("info", "init waiting for %(path)s status refresh", path=path)
                return False
        return True

    def services_purge_status(self, paths=None):
        paths = paths or list_services()
        for path in paths:
            fpath = svc_pathvar(path, "status.json")
            try:
                os.unlink(fpath)
            except Exception as exc:
                pass

    def init_steps_done(self):
        """
        Return true if both boot and status commands are finished.
        Used to determine if we can run service_status() from the monitor loop.
        """
        return len(self.init_steps) == 2

    def add_init_step(self, step):
        """
        Used as a callback of initial boot and status commands.
        """
        self.init_steps.add(step)

    def services_init_status(self):
        svcs = list_services()
        if not svcs:
            self.log.info("no objects to get an initial status from")
            return
        self.services_purge_status(paths=svcs)
        proc = self.service_command(",".join(svcs), ["status", "--parallel", "--refresh"], local=False)
        self.add_init_step("boot")
        self.push_proc(
            proc=proc,
            cmd="init status",
            on_success="add_init_step",
            on_success_args=["status"],
            on_error="add_init_step",
            on_error_args=["status"],
        )

    def services_init_boot(self):
        self.services_purge_status()
        proc1 = self.service_command(",".join(list_services(kinds=["vol", "svc"])), ["boot", "--parallel"])
        self.push_proc(
            proc=proc1,
            on_success="add_init_step",
            on_success_args=["boot"],
            on_error="add_init_step",
            on_error_args=["boot"],
        )
        proc2 = self.service_command(",".join(list_services(kinds=["usr", "cfg", "sec", "ccfg"])), ["status", "--parallel", "--refresh"], local=False)
        self.push_proc(
            proc=proc2,
            on_success="add_init_step",
            on_success_args=["status"],
            on_error="add_init_step",
            on_error_args=["status"],
        )


    #########################################################################
    #
    # Orchestration
    #
    #########################################################################
    def orchestrator(self):
        if self.get_node_monitor().status == "init":
            return

        if self.missing_beating_peer_data():
            # just after a split+rejoin, we don't have the peers full dataset
            # even if all hb rx are reporting beating. Avoid taking decisions
            # during this transient period.
            return

        # node
        self.node_orchestrator()

        # services (iterate over deleting services too)
        paths = self.prioritized_paths()
        for path in paths:
            self.clear_start_failed(path)
            if self.transitions_maxed():
                break
            if self.status_older_than_cf(path):
                #self.log.info("%s status dump is older than its config file",
                #              path)
                instance = self.get_service_instance(path, Env.nodename)
                if instance:
                    self.service_status(path)
                continue
            svc = self.get_service(path)
            self.resources_orchestrator(path, svc)
            self.object_orchestrator(path, svc)
        self.sync_services_conf()

    def prioritized_paths(self):
        def prio(path):
            try:
                return self.instances_status_data.get([path, "priority"])
            except KeyError:
                return Env.default_priority
        paths = self.instances_status_data.keys()
        data = [(path, prio(path)) for path in paths]
        return [d[0] for d in sorted(data, key=lambda x: x[1])]

    def transitions_maxed(self):
        transitions = self.transition_count()
        if transitions <= shared.NODE.max_parallel:
            return False
        self.duplog("info", "delay services orchestration: "
                    "%(transitions)d/%(max)d transitions already "
                    "in progress", transitions=transitions,
                    max=shared.NODE.max_parallel)
        return True

    def resources_orchestrator(self, path, svc):
        if self.get_node_monitor().status == "shutting":
            return
        if svc is None:
            return
        if self.node_frozen or self.instance_frozen(path):
            #self.log.info("resource %s orchestrator out (frozen)", svc.path)
            return
        if svc.disabled:
            #self.log.info("resource %s orchestrator out (disabled)", svc.path)
            return

        def monitored_resource(svc, rid, resource, smon):
            if resource.get("disable"):
                return False
            if smon.local_expect != "started":
                return False
            try:
                nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            except AttributeError:
                nb_restart = 0
            retries = self.get_smon_retries(svc.path, rid)

            if retries > nb_restart:
                return False
            elif retries == nb_restart:
                if nb_restart > 0:
                    self.event("max_resource_restart", {
                        "path": svc.path,
                        "rid": rid,
                        "resource": resource,
                        "restart": nb_restart,
                    })
                self.inc_smon_retries(svc.path, rid)
                if resource.get("monitor"):
                    candidates = self.placement_candidates(svc)
                    if candidates != [Env.nodename] and len(candidates) > 0:
                        self.event("resource_toc", {
                            "path": svc.path,
                            "rid": rid,
                            "resource": resource,
                        })
                        try:
                            smon = self.get_service_monitor(path)
                        except KeyError:
                            smon = Storage()
                        if smon.status != "tocing":
                            self.service_toc(svc.path)
                    else:
                        self.event("resource_would_toc", {
                            "reason": "no_candidate",
                            "path": svc.path,
                            "rid": rid,
                            "resource": resource,
                        })
                else:
                    self.event("resource_degraded", {
                        "path": svc.path,
                        "rid": rid,
                        "resource": resource,
                    })
                    return False
            else:
                self.inc_smon_retries(svc.path, rid)
                self.event("resource_restart", {
                    "path": svc.path,
                    "rid": rid,
                    "resource": resource,
                    "restart": nb_restart,
                    "try": retries+1,
                })
                return True

        def stdby_resource(svc, rid, resource):
            if resource.get("standby") is not True:
                return False
            try:
                nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
                if nb_restart < self.default_stdby_nb_restart:
                    nb_restart = self.default_stdby_nb_restart
            except AttributeError:
                nb_restart = self.default_stdby_nb_restart

            retries = self.get_smon_retries(svc.path, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                self.inc_smon_retries(svc.path, rid)
                self.event("max_stdby_resource_restart", {
                    "path": svc.path,
                    "rid": rid,
                    "resource": resource,
                    "restart": nb_restart,
                })
                return False
            self.inc_smon_retries(svc.path, rid)
            self.event("stdby_resource_restart", {
                "path": svc.path,
                "rid": rid,
                "resource": resource,
                "restart": nb_restart,
                "try": retries+1,
            })
            return True

        smon = self.get_service_monitor(svc.path)
        if smon.status != "idle":
            return
        if smon.global_expect in ("unprovisioned", "purged", "deleted", "frozen"):
            return

        instance = self.get_service_instance(svc.path, Env.nodename)
        if not instance:
            return
        if instance.encap is True:
            return
        resources = instance.get("resources", {})

        mon_rids = []
        stdby_rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("warn", "down", "stdby down"):
                self.reset_smon_retries(svc.path, rid)
                continue
            if resource.get("provisioned", {}).get("state") is False:
                continue
            if monitored_resource(svc, rid, resource, smon):
                mon_rids.append(rid)
            elif stdby_resource(svc, rid, resource):
                stdby_rids.append(rid)

        if len(mon_rids) > 0:
            self.service_start_resources(svc.path, mon_rids)
        if len(stdby_rids) > 0:
            self.service_startstandby_resources(svc.path, stdby_rids)

        # same for encap resources
        rids = []
        for crid, cdata in instance.get("encap", {}).items():
            if cdata.get("frozen"):
                continue
            resources = cdata.get("resources", {})
            mon_rids = []
            stdby_rids = []
            for rid, resource in resources.items():
                if resource["status"] not in ("warn", "down", "stdby down"):
                    self.reset_smon_retries(svc.path, rid)
                    continue
                if resource.get("provisioned", {}).get("state") is False:
                    continue
                if monitored_resource(svc, rid, resource, smon):
                    mon_rids.append(rid)
                elif stdby_resource(svc, rid, resource):
                    stdby_rids.append(rid)
            if len(mon_rids) > 0:
                self.service_start_resources(svc.path, mon_rids, slave=crid)
            if len(stdby_rids) > 0:
                self.service_startstandby_resources(svc.path, stdby_rids, slave=crid)

    def node_orchestrator(self):
        nmon = self.get_node_monitor()
        if nmon.status == "shutting":
            return
        if nmon.status == "draining":
            self.node_orchestrator_clear_draining()
        self.orchestrator_auto_grace()
        nmon = self.get_node_monitor()
        if self.unfreeze_when_all_nodes_joined and self.node_frozen and len(self.cluster_nodes) == len(self.thread_data.keys(["nodes"])):
            self.event("node_thaw", data={"reason": "upgrade"})
            self.freezer.node_thaw()
            self.unfreeze_when_all_nodes_joined = False
            self.node_frozen = 0
        if nmon.status != "idle":
            return
        self.set_nmon_g_expect_from_status()
        if nmon.global_expect == "frozen":
            self.unfreeze_when_all_nodes_joined = False
            if not self.node_frozen:
                self.event("node_freeze", {"reason": "target"})
                self.freezer.node_freeze()
                self.node_frozen = self.freezer.node_frozen()
        elif nmon.global_expect == "thawed":
            self.unfreeze_when_all_nodes_joined = False
            if self.node_frozen:
                self.event("node_thaw", {"reason": "target"})
                self.freezer.node_thaw()
                self.node_frozen = 0

    def object_orchestrator(self, path, svc):
        smon = self.get_service_monitor(path)
        nmon = self.get_node_monitor()
        if svc is None:
            if smon and path in self.list_cluster_paths():
                # deleting service: unset global expect if done cluster-wide
                agg = self.get_service_agg(path)
                self.set_smon_g_expect_from_status(path, smon, agg.avail)
            return
        if self.peer_init(svc):
            return
        if smon.global_expect and smon.global_expect != "aborted":
            if "failed" in smon.status:
                if self.abort_state(smon.status, smon.global_expect, smon.placement):
                    self.set_smon(path, global_expect="unset")
                    return
            elif smon.status not in ORCHESTRATE_STATES:
                #self.log.info("service %s orchestrator out (mon status %s)", svc.path, smon.status)
                return
        agg = self.get_service_agg(path)
        self.set_smon_g_expect_from_status(svc.path, smon, agg.avail)
        if nmon.status in ("shutting", "draining"):
            self.object_orchestrator_shutting(svc, smon, agg.avail)
        elif smon.global_expect:
            self.object_orchestrator_manual(svc, smon, agg.avail)
        else:
            self.object_orchestrator_auto(svc, smon, agg.avail)

    def abort_state(self, status, global_expect, placement):
        states = (status, global_expect)
        if states in ABORT_STATES:
            return True
        if placement == "leader" and states in LEADER_ABORT_STATES:
            return True
        if placement != "leader" and states in NON_LEADER_ABORT_STATES:
            return True
        return False

    @staticmethod
    def scale_path(path, idx):
        name, namespace, kind = split_path(path)
        return fmt_path(str(idx)+"."+name, namespace, kind)

    def object_orchestrator_auto(self, svc, smon, status):
        """
        Automatic instance start decision.
        Verifies hard and soft affinity and anti-affinity, then routes to
        failover and flex specific policies.
        """
        if status == "unknown":
            return
        if svc.topology == "span":
            return
        if svc.disabled:
            #self.log.info("%s orchestrator out (disabled)", svc.path)
            return
        if not self.compat:
            return
        if svc.topology == "failover" and smon.local_expect == "started":
            # decide if local_expect=started should be reset
            if status == "up" and self.get_service_instance(svc.path, Env.nodename).avail != "up":
                self.log.info("%s is globally up but the local instance is "
                              "not and is in 'started' local expect. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            elif self.service_started_instances_count(svc.path) > 1 and \
                 self.get_service_instance(svc.path, Env.nodename).avail != "up" and \
                 not self.placement_leader(svc):
                self.log.info("%s has multiple instance in 'started' "
                              "local expect and we are not leader. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            elif status != "up" and \
                 self.get_service_instance(svc.path, Env.nodename).avail in ("down", "stdby down", "undef", "n/a") and \
                 not self.resources_orchestrator_will_handle(svc):
                self.log.info("%s is not up and no resource monitor "
                              "action will be attempted, but "
                              "is in 'started' local expect. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            else:
                return
        if self.node_frozen or self.instance_frozen(svc.path):
            #self.log.info("%s orchestrator out (frozen)", svc.path)
            return
        if not self.rejoin_grace_period_expired:
            return
        if svc.scale_target is not None and smon.global_expect is None:
            self.object_orchestrator_scaler(svc)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("%s orchestrator out (agg avail status %s)",
            #              svc.path, status)
            return

        candidates = self.placement_candidates(svc)
        if not self.pass_soft_affinities(svc, candidates):
            return

        if svc.topology == "failover":
            self.object_orchestrator_auto_failover(svc, smon, status, candidates)
        elif svc.topology == "flex":
            self.object_orchestrator_auto_flex(svc, smon, status, candidates)

    def object_orchestrator_auto_failover(self, svc, smon, status, candidates):
        if svc.orchestrate == "start":
            ranks = self.placement_ranks(svc, candidates=svc.peers)
            if ranks == []:
                return
            nodename = ranks[0]
            if nodename != Env.nodename:
                # after loosing the placement leader status, the smon state
                # may need a reset
                if smon.status in ("ready", "wait parents"):
                    self.set_smon(svc.path, "idle")
                # not natural leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.path, Env.nodename)
        if smon.global_expect in ("started", "placed"):
            allowed_status = ("down", "stdby down", "stdby up", "warn")
        else:
            allowed_status = ("down", "stdby down", "stdby up")
        if smon.status in ("ready", "wait parents"):
            if instance.avail == "up":
                self.log.info("abort '%s' because the local instance "
                              "has started", smon.status)
                self.set_smon(svc.path, "idle")
                return
            if status not in allowed_status or \
               self.peer_warn(svc.path):
                self.log.info("abort '%s' because the aggregated status has "
                              "gone %s", smon.status, status)
                self.set_smon(svc.path, "idle")
                return
            peer = self.better_peer_ready(svc, candidates)
            if peer:
                self.log.info("abort '%s' because node %s has a better "
                              "placement score for service %s and is also "
                              "ready", smon.status, peer, svc.path)
                self.set_smon(svc.path, "idle")
                return
            peer = self.peer_transitioning(svc.path)
            if peer:
                self.log.info("abort '%s' because node %s is already "
                              "acting on service %s", smon.status, peer,
                              svc.path)
                self.set_smon(svc.path, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.path, status="idle")
                return
        elif smon.status == "ready":
            if self.parent_transitioning(svc):
                self.log.info("abort 'ready' because a parent is transitioning")
                self.set_smon(svc.path, "idle")
                return
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.event("instance_start", {
                    "reason": "from_ready",
                    "path": svc.path,
                    "since": int(now-smon.status_updated),
                })
                if smon.stonith and smon.stonith not in self.thread_data.keys(["nodes"]):
                    # stale peer which previously ran the service
                    self.node_stonith(smon.stonith)
                self.service_start(svc.path, err_status="place failed" if smon.global_expect == "placed" else "start failed")
                return
            tmo = int(smon.status_updated + self.ready_period - now) + 1
            self.log.info("service %s will start in %d seconds",
                          svc.path, tmo)
            self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect not in ("started", "placed"):
                return
            if status not in allowed_status:
                return
            if self.peer_warn(svc.path):
                return
            if svc.disable_rollback and self.peer_start_failed(svc.path):
                return
            peer = self.peer_transitioning(svc.path)
            if peer:
                return
            if not self.placement_leader(svc, candidates):
                return
            if not self.parents_available(svc) or self.parent_transitioning(svc):
                self.set_smon(svc.path, status="wait parents")
                return
            if len(svc.peers) == 1:
                self.event("instance_start", {
                    "reason": "single_node",
                    "path": svc.path,
                })
                self.service_start(svc.path)
                return
            self.log.info("failover service %s status %s", svc.path,
                          status)
            self.set_smon(svc.path, "ready")

    def object_orchestrator_auto_flex(self, svc, smon, status, candidates):
        if svc.orchestrate == "start":
            ranks = self.placement_ranks(svc, candidates=svc.peers)
            if ranks == []:
                return
            try:
                idx = ranks.index(Env.nodename)
            except ValueError:
                return
            if Env.nodename not in ranks[:svc.flex_target]:
                # after loosing the placement leader status, the smon state
                # may need a reset
                if smon.status in ("ready", "wait parents"):
                    self.set_smon(svc.path, "idle")
                # natural not a leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.path, Env.nodename)
        up_nodes = self.up_service_instances(svc.path)
        n_up = len(up_nodes)
        n_missing = svc.flex_target - n_up

        if smon.status in ("ready", "wait parents"):
            if n_up > svc.flex_target:
                self.log.info("flex service %s instance count reached "
                              "required minimum while we were ready",
                              svc.path)
                self.set_smon(svc.path, "idle")
                return
            better_peers = self.better_peers_ready(svc);
            if n_missing > 0 and len(better_peers) >= n_missing:
                self.log.info("abort 'ready' because nodes %s have a better "
                              "placement score for service %s and are also "
                              "ready", ','.join(better_peers), svc.path)
                self.set_smon(svc.path, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.path, status="idle")
                return
        if smon.status == "ready":
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.event("instance_start", {
                    "reason": "from_ready",
                    "path": svc.path,
                    "since": now-smon.status_updated,
                })
                self.service_start(svc.path)
            else:
                tmo = int(smon.status_updated + self.ready_period - now) + 1
                self.log.info("%s will start in %d seconds",
                              svc.path, tmo)
                self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect not in ("started", "placed"):
                return
            if n_up == svc.flex_target and smon.global_expect != "placed":
                return
            if n_up <= svc.flex_target:
                if smon.global_expect in ("started", "placed"):
                    allowed_avail = STOPPED_STATES + ["warn"]
                else:
                    allowed_avail = STOPPED_STATES
                if instance.avail not in allowed_avail:
                    return
                if not self.placement_leader(svc, candidates):
                    return
                if not self.parents_available(svc):
                    self.set_smon(svc.path, status="wait parents")
                    return
                self.log.info("flex %s started, starting or ready to "
                              "start instances: %d/%d. local status %s",
                              svc.path, n_up, svc.flex_target,
                              instance.avail)
                self.set_smon(svc.path, "ready")
            elif n_up > svc.flex_target:
                if instance is None:
                    return
                if instance.avail not in STARTED_STATES:
                    return
                try:
                    peer = self.place_in_progress(svc.path, discard_local=False)
                    if peer:
                        # don't auto-stop instance while a place is in progress
                        # to allow for temporary excess of running instances and
                        # thus avoid outages/performance degradation during moves.
                        return
                except ValueError:
                    return
                n_to_stop = n_up - svc.flex_target
                overloaded_up_nodes = self.overloaded_up_service_instances(svc.path)
                to_stop = self.placement_ranks(svc, candidates=overloaded_up_nodes)[-n_to_stop:]
                n_to_stop -= len(to_stop)
                if n_to_stop > 0:
                    to_stop += self.placement_ranks(svc, candidates=set(up_nodes)-set(overloaded_up_nodes))[-n_to_stop:]
                self.log.info("%d nodes to stop to honor %s "
                              "flex_target=%d. choose %s",
                              n_to_stop, svc.path, svc.flex_target,
                              ", ".join(to_stop))
                if Env.nodename not in to_stop:
                    return
                self.event("instance_stop", {
                    "reason": "flex_threshold",
                    "path": svc.path,
                    "up": n_up,
                })
                self.service_stop(svc.path)

    def node_orchestrator_clear_draining(self):
        for path, smon in self.iter_local_services_monitors():
            if not smon:
                continue
            if smon.status == "shutdown failed":
                continue
            if smon.local_expect == "shutdown":
                return
        self.set_nmon("idle")

    def object_orchestrator_shutting(self, svc, smon, status):
        """
        Take actions to shutdown all local services instances marked with
        local_expect == "shutdown", even if frozen.

        Honor parents/children sequencing.
        """
        instance = self.get_service_instance(svc.path, Env.nodename)
        if smon.local_expect == "shutdown":
            if smon.status in ("shutdown", "shutdown failed"):
                return
            if self.is_instance_shutdown(instance):
                self.set_smon(svc.path, local_expect="unset")
                return
            if not self.local_children_down(svc):
                self.set_smon(svc.path, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")
            self.service_shutdown(svc.path)

    def scaler_current_slaves(self, path):
        name, namespace, kind = split_path(path)
        pattern = r"[0-9]+\." + name + "$"
        if namespace:
            pattern = "^%s/%s/%s" % (namespace, kind, pattern)
        else:
            pattern = "^%s" % pattern
        return [slave for slave in shared.SERVICES if re.match(pattern, slave)]

    def object_orchestrator_scaler(self, svc):
        smon = self.get_service_monitor(svc.path)
        if smon.status != "idle":
            return
        peer = self.peer_transitioning(svc.path)
        if peer:
            return
        candidates = self.placement_candidates(
            svc, discard_frozen=False,
            discard_na=False,
            discard_overloaded=False,
            discard_unprovisioned=False,
            discard_constraints_violation=False,
            discard_start_failed=False
        )
        ranks = self.placement_ranks(svc, candidates)
        if not ranks:
            return
        if ranks[0] != Env.nodename:
            # not natural leader
            return
        current_slaves = self.scaler_current_slaves(svc.path)
        n_slots = self.scaler_slots(current_slaves)
        if n_slots == svc.scale_target:
            return
        missing = svc.scale_target - n_slots
        if missing > 0:
            self.event("scale_up", {
               "path": svc.path,
               "delta": missing,
            })
            self.object_orchestrator_scaler_up(svc, missing, current_slaves)
        else:
            self.event("scale_down", {
               "path": svc.path,
               "delta": -missing,
            })
            self.object_orchestrator_scaler_down(svc, missing, current_slaves)

    def object_orchestrator_scaler_up(self, svc, missing, current_slaves):
        if svc.topology == "flex":
            self.object_orchestrator_scaler_up_flex(svc, missing, current_slaves)
        else:
            self.object_orchestrator_scaler_up_failover(svc, missing, current_slaves)

    def object_orchestrator_scaler_down(self, svc, missing, current_slaves):
        if svc.topology == "flex":
            self.object_orchestrator_scaler_down_flex(svc, missing, current_slaves)
        else:
            self.object_orchestrator_scaler_down_failover(svc, missing, current_slaves)

    def object_orchestrator_scaler_up_flex(self, svc, missing, current_slaves):
        candidates = self.placement_candidates(svc, discard_na=False, discard_preserved=False)
        width = len(candidates)
        if width == 0:
            return

        # start fill-up the current slaves that might have holes due to
        # previous scaling while some nodes where overloaded
        n_current_slaves = len(current_slaves)
        current_slaves = self.sort_scaler_slaves(current_slaves, reverse=True)
        for slavename in current_slaves:
            slave = shared.SERVICES[slavename]
            if slave.flex_max >= width:
                continue
            remain = width - slave.flex_max
            if remain > missing:
                pad = remain - missing
                new_width = slave.flex_max + pad
            else:
                pad = remain
                new_width = width
            ret = self.service_set_flex_instances(slavename, new_width)
            if ret != 0:
                self.set_smon(slavename, "set failed")
            else:
                missing -= pad

        left = missing % width
        slaves_count = missing // width
        if left:
            slaves_count += 1

        if slaves_count == 0:
            return

        to_add = []
        max_burst = 3

        # create services in holes first
        for slavename in [self.scale_path(svc.path, idx) for idx in range(n_current_slaves)]:
            if slavename in current_slaves:
                continue
            to_add.append([slavename, width])
            slaves_count -= 1
            if slaves_count == 0:
                break

        to_add += [[self.scale_path(svc.path, n_current_slaves+idx), width] for idx in range(slaves_count)]
        if left != 0 and len(to_add):
            to_add[-1][1] = left
        to_add = to_add[:max_burst]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            tname = "scaler:%s" % svc.path
            thr = threading.Thread(target=self.scaling_worker, name=tname, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for %s: %s", svc.path, exc)

    def object_orchestrator_scaler_down_flex(self, svc, missing, current_slaves):
        to_remove = []
        excess = -missing
        for slavename in self.sort_scaler_slaves(current_slaves, reverse=True):
            slave = shared.SERVICES[slavename]
            n_slots = slave.flex_target
            if n_slots > excess:
                width = n_slots - excess
                ret = self.service_set_flex_instances(slavename, width)
                if ret != 0:
                    self.set_smon(slavename, "set failed")
                break
            else:
                to_remove.append(slavename)
                excess -= n_slots
        if len(to_remove) == 0:
            return
        delta = "delete " + ",".join(to_remove)
        self.log.info("scale %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            tname = "scaler:%s" % svc.path
            thr = threading.Thread(target=self.scaling_worker, name=tname, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for %s: %s", svc.path, exc)

    @staticmethod
    def sort_scaler_slaves(slaves, reverse=False):
        return sorted(slaves, key=lambda x: int(x.split("/")[-1].split(".")[0]), reverse=reverse)

    def object_orchestrator_scaler_up_failover(self, svc, missing, current_slaves):
        slaves_count = missing
        n_current_slaves = len(current_slaves)
        new_slaves_list = [self.scale_path(svc.path, n_current_slaves+idx) for idx in range(slaves_count)]

        to_add = self.sort_scaler_slaves(new_slaves_list)
        to_add = [[path, None] for path in to_add]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            tname = "scaler:%s" % svc.path
            thr = threading.Thread(target=self.scaling_worker, name=tname, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for %s: %s", svc.path, exc)

    def object_orchestrator_scaler_down_failover(self, svc, missing, current_slaves):
        slaves_count = -missing
        n_current_slaves = len(current_slaves)
        slaves_list = [self.scale_path(svc.path, n_current_slaves-1-idx) for idx in range(slaves_count)]

        to_remove = self.sort_scaler_slaves(slaves_list)
        to_remove = [path for path in to_remove]
        delta = "delete " + ",".join([elem[0] for elem in to_remove])
        self.log.info("scale %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            tname = "scaler:%s" % svc.path
            thr = threading.Thread(target=self.scaling_worker, name=tname, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for %s: %s", svc.path, exc)

    def scaling_worker(self, svc, to_add, to_remove):
        threads = []
        for path, instances in to_add:
            if path in shared.SERVICES:
                continue
            data = svc.print_config_data()
            try:
                thr = threading.Thread(
                    target=self.service_create_scaler_slave,
                    name="scaler:%s" % svc.path,
                    args=(path, svc, data, instances)
                )
                thr.start()
                threads.append(thr)
            except RuntimeError as exc:
                self.log.warning("failed to start a scaling thread for %s: %s", svc.path, exc)
        for path in to_remove:
            if path not in shared.SERVICES:
                continue
            self.set_smon(path, global_expect="purged")
        for path in to_remove:
            self.wait_global_expect_change(path, "purged", 300)
        while True:
            for thr in threads:
                thr.join(0)
            if any(thr.is_alive() for thr in threads):
                time.sleep(1)
                if self.stopped():
                    break
                continue
            break
        self.set_smon(svc.path, global_expect="unset", status="idle")

    def pass_soft_affinities(self, svc, candidates):
        if candidates != [Env.nodename]:
            # the local node is not the only candidate, we can apply soft
            # affinity filtering
            if svc.soft_anti_affinity:
                intersection = set(self.get_local_paths()) & set(svc.soft_anti_affinity)
                if len(intersection) > 0:
                    #self.log.info("%s orchestrator out (soft anti-affinity with %s)",
                    #              svc.path, ','.join(intersection))
                    return False
            if svc.soft_affinity:
                intersection = set(self.get_local_paths()) & set(svc.soft_affinity)
                if len(intersection) < len(set(svc.soft_affinity)):
                    #self.log.info("%s orchestrator out (soft affinity with %s)",
                    #              svc.path, ','.join(intersection))
                    return False
        return True

    def end_rejoin_grace_period(self, reason=""):
        self.rejoin_grace_period_expired = True
        self.duplog("info", "end of rejoin grace period: %s" % reason,
                    nodename="")
        nmon = self.get_node_monitor()
        if nmon.status == "rejoin":
            self.set_nmon(status="idle")
        self.merge_frozen()
        try:
            del os.environ["OPENSVC_AGENT_UPGRADE"]
        except Exception:
            pass

    def idle_node_count(self):
        n = 0
        for node in self.thread_data.keys(["nodes"]):
            nmon = self.get_node_monitor(node)
            if nmon.status not in ("idle", "rejoin"):
                continue
            try:
                paths = self.thread_data.keys(["nodes", node, "services"])
            except KeyError:
                continue
            n += 1
        return n

    def orchestrator_auto_grace(self):
        """
        After daemon startup, wait for <rejoin_grace_period_expired> seconds
        before allowing object_orchestrator_auto() to proceed.
        """
        if self.rejoin_grace_period_expired:
            return False
        if len(self.cluster_nodes) == 1:
            self.end_rejoin_grace_period("single node cluster")
            return False
        n_idle = self.idle_node_count()
        if n_idle >= len(self.cluster_nodes):
            self.end_rejoin_grace_period("now rejoined")
            return False
        now = time.time()
        if now > self.startup + self.rejoin_grace_period:
            self.end_rejoin_grace_period("expired, but some nodes are still "
                                         "unreacheable. freeze node.")
            self.event("node_freeze", data={"reason": "rejoin_expire"})
            self.freezer.node_freeze()
            self.node_frozen = self.freezer.node_frozen()
            return False
        self.duplog("info", "in rejoin grace period", nodename="")
        return True

    def clear_start_failed(self, path):
        try:
            agg = self.get_service_agg(path)
            avail = agg.avail
        except KeyError:
            avail = "unknown"
        if avail != "up":
            return
        smon = self.get_service_monitor(path)
        if not smon:
            return
        if smon.status not in ("start failed", "place failed"):
            return
        for nodename, instance in self.get_service_instances(path).items():
            if instance["monitor"].get("global_expect") is not None:
                return True
        self.log.info("clear %s %s: the service is up", path, smon.status)
        self.set_smon(path, status="idle")

    def local_children_down(self, svc):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
            if child == svc.path:
                continue
            instance = self.get_service_instance(child, Env.nodename)
            if not instance:
                continue
            avail = instance.get("avail", "unknown")
            if avail in STOPPED_STATES + ["unknown"]:
                continue
            missing.append(child)
        if len(missing) == 0:
            self.duplog("info", "%(path)s local children all avail down",
                        path=svc.path)
            return True
        self.duplog("info", "%(path)s local children still available:"
                    " %(missing)s", path=svc.path,
                    missing=" ".join(missing))
        return False

    def children_unprovisioned(self, svc):
        return self.children_down(svc, unprovisioned=True)

    def children_down(self, svc, unprovisioned=None):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
            child = resolve_path(child, svc.namespace)
            if child == svc.path:
                continue
            agg = self.get_service_agg(child) or Storage()
            avail = agg.avail or "unknown"
            if avail not in STOPPED_STATES + ["unknown"]:
                missing.append(child)
                continue
            if unprovisioned:
                prov = agg.provisioned or "unknown"
                if prov not in [False, "unknown"]:
                    # mixed or true
                    missing.append(child)
        if len(missing) == 0:
            state = "avail down"
            if unprovisioned:
                state += " and unprovisioned"
            self.duplog("info", "%(path)s children all %(state)s:"
                        " %(children)s", path=svc.path, state=state,
                        children=" ".join(svc.children_and_slaves))
            return True
        state = "available"
        if unprovisioned:
            state += " or provisioned"
        self.duplog("info", "%(path)s children still %(state)s:"
                    " %(missing)s", path=svc.path, state=state,
                    missing=" ".join(missing))
        return False

    def parents_available(self, svc):
        if len(svc.parents) == 0:
            return True
        missing = []
        for parent in svc.parents:
            try:
                parent, nodename = parent.split("@")
            except ValueError:
                nodename = None
            parent = resolve_path(parent, svc.namespace)
            if parent == svc.path:
                continue
            if nodename:
                instance = self.get_service_instance(parent, nodename)
                if instance:
                    avail = instance["avail"]
                else:
                    missing.append(parent)
                    continue
            else:
                agg = self.get_service_agg(parent) or Storage()
                avail = agg.avail or "unknown"
            if avail in STARTED_STATES + ["unknown"]:
                continue
            missing.append(parent)
        if len(missing) == 0:
            self.duplog("info", "%(path)s parents all avail up",
                        path=svc.path)
            return True
        self.duplog("info", "%(path)s parents not available:"
                    " %(missing)s", path=svc.path,
                    missing=" ".join(missing))
        return False

    def min_instances_reached(self, svc):
        instances = self.get_service_instances(svc.path, discard_empty=False)
        live_nodes = self.list_nodes()
        min_instances = set(svc.peers) & set(live_nodes)
        return len(instances) >= len(min_instances)

    def instances_started_or_start_failed(self, path, nodes):
        return self.instances_started(path, nodes, accept_start_failed=True)

    def instances_started(self, path, nodes, accept_start_failed=False):
        for nodename in nodes:
            instance = self.get_service_instance(path, nodename)
            if instance is None:
                continue
            if instance.get("avail") in STOPPED_STATES:
                if accept_start_failed:
                    if instance["monitor"].get("status") != "start failed":
                        return False
                else:
                    return False
        self.log.info("%s instances on nodes '%s' are stopped",
                      path, ", ".join(nodes))
        return True

    def instances_stopped(self, path, nodes):
        for nodename in nodes:
            instance = self.get_service_instance(path, nodename)
            if instance is None:
                continue
            if instance.get("avail") not in STOPPED_STATES:
                self.log.info("%s instance node '%s' is not stopped yet",
                              path, nodename)
                return False
        return True

    def has_leader(self, svc):
        for nodename, instance in self.get_service_instances(svc.path).items():
            if instance["monitor"].get("placement") == "leader":
                return True
        return False

    def leaders_started(self, path, exclude_status=None):
        svc = self.get_service(path)
        exclude_status = exclude_status or []
        if svc is None:
            return True
        for nodename in svc.peers:
            if nodename == Env.nodename:
                continue
            instance = self.get_service_instance(svc.path, nodename)
            if instance is None:
                continue
            if instance.get("monitor", {}).get("placement") != "leader":
                continue
            avail = instance.get("avail")
            smon_status = instance.get("monitor", {}).get("status")
            if avail not in STARTED_STATES and smon_status not in exclude_status:
                if exclude_status:
                    extra = "(%s/%s)" % (avail, smon_status)
                else:
                    extra = "(%s)" % avail
                self.log.info("%s leader instance on node %s "
                              "is not started yet %s",
                              svc.path, nodename, extra)
                return False
        return True

    def non_leaders_stopped(self, path, exclude_status=None):
        svc = self.get_service(path)
        exclude_status = exclude_status or []
        if svc is None:
            return True
        for nodename in svc.peers:
            if nodename == Env.nodename:
                continue
            instance = self.get_service_instance(svc.path, nodename)
            if instance is None:
                continue
            if instance.get("monitor", {}).get("placement") == "leader":
                continue
            avail = instance.get("avail")
            smon_status = instance.get("monitor", {}).get("status")
            if avail not in STOPPED_STATES and smon_status not in exclude_status:
                if exclude_status:
                    extra = "(%s/%s)" % (avail, smon_status)
                else:
                    extra = "(%s)" % avail
                self.log.info("%s non leader instance on node %s "
                              "is not stopped yet %s",
                              svc.path, nodename, extra)
                return False
        return True

    def leader_last(self, svc, provisioned=False, deleted=False, silent=False):
        """
        Return the leader nodename if the peers not selected for anteriority are
        found to have reached the target status, or if the local node is not the
        one with anteriority.

        Anteriority selection is done with these criteria:

        * choose the placement top node amongst node with a up instance
        * if none, choose the placement top node amongst all nodes,
          whatever their frozen, and current provisioning state. Still
          honor the constraints and overload discards.
        """
        agg = self.get_service_agg(svc.path) or Storage()
        if agg.avail is None:
            # base services can be unprovisioned and purged in parallel
            return Env.nodename
        candidates = self.placement_candidates(
            svc, discard_frozen=False,
            discard_unprovisioned=True,
            discard_start_failed=False,
            discard_overloaded=False,
        )
        try:
            ranks = self.placement_ranks(svc, candidates=candidates)
            top = ranks[0]
            if not silent:
                self.log.info("elected %s as the last node to take action on "
                              "%s", top, svc.path)
        except IndexError:
            if not silent:
                self.log.info("unblock %s leader last action (placement ranks empty)", svc.path)
            return Env.nodename
        if top != Env.nodename:
            if not silent:
                self.log.info("unblock %s leader last action (not leader)",
                              svc.path)
            return top
        for node in svc.peers:
            if node == Env.nodename:
                continue
            instance = self.get_service_instance(svc.path, node)
            if instance is None:
                continue
            elif deleted:
                if not silent:
                    self.log.info("delay leader-last action on %s: "
                                  "node %s is still not deleted", svc.path, node)
                return
            if instance.get("provisioned", False) is not provisioned:
                if not silent:
                    self.log.info("delay leader-last action on %s: "
                                  "node %s is still %s", svc.path, node,
                                  "unprovisioned" if provisioned else "provisioned")
                return
        self.log.info("unblock %s leader last action (leader)",
                      svc.path)
        return Env.nodename

    def leader_first(self, svc, provisioned=False, deleted=None, silent=False):
        """
        Return True if the peer selected for anteriority is found to have
        reached the target status, or if the local node is the one with
        anteriority.

        Anteriority selection is done with these criteria:

        * choose the placement top node amongst node with a up instance
        * if none, choose the placement top node amongst all nodes,
          whatever their frozen, and current provisioning state. Still
          honor the constraints and overload discards.
        """
        instances = self.get_service_instances(svc.path, discard_empty=True)
        candidates = [nodename for (nodename, data) in instances.items() \
                      if data.get("avail") in ("up", "warn")]
        if len(candidates) == 0:
            if not silent:
                self.log.info("%s has no up instance, relax candidates "
                              "constraints", svc.path)
            candidates = self.placement_candidates(
                svc, discard_frozen=False,
                discard_unprovisioned=False,
            )
        try:
            top = self.placement_ranks(svc, candidates=candidates)[0]
            if not silent:
                self.log.info("elected %s as the first node to take action on "
                              "%s", top, svc.path)
        except IndexError:
            if not silent:
                self.log.error("%s placement ranks list is empty", svc.path)
            return True
        if top == Env.nodename:
            return True
        instance = self.get_service_instance(svc.path, top)
        if instance is None and deleted:
            return True
        if instance.get("provisioned", True) is provisioned:
            return True
        if not silent:
            self.log.info("delay leader-first action on %s", svc.path)
        return False

    def overloaded_up_service_instances(self, path):
        return [nodename for nodename in self.up_service_instances(path) if self.node_overloaded(nodename)]

    def scaler_slots(self, paths):
        count = 0
        for path in paths:
            svc = shared.SERVICES[path]
            if svc.topology == "flex":
                width = len([1 for nodename in svc.peers if nodename in self.list_nodes()])
                count += min(width, svc.flex_target)
            else:
                count += 1
        return count

    def resources_orchestrator_will_handle(self, svc):
        """
        Return True if the resource orchestrator will try something to restore
        service to its optimal state.
        """
        for res in svc.get_resources():
            if res.disabled:
                continue
            try:
                status = self.thread_data.get(["nodes", Env.nodename, "services", "status", svc.path, "resources", res.rid, "status"])
            except KeyError:
                continue
            if status in ("up", "stdby up", "n/a", "undef"):
                continue
            if res.nb_restart and self.get_smon_retries(svc.path, res.rid) < res.nb_restart:
                return True
            if res.monitor:
                return True
        return False

    def service_started_instances_count(self, path):
        """
        Count the number of instances in 'started' local expect state.
        """
        count = 0
        try:
            for node, ndata in self.iter_nodes():
                try:
                    local_expect = ndata["services"]["status"][path]["monitor"]["local_expect"]
                except Exception:
                    continue
                if local_expect == "started":
                    count += 1
            return count
        except Exception as exc:
            return 0

    def up_service_instances(self, path):
        nodenames = []
        for nodename, instance in self.get_service_instances(path).items():
            if instance["avail"] == "up":
                nodenames.append(nodename)
            elif instance["monitor"].get("status") in ("restarting", "starting", "wait children", "provisioning", "placing"):
                nodenames.append(nodename)
        return nodenames

    def parent_transitioning(self, svc):
        if len(svc.parents) == 0:
            return False
        for parent in svc.parents:
            if parent == svc.path:
                continue
            if self.peer_transitioning(parent, discard_local=False):
                return True
        return False

    def peer_init(self, svc):
        """
        Return True if a peer node is in init nmon status.
        Else return False.
        """
        for nodename in svc.peers:
            nmon = self.get_node_monitor(nodename=nodename)
            if nmon is None:
                continue
            if nmon.status == "init":
                return True
        return False

    def peer_warn(self, path, with_self=False):
        """
        For failover services, return the nodename of the first peer with the
        service in warn avail status.
        """
        try:
            if shared.SERVICES[path].topology != "failover":
                return
        except:
            return
        for nodename, instance in self.get_service_instances(path).items():
            if not with_self and nodename == Env.nodename:
                continue
            if instance["avail"] == "warn" and not instance["monitor"].get("status").endswith("ing"):
                return nodename

    def peers_options(self, path, candidates, status):
        """
        Return the nodes in <candidates> that are a viable start/place
        orchestration option.

        This method is used to determine if the global expect should be
        reset if no options are left.
        """
        nodenames = []
        for nodename in candidates:
            instance = self.get_service_instance(path, nodename)
            if instance is None:
                continue
            smon_status = instance["monitor"].get("status", "")
            if smon_status in status:
               continue
            avail = instance["avail"]
            if (avail == "warn" and not smon_status.endswith("ing")) or \
               avail in STOPPED_STATES + STARTED_STATES:
                nodenames.append(nodename)
        return nodenames

    def place_in_progress(self, path, discard_local=True):
        """
        Return the nodename of any object instance that has its global expect
        set to any kind of placed variants.
        """
        for nodename, instance in self.get_service_instances(path).items():
            if discard_local and nodename == Env.nodename:
                continue
            ge_time = instance["monitor"].get("global_expect_updated") or 0
            status_time = instance.get("updated") or 0
            if ge_time > status_time:
                # we don't have the lastest instance status. in doubt, avoid
                # taking decision
                raise ValueError
            ge = instance["monitor"].get("global_expect") or ""
            if ge.startswith("placed"):
                return nodename

    def peer_transitioning(self, path, discard_local=True):
        """
        Return the nodename of the first peer with the service in a transition
        state.
        """
        for nodename, instance in self.get_service_instances(path).items():
            if discard_local and nodename == Env.nodename:
                continue
            if instance["monitor"].get("status", "").endswith("ing"):
                return nodename

    def peer_start_failed(self, path):
        """
        Return the nodename of the first peer with the service in a start failed
        state.
        """
        for nodename, instance in self.get_service_instances(path).items():
            if nodename == Env.nodename:
                continue
            if instance["monitor"].get("status") == "start failed":
                return nodename

    def better_peers_ready(self, svc):
        ranks = self.placement_ranks(svc, candidates=svc.peers)
        peers = []
        for nodename in ranks:
            if nodename == Env.nodename:
                return peers
            instance = self.get_service_instance(svc.path, nodename)
            if instance is None:
                continue
            if instance["monitor"].get("status") == "ready":
                peers.append(nodename)
        return peers

    def better_peer_ready(self, svc, candidates):
        """
        Return the nodename of the first peer with the service in ready state, or
        None if we are placement leader or no peer is in ready state.
        """
        if self.placement_leader(svc, candidates):
            return
        for nodename, instance in self.get_service_instances(svc.path).items():
            if nodename == Env.nodename:
                continue
            if instance["monitor"].get("status") == "ready":
                return nodename

    #########################################################################
    #
    # Cluster nodes aggregations
    #
    #########################################################################
    def get_clu_agg_frozen(self):
        fstatus = "undef"
        fstatus_l = []
        n_instances = 0
        for nodename, ndata in self.iter_nodes():
            try:
                fstatus_l.append(ndata["frozen"])
            except KeyError:
                # sender daemon outdated
                continue
            n_instances += 1
        n_frozen = len([True for froz in fstatus_l if froz])
        if n_instances == 0:
            fstatus = 'n/a'
        elif n_frozen == n_instances:
            fstatus = 'frozen'
        elif n_frozen == 0:
            fstatus = 'thawed'
        else:
            fstatus = 'mixed'
        return fstatus

    #########################################################################
    #
    # Service instances status aggregation
    #
    #########################################################################
    def get_agg_avail(self, path):
        try:
            instance = self.get_any_service_instance(path)
        except IndexError:
            instance = None
        if instance is None:
            # during init for example
            return "unknown"
        topology = instance.get("topology")
        if topology == "failover":
            avail = self.get_agg_avail_failover(path)
        elif topology == "flex":
            avail = self.get_agg_avail_flex(path)
        else:
            avail = "unknown"

        if instance.get("scale") is not None:
            n_up = 0
            for slave in self.scaler_current_slaves(path):
                n_up += len(self.up_service_instances(slave))
            if n_up == 0:
                return "n/a"
            if n_up > 0 and n_up < instance.get("scale"):
                return "warn"

        slaves = instance.get("slaves", [])
        slaves += instance.get("scaler_slaves", [])
        if slaves:
            _, namespace, _ = split_path(path)
            avails = set([avail])
            for child in slaves:
                child = resolve_path(child, namespace)
                agg = self.get_service_agg(child) or Storage()
                child_avail = agg.avail or "unknown"
                avails.add(child_avail)
            if avails == set(["n/a"]):
                return "n/a"
            avails -= set(["n/a", "undef", "unknown"])
            n_avails = len(avails)
            if n_avails == 0:
                return "n/a"
            elif n_avails == 1:
                return list(avails)[0]
            else:
                return "warn"
        elif instance.get("scale") is not None:
            # scaler without slaves
            return "n/a"
        return avail

    def get_agg_overall(self, path):
        ostatus = 'undef'
        ostatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(path).values():
            if "overall" not in instance:
                continue
            ostatus_l.append(instance["overall"])
            n_instances += 1
        ostatus_s = set(ostatus_l)

        if n_instances == 0:
            ostatus = "n/a"
        elif "warn" in ostatus_s or "stdby down" in ostatus_s:
            ostatus = "warn"
        elif len(ostatus_s) == 1:
            ostatus = ostatus_s.pop()
        elif set(["up", "down"]) == ostatus_s or \
             set(["up", "stdby up"]) == ostatus_s or \
             set(["up", "down", "stdby up"]) == ostatus_s or \
             set(["up", "down", "stdby up", "n/a"]) == ostatus_s:
            ostatus = "up"
        elif set(["down", "stdby up"]) == ostatus_s or \
             set(["down", "stdby up", "n/a"]) == ostatus_s:
            ostatus = "down"
        if "stdby" in ostatus:
            ostatus = "down"
        try:
            instance = self.get_any_service_instance(path)
        except IndexError:
            instance = Storage()
        if instance is None:
            # during init for example
            return "unknown"
        slaves = instance.get("slaves", [])
        slaves += instance.get("scaler_slaves", [])
        if slaves:
            _, namespace, _ = split_path(path)
            avails = set([ostatus])
            for child in slaves:
                child = resolve_path(child, namespace)
                agg = self.get_service_agg(child) or Storage()
                child_status = agg.overall or "unknown"
                avails.add(child_status)
            if avails == set(["n/a"]):
                return "n/a"
            avails -= set(["n/a"])
            if len(avails) == 1:
                return list(avails)[0]
            return "warn"
        elif instance.get("scale") is not None:
            # scaler without slaves
            return "n/a"
        return ostatus

    def get_agg_frozen(self, path):
        frozen = 0
        total = 0
        for instance in self.get_service_instances(path).values():
            if "avail" not in instance:
                # deleting instance
                continue
            if instance.get("frozen"):
                frozen += 1
            total += 1
        if total == 0:
            return "n/a"
        elif frozen == total:
            return "frozen"
        elif frozen == 0:
            return "thawed"
        else:
            return "mixed"

    def is_instance_shutdown(self, instance):
        def has_stdby(instance):
            for resource in instance.get("resources", {}).values():
                if resource.get("standby"):
                    return True
            return False
        _has_stdby = has_stdby(instance)
        if _has_stdby and instance["avail"] not in ("n/a", "stdby down") or \
           not _has_stdby and instance["avail"] not in ("n/a", "down"):
            return False
        return True

    def get_agg_shutdown(self, path):
        for instance in self.get_service_instances(path).values():
            if not self.is_instance_shutdown(instance):
                return False
        return True

    def get_agg_avail_failover(self, path):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(path).values():
            if "avail" not in instance:
                continue
            astatus_l.append(instance["avail"])
            n_instances += 1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_up == 1:
            return 'up'
        elif n_instances == 0:
            return 'n/a'
        elif astatus_s == set(['n/a']):
            return 'n/a'
        elif n_up > 1:
            return 'warn'
        elif 'warn' in astatus_l:
            return 'warn'
        else:
            return 'down'

    def get_agg_avail_flex(self, path):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(path).values():
            if "avail" not in instance:
                continue
            astatus_l.append(instance["avail"])
            n_instances += 1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_instances == 0:
            return 'n/a'
        elif astatus_s == set(['n/a']):
            return 'n/a'
        elif n_up == 0:
            if "warn" in astatus_l:
                return "warn"
            else:
                return 'down'
        elif n_up > instance.get("flex_max", n_instances):
            return 'warn'
        elif n_up < instance.get("flex_min", 1) and not instance.get("scaler_slave"):
            # scaler slaves are allowed to go under-target: the scaler will pop more slices
            # to reach the target. This is what happens when a node goes does.
            return 'warn'
        else:
            return 'up'

    def get_agg_placement(self, path):
        try:
            if shared.SERVICES[path].placement == "none":
                return "n/a"
            if shared.SERVICES[path].topology == "flex" and shared.SERVICES[path].flex_min == 0:
                return "n/a"
        except KeyError:
            pass
        instances = [instance for instance in self.get_service_instances(path).values() \
                     if not instance.get("frozen")]
        if len(instances) < 2:
            return "optimal"
        has_up = False
        placement = "optimal"
        for instance in instances:
            try:
                leader = instance["monitor"].get("placement") == "leader"
                avail = instance["avail"]
            except KeyError:
                continue
            if avail in ("up", "warn"):
                has_up = True
                if not leader:
                    placement = "non-optimal"
            elif leader:
                placement = "non-optimal"
        if not has_up:
            return "n/a"
        return placement

    def get_agg_provisioned(self, path):
        provisioned = 0
        total = 0
        for instance in self.get_service_instances(path).values():
            try:
                instance_provisioned = instance["provisioned"]
            except KeyError:
                continue
            total += 1
            if instance_provisioned is True:
                provisioned += 1
            elif instance_provisioned == "mixed":
                return "mixed"
        if total == 0:
            return "n/a"
        elif provisioned == total:
            return True
        elif provisioned == 0:
            return False
        return "mixed"

    def get_agg_aborted(self, path):
        for inst in self.get_service_instances(path).values():
            try:
                global_expect = inst["monitor"].get("global_expect")
            except KeyError:
                global_expect = None
            if global_expect not in (None, "aborted"):
                return False
            try:
                local_expect = inst["monitor"].get("local_expect")
            except KeyError:
                local_expect = None
            if local_expect not in (None, "started"):
                return False
        return True

    def get_agg_conf(self, path):
        data = Storage()
        for inst in self.get_service_instances(path).values():
            scale = inst.get("scale")
            if scale is not None:
                data.scale = scale
            scaler_slave = inst.get("scaler_slave")
            if scaler_slave:
                data.scaler_slave = True
            break
        return data

    def get_agg_deleted(self, path):
        if len([True for inst in self.get_service_instances(path).values() if "updated" in inst]) > 0:
            return False
        return True

    def get_agg_purged(self, provisioned, deleted):
        if deleted is False:
            return False
        if provisioned in (False, None, "mixed"):
            return False
        return True

    #########################################################################
    #
    # Convenience methods
    #
    #########################################################################
    def status_older_than_cf(self, path):
        """
        Return True if the instance status data is older than its config data
        or if one of the age is not a timestamp float.

        Returning True skips orchestration of the instance.
        """
        status_age = self.node_data.get(["services", "status", path, "updated"], default=0)
        config_age = self.node_data.get(["services", "config", path, "updated"], default=0)
        try:
            return status_age < config_age
        except TypeError:
            return True

    def service_instances_frozen(self, path):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(path).items() if \
                instance.get("frozen")]

    def service_instances_thawed(self, path):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(path).items() if \
                not instance.get("frozen")]

    def has_instance_with(self, path, global_expect=None):
        """
        Return True if an instance of the specified service is in the
        specified state.
        """
        for nodename, smon in self.iter_service_monitors(path):
            if global_expect and smon.global_expect in global_expect:
                return True
        return False

    def get_local_paths(self):
        """
        Extract service instance names from the locally maintained hb data.
        """
        paths = []
        for path in self.node_data.keys(["services", "status"]):
            try:
                status = self.node_data.get(["services", "status", path, "avail"])
            except KeyError:
                continue
            if status == "up":
                paths.append(path)
        return paths

    def get_services_configs(self):
        """
        Return a hash indexed by path and nodename, containing the services
        configuration mtime and checksum.
        """
        data = {}
        for path, nodename, config in self.iter_services_configs():
            if path not in data:
                data[path] = {}
            data[path][nodename] = config
        return data

    def get_any_service_instance(self, path):
        """
        Return the specified service status structure on any node.
        """
        for nodename, data in self.iter_service_instances(path):
            if data in (None, ""):
                continue
            return data

    def wait_service_config_consensus(self, path, peers, timeout=60):
        if len(peers) < 2:
            return True
        self.log.info("wait for service %s consensus on config amongst peers %s",
                      path, ",".join(peers))
        for _ in range(timeout):
            if self.service_config_consensus(path, peers):
                return True
            time.sleep(1)
        self.log.error("service %s couldn't reach config consensus in %d seconds",
                       path, timeout)
        return False

    def service_config_consensus(self, path, peers):
        if len(peers) < 2:
            self.log.debug("%s auto consensus. peers: %s", path, peers)
            return True
        ref_csum = None
        for peer in peers:
            if peer not in self.list_nodes():
                # discard unreachable nodes from the consensus
                continue
            try:
                csum = self.get_service_config(path, peer).csum
            except (TypeError, KeyError, AttributeError):
                #self.log.debug("service %s peer %s has no config cksum yet", path, peer)
                return False
            except Exception as exc:
                self.log.exception(exc)
                return False
            if ref_csum is None:
                ref_csum = csum
            if ref_csum is not None and ref_csum != csum:
                #self.log.debug("service %s peer %s has a different config cksum", path, peer)
                return False
        self.log.info("service %s config consensus reached", path)
        return True

    def update_status(self):
        data = self.status()
        data.update({
            "compat": self.compat,
            "transitions": self.transition_count(),
            "frozen": self.get_clu_agg_frozen(),
        })
        self.thread_data.merge([], data)

    def update_services_config(self):
        config = {}
        for path in list_services():
            cfg = svc_pathcf(path)
            try:
                config_mtime = os.path.getmtime(cfg)
            except Exception as exc:
                self.log.warning("failed to get %s mtime: %s", cfg, str(exc))
                config_mtime = 0
            last_config = self.get_service_config(path, Env.nodename)
            if last_config is None or config_mtime > last_config["updated"]:
                #self.log.debug("compute service %s config checksum", path)
                try:
                    csum = fsum(cfg)
                except (OSError, IOError) as exc:
                    self.log.warning("service %s config checksum error: %s", path, exc)
                    continue
                try:
                    self.add_service(path)
                except Exception as exc:
                    self.log.error("%s build error: %s", path, str(exc))
                    continue
            else:
                csum = last_config["csum"]
            if last_config is None or last_config["csum"] != csum:
                if last_config is not None:
                    self.log.info("service %s configuration change" % path)
                try:
                    status_mtime = os.path.getmtime(shared.SERVICES[path].status_data_dump)
                    if config_mtime > status_mtime:
                        self.log.info("service %s refresh instance status older than config", path)
                        self.service_status(path)
                except OSError:
                    pass
            with shared.SERVICES_LOCK:
                scope = sorted(list(shared.SERVICES[path].nodes))
            config[path] = {
                "updated": config_mtime,
                "csum": csum,
                "scope": scope,
            }

        # purge deleted services
        with shared.SERVICES_LOCK:
            for path in list(shared.SERVICES.keys()):
                if path not in config:
                    self.log.info("purge deleted %s from daemon data", path)
                    del shared.SERVICES[path]
                    self.node_data.unset_safe(["services", "status", path])
        self.node_data.set(["services", "config"], config)
        return config

    def get_last_svc_status_mtime(self, path):
        """
        Return the mtime of the specified service configuration file on the
        local node. If unknown, return 0.
        """
        instance = self.get_service_instance(path, Env.nodename)
        if instance is None:
            return 0
        mtime = instance["updated"]
        if mtime is None:
            return 0
        return mtime

    def service_status_fallback(self, path):
        """
        Return the specified service status structure fetched from an execution
        of "om svc" -s <path> json status". As we arrive here when the
        status.json doesn't exist, we don't have to specify --refresh.
        """
        self.log.info("synchronous service status eval: %s", path)
        cmd = ["status", "--refresh"]
        proc = self.service_command(path, cmd, local=False)
        self.push_proc(proc=proc)
        proc.communicate()
        fpath = svc_pathvar(path, "status.json")
        return self.load_instance_status_cache(fpath)

    @staticmethod
    def load_instance_status_cache(fpath):
        try:
            with open(fpath, 'r') as filep:
                try:
                    return json.load(filep)
                except ValueError as exc:
                    # json corrupted
                    return
        except Exception as exc:
            # json not found
            return

    def update_services_status(self):
        """
        Return the local services status data, fetching data from status.json
        caches if their mtime changed or from the node data if not.

        Also update the monitor 'local_expect' field for each service.
        """
        for path, idata in self.iter_local_services_instances():
            data = {}
            smon = Storage(idata.get("monitor", {}))
            if not smon:
                continue
            if idata.get("avail") == "up" and \
               smon.global_expect is None and \
               smon.status == "idle" and \
               smon.local_expect not in ("started", "shutdown"):
                self.log.info("%s local expect set: started", path)
                data["local_expect"] = "started"

            placement = self.get_service_placement(path)
            if placement != smon.placement:
                data["placement"] = self.get_service_placement(path)

            if data:
                self.node_data.merge(["services", "status", path, "monitor"], data)

            # forget the stonith target node if we run the service
            if idata.get("avail", "n/a") == "up":
                self.node_data.unset_safe(["services", "status", path, "monitor", "stonith"])


    #########################################################################
    #
    # Service-specific monitor data helpers
    #
    #########################################################################
    def reset_smon_retries(self, path, rid):
        self.node_data.unset_safe(["services", "status", path, "monitor", "restart", rid])

    def get_smon_retries(self, path, rid):
        return self.node_data.get(["services", "status", path, "monitor", "restart", rid], default=0)

    def inc_smon_retries(self, path, rid):
        smon = self.get_service_monitor(path)
        if not smon:
            return
        try:
            self.node_data.inc(["services", "status", path, "monitor", "restart", rid])
        except TypeError:
            self.node_data.merge(["services", "status", path, "monitor"], {"restart": {rid: 1}})

    def all_nodes_frozen(self):
        for nodename in self.list_nodes():
            frozen = self.thread_data.get(["nodes", nodename, "frozen"])
            if not frozen:
                return False
        return True

    def all_nodes_thawed(self):
        for nodename in self.list_nodes():
            frozen = self.thread_data.get(["nodes", nodename, "frozen"])
            if frozen:
                return False
        return True

    def set_nmon_g_expect_from_status(self):
        nmon = self.get_node_monitor()
        if nmon.global_expect is None:
            return
        if nmon.global_expect == "frozen" and self.all_nodes_frozen():
            self.log.info("node global expect is %s and is frozen",
                          nmon.global_expect)
            self.set_nmon(global_expect="unset")
        elif nmon.global_expect == "thawed" and self.all_nodes_thawed():
            self.log.info("node global expect is %s and is thawed",
                          nmon.global_expect)
            self.set_nmon(global_expect="unset")

    def set_smon_g_expect_from_status(self, path, smon, status):
        """
        Align global_expect with the actual service states.
        """
        instance = self.get_service_instance(path, Env.nodename)
        agg = self.get_service_agg(path)

        if instance is None:
            return

        def handle_stopped():
            local_frozen = instance.get("frozen", 0)
            stopped = status in STOPPED_STATES
            if not stopped or not local_frozen:
                return
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", path, smon.global_expect, status)
            self.set_smon(path, global_expect="unset")

        def handle_shutdown():
            local_frozen = instance.get("frozen", 0)
            if not self.get_agg_shutdown(path) or not local_frozen:
                return
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", path, smon.global_expect, status)
            self.set_smon(path, global_expect="unset")

        def handle_started():
            local_frozen = instance.get("frozen", 0)
            if smon.placement == "none":
                self.set_smon(path, global_expect="unset")
            if status in STARTED_STATES and not local_frozen:
                self.log.info("service %s global expect is %s and its global "
                              "status is %s", path, smon.global_expect, status)
                self.set_smon(path, global_expect="unset")
                return
            agg = self.get_service_agg(path)
            if agg.frozen != "thawed":
                return
            svc = self.get_service(path)
            if self.peer_warn(path, with_self=True):
                self.set_smon(path, global_expect="unset")
                return

        def handle_frozen():
            agg = self.get_service_agg(path)
            if agg.frozen != "frozen":
                return
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            self.set_smon(path, global_expect="unset")

        def handle_thawed():
            if agg.frozen != "thawed":
                return
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            self.set_smon(path, global_expect="unset")

        def handle_unprovisioned():
            stopped = status in STOPPED_STATES
            if agg.provisioned not in (False, "n/a") or not stopped:
                return
            self.log.debug("service %s global expect is %s, already is",
                       path, smon.global_expect)
            self.set_smon(path, global_expect="unset")

        def handle_provisioned():
            if agg.provisioned not in (True, "n/a"):
                return
            if smon.placement == "none":
                self.set_smon(path, global_expect="unset")
            if agg.avail in ("up", "n/a"):
                # provision success, thaw
                self.set_smon(path, global_expect="thawed")
            else:
                self.set_smon(path, global_expect="started")

        def handle_purged():
            deleted = self.get_agg_deleted(path)
            purged = self.get_agg_purged(agg.provisioned, deleted)
            if not purged is True:
                return
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            self.node_data.unset_safe(["services", "status", path, "monitor"])

        def handle_deleted():
            deleted = self.get_agg_deleted(path)
            if not deleted is True:
                return
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            self.node_data.unset_safe(["services", "status", path, "monitor"])

        def handle_aborted():
            if not self.get_agg_aborted(path):
                return
            self.log.info("service %s action aborted", path)
            self.set_smon(path, global_expect="unset")
            if smon.status and smon.status.startswith("wait "):
                # don't leave lingering "wait" mon state when we no longer
                # have a target state to reach
                self.set_smon(path, status="idle")

        def handle_placed():
            if agg.frozen != "thawed":
                return
            if agg.placement in ("optimal", "n/a") and \
               agg.avail in ("up", "n/a"):
                self.set_smon(path, global_expect="unset")
                return
            svc = self.get_service(path)
            if svc is None:
                # foreign
                return
            candidates = self.placement_candidates(svc, discard_start_failed=False, discard_frozen=False)
            candidates = self.placement_leaders(svc, candidates=candidates)
            peers = self.peers_options(path, candidates, ["place failed"])
            if not peers and self.non_leaders_stopped(path, ["place failed"]):
                self.log.info("service %s global expect is %s, not optimal "
                              "and no options left", path, smon.global_expect)
                self.set_smon(path, global_expect="unset")
                return

        def handle_placed_at():
            target = smon.global_expect.split("@")[-1].split(",")
            if self.instances_started_or_start_failed(path, target):
                self.set_smon(path, global_expect="unset")

        try:
            ge, at = smon.global_expect.split("@", 1)
            handler = "handle_%s_at" % ge
        except AttributeError:
            return
        except ValueError:
            handler = "handle_" + smon.global_expect

        try:
            fn = locals()[handler]
        except KeyError:
            return

        fn()

    def get_arbitrators_data(self):
        if self.arbitrators_data is None or self.last_arbitrator_ping < time.time() - self.arbitrators_check_period:
            votes = self.arbitrators_votes()
            self.last_arbitrator_ping = time.time()
            arbitrators_data = {}
            for arbitrator in shared.NODE.arbitrators:
                arbitrators_data[arbitrator["id"]] = {
                    "name": arbitrator["name"],
                    "status": "up" if arbitrator["name"] in votes else "down"
                }
                if self.arbitrators_data is None or \
                   self.arbitrators_data[arbitrator["id"]]["status"] != arbitrators_data[arbitrator["id"]]["status"]:
                    if arbitrators_data[arbitrator["id"]]["status"] == "up":
                        self.event("arbitrator_up", data={"arbitrator": arbitrator["name"]})
                    else:
                        self.event("arbitrator_down", data={"arbitrator": arbitrator["name"]})
            self.arbitrators_data = arbitrators_data
        return self.arbitrators_data

    def update_cluster_data(self):
        self.update_node_data()
        self.purge_left_nodes()
        self.merge_hb_data()
        self.update_agg_services()
        self.update_status()

    def _update_cluster_data(self):
        self.daemon_status_data.set(["cluster"], {
            "name": self.cluster_name,
            "id": self.cluster_id,
            "nodes": self.cluster_nodes,
        })

    def purge_left_nodes(self):
        left = set(self.list_nodes()) - set(self.cluster_nodes)
        for node in left:
            self.log.info("purge left node %s data", node)
            self.thread_data.unset_safe(["nodes", node])

    def update_node_data(self):
        """
        Rescan services config and status.
        """
        data = {
            "stats": shared.NODE.stats(),
            "frozen": self.node_frozen,
            "env": shared.NODE.env,
            "labels": shared.NODE.labels,
            "targets": shared.NODE.targets,
            "locks": shared.LOCKS,
            "speaker": self.speaker() and "collector" in shared.THREADS,
            "min_avail_mem": shared.NODE.min_avail_mem,
            "min_avail_swap": shared.NODE.min_avail_swap,
        }
        self.node_data.merge([], data)
        self.update_services_config()
        self.update_services_status()

        if self.quorum:
            self.node_data.set(["arbitrators"], self.get_arbitrators_data())
        else:
            self.node_data.unset_safe(["arbitrators"])

        # purge deleted service instances
        for path in self.node_data.keys(["services", "status"]):
            sconf = self.get_service_config(path, Env.nodename)
            if sconf:
                continue
            if not self.node_data.exists(["services", "status", path, "monitor"]):
                continue
            smon = self.get_service_monitor(path)
            global_expect = smon.global_expect
            global_expect_updated = smon.global_expect_updated or 0
            if global_expect is not None and time.time() < global_expect_updated + 3:
                # keep the smon around for a while
                #self.log.info("relay foreign service %s global expect %s",
                #              path, global_expect)
                continue
            try:
                self.node_data.unset(["services", "status", path])
                self.log.debug("purge deleted service %s from status data", path)
            except KeyError:
                pass

    def update_hb_data(self):
        """
        Prepare the heartbeat data we send to other nodes.
        """

        if self.mon_changed():
            self.update_cluster_data()

        diff = self._update_hb_data_locked()

        if diff is None:
            return

        # don't store the diff if we have no peers
        if len(shared.LOCAL_GEN) == 0:
            return

        shared.GEN_DIFF[shared.GEN] = diff
        self.purge_log()
        with shared.HB_MSG_LOCK:
             # reset the full status cache. get_message() will refill if
             # needed.
             shared.HB_MSG = None
             shared.HB_MSG_LEN = 0
        shared.wake_heartbeat_tx()

    def _update_hb_data_locked(self):
        now = time.time()
        diff = self.daemon_status_data.pop_diff()

        # excluded from the diff: gen, updated
        updated = self.node_data.get(["updated"], default=now)

        if self.last_node_data is None:
            # first run
            self.last_node_data = True
            self.node_data.set(["gen"], self.get_gen(inc=True))
            self.node_data.set(["updated"], now)
            return

        if len(diff) == 0:
            self.node_data.set(["gen"], self.get_gen(inc=False))
            self.node_data.set(["updated"], updated)
            return

        self.last_node_data = True
        self.node_data.set(["gen"], self.get_gen(inc=True))
        self.node_data.set(["updated"], now)
        diff.append([["updated"], now])
        return diff

    def merge_hb_data(self):
        self.merge_hb_data_locks()
        self.merge_hb_data_compat()
        self.merge_hb_data_monitor()

    def merge_hb_data_locks(self):
        changed = False
        for nodename in self.list_nodes():
            if nodename == Env.nodename:
                continue
            locks = self.thread_data.get(["nodes", nodename, "locks"], default={})
            for name in list(locks):
                try:
                    lock = locks[name]
                except KeyError:
                    # deleted during iteration
                    continue
                if lock["requester"] != nodename:
                    # only trust locks from requester views
                    continue
                if lock["requester"] == Env.nodename and name not in shared.LOCKS:
                    # don't re-merge a released lock emitted by this node
                    continue
                with shared.LOCKS_LOCK:
                    if name not in shared.LOCKS:
                        self.log.info("merge lock %s from node %s", name, nodename)
                        shared.LOCKS[name] = lock
                        changed = True
                        continue

                    # Lock name is already present in shared.LOCKS
                    merge = False
                    if lock["requester"] == nodename:
                        if lock["requested"] < shared.LOCKS[name]["requested"]:
                            merge = "older"
                        elif lock["requested"] > shared.LOCKS[name]["requested"]:
                            merge = "newer"
                        if merge:
                            self.log.info("merge %s lock %s from node %s (id %s replaced by id %s)",
                                          merge, name, nodename, shared.LOCKS[name]["id"], lock["id"])
                            shared.LOCKS[name] = lock
                            changed = True
                            continue
        for name in list(shared.LOCKS):
            with shared.LOCKS_LOCK:
                try:
                    shared_lock = shared.LOCKS[name]
                except KeyError:
                    # deleted during iteration
                    continue
                shared_lock_requester = shared_lock["requester"]
                if shared_lock_requester == Env.nodename:
                    continue
                requester_lock = self.thread_data.get(["nodes", shared_lock_requester, "locks", name], default=None)
                if requester_lock is None:
                    self.log.info("drop lock %s from node %s", name, shared_lock_requester)
                    del shared.LOCKS[name]
                    changed = True
        if changed:
            with shared.LOCKS_LOCK:
                self.update_cluster_locks_lk()

    def merge_hb_data_compat(self):
        compat = set()
        for nodename, ndata in self.iter_nodes():
            try:
                compat.add(ndata["compat"])
            except KeyError:
                pass
        new_compat = len(set(compat)) <= 1
        if self.compat != new_compat:
            if new_compat:
                self.log.info("cluster members run compatible versions. "
                              "enable ha orchestration")
            else:
                self.log.warning("cluster members run incompatible versions. "
                                 "disable ha orchestration")
            self.compat = new_compat

    def merge_hb_data_monitor(self):
        """
        Set the global expect received through heartbeats as local expect, if
        the service instance is not already in the expected status.
        """
        nodenames = self.list_nodes()
        if Env.nodename not in nodenames:
            return
        nodenames.remove(Env.nodename)

        # merge node monitors
        local_frozen = self.node_data.get(["frozen"], default=0)
        for nodename in nodenames:
            try:
                nmon = self.get_node_monitor(nodename)
                global_expect = nmon.global_expect
            except KeyError:
                # sender daemon is outdated
                continue
            if global_expect is None:
                continue
            if (global_expect == "frozen" and not local_frozen) or \
               (global_expect == "thawed" and local_frozen):
                self.log.info("node %s wants local node %s", nodename, global_expect)
                self.set_nmon(global_expect=global_expect)
            #else:
            #    self.log.info("node %s wants local node %s, already is", nodename, global_expect)

        # merge every service monitors
        for path, instance in self.iter_local_services_instances():
            if not instance:
                continue
            smon = Storage(instance.get("monitor", {}))
            if smon.global_expect == "aborted":
                # refuse a new global expect if aborting
                continue
            for nodename in nodenames:
                rinstance = self.get_service_instance(path, nodename)
                if rinstance is None:
                    continue
                if rinstance.get("stonith") is True and \
                   instance["monitor"].get("stonith") != nodename:
                    self.set_smon(path, stonith=nodename)
                global_expect = rinstance.get("monitor", {}).get("global_expect")
                if global_expect is None:
                    continue
                global_expect_updated = rinstance.get("monitor", 0).get("global_expect_updated")
                if smon.global_expect and global_expect_updated and \
                   smon.global_expect_updated and \
                   global_expect_updated < smon.global_expect_updated:
                    # we have a more recent update
                    continue
                if path in shared.SERVICES and shared.SERVICES[path].disabled and \
                   global_expect not in ("frozen", "thawed", "aborted", "deleted", "purged"):
                    continue
                if global_expect == smon.global_expect:
                    self.log.debug("node %s wants service %s %s, already targeting that",
                                   nodename, path, global_expect)
                    continue
                #else:
                #    self.log.info("node %s wants service %s %s, already is", nodename, path, global_expect)
                if self.accept_g_expect(path, instance, global_expect):
                    self.log.info("node %s wants service %s %s", nodename, path, global_expect)
                    self.set_smon(path, global_expect=global_expect)

    def accept_g_expect(self, path, instance, global_expect):
        agg = self.get_service_agg(path)
        smon = self.get_service_monitor(path)
        if global_expect not in ("aborted", "thawed", "frozen") and \
           self.abort_state(smon.status, global_expect, smon.placement):
            return False
        if global_expect == "stopped":
            local_avail = instance["avail"]
            local_frozen = instance.get("frozen", 0)
            if local_avail not in STOPPED_STATES or not local_frozen:
                return True
            else:
                return False
        elif global_expect == "shutdown":
            if instance["avail"] == "n/a" and instance.get("scale") is None:
                return False
            return not self.get_agg_shutdown(path)
        elif global_expect == "started":
            if instance["avail"] == "n/a" and instance.get("scale") is None:
                return False
            if smon.placement == "none":
                return False
            local_frozen = instance.get("frozen", 0)
            if agg.avail is None:
                return False
            if agg.avail in STOPPED_STATES or local_frozen:
                return True
            else:
                return False
        elif global_expect == "frozen":
            if agg.frozen and agg.frozen != "frozen":
                return True
            else:
                return False
        elif global_expect == "thawed":
            if agg.frozen and agg.frozen != "thawed":
                 return True
            else:
                return False
        elif global_expect == "provisioned":
            if smon.placement == "none":
                return False
            if agg.provisioned not in (True, None):
                return True
            else:
                return False
        elif global_expect == "unprovisioned":
            if agg.provisioned not in (False, None):
                return True
            else:
                return False
        elif global_expect == "deleted":
            deleted = self.get_agg_deleted(path)
            if deleted is False:
                return True
            else:
                return False
        elif global_expect == "purged":
            if smon.placement == "none":
                return False
            deleted = self.get_agg_deleted(path)
            purged = self.get_agg_purged(agg.provisioned, deleted)
            if purged is False:
                return True
            else:
                return False
        elif global_expect == "aborted":
            aborted = self.get_agg_aborted(path)
            if aborted is False:
                return True
            else:
                return False
        elif global_expect == "placed":
            if instance["avail"] == "n/a" and instance.get("scale") is None:
                return False
            if smon.placement == "none":
                return False
            if agg.placement == "non-optimal" or agg.avail != "up" or agg.frozen == "frozen":
                svc = shared.SERVICES.get(path)
                if svc is None:
                    return True
                candidates = self.placement_candidates(svc, discard_start_failed=False, discard_frozen=False)
                candidates = self.placement_leaders(svc, candidates=candidates)
                peers = self.peers_options(path, candidates, ["place failed"])
                if not peers and self.non_leaders_stopped(path, ["place failed"]):
                    return False
                return True
            else:
                return False
        elif global_expect.startswith("placed@"):
            if instance["avail"] == "n/a" and instance.get("scale") is None:
                return False
            target = global_expect.split("@")[-1].split(",")
            if Env.nodename in target:
                if instance["avail"] in STOPPED_STATES:
                    return True
            else:
                if instance["avail"] not in STOPPED_STATES:
                    return True
        return False

    def instance_provisioned(self, instance):
        if instance is None:
            return False
        instance_provisioned = instance.get("provisioned", True)
        if instance_provisioned == "mixed":
            return False
        return instance_provisioned

    def instance_unprovisioned(self, instance):
        if instance is None:
            return True
        instance_provisioned = instance.get("provisioned", False)
        if instance_provisioned == "mixed":
            return False
        return not instance_provisioned

    def get_agg(self, path):
        data = self.get_agg_conf(path)
        data.avail = self.get_agg_avail(path)
        data.frozen = self.get_agg_frozen(path)
        data.overall = self.get_agg_overall(path)
        data.placement = self.get_agg_placement(path)
        data.provisioned = self.get_agg_provisioned(path)
        return data

    def update_agg_services(self):
        data = {}
        for path in self.list_cluster_paths():
            try:
                if self.get_service(path).topology == "span":
                    data[path] = Storage()
                    continue
            except Exception as exc:
                data[path] = Storage()
                pass
            data[path] = self.get_agg(path)
        self.daemon_status_data.set(["monitor", "services"], data)
        return data

    def update_completions(self):
        self.update_completion("services")
        self.update_completion("nodes")

    def update_completion(self, otype):
        try:
            if otype == "services":
                olist = self.list_cluster_paths()
            else:
                olist = self.cluster_nodes
            with open(os.path.join(Env.paths.pathvar, "list."+otype), "w") as filep:
                filep.write("\n".join(olist)+"\n")
        except Exception as exc:
            print(exc)
            pass

    def get_last_shutdown(self):
        try:
            return os.path.getmtime(Env.paths.last_shutdown)
        except Exception:
            return 0

    def merge_frozen(self):
        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            return
        last_shutdown = self.get_last_shutdown()
        self.merge_node_frozen(last_shutdown)
        self.merge_service_frozen(last_shutdown)

    def merge_node_frozen(self, last_shutdown):
        """
        This method is only called at the end of the rejoin grace period.

        It freezes the local services instances for services that have
        a live remote instance frozen. This prevents a node
        rejoining the cluster from taking over services that where frozen
        and stopped while we were not alive.
        """
        if len(self.cluster_nodes) < 2:
            return
        try:
            frozen = self.node_data.get(["frozen"]) or 0
        except:
            return
        if frozen:
            return
        if self.node_frozen:
            return
        nmon = self.get_node_monitor()
        if nmon.global_expect == "thawed":
            return
        for peer in self.cluster_nodes:
            if peer == Env.nodename:
                continue
            try:
                frozen = self.thread_data.get(["nodes", peer, "frozen"]) or 0
            except:
                continue
            if not isinstance(frozen, float):
                # compat with older agent where frozen is a bool
                continue
            if frozen and frozen > last_shutdown:
                self.event("node_freeze", data={
                    "reason": "merge_frozen",
                    "peer": peer,
                })
                self.freezer.node_freeze()
                self.node_frozen = self.freezer.node_frozen()

    def merge_service_frozen(self, last_shutdown):
        """
        This method is only called at the end of the rejoin grace period.

        It freezes the local services instances for services that have
        a live remote instance frozen. This prevents a node
        rejoining the cluster from taking over services that where frozen
        and stopped while we were not alive.
        """
        last_shutdown = self.get_last_shutdown()
        for svc in shared.SERVICES.values():
            if svc.orchestrate == "no":
                continue
            if len(svc.peers) < 2:
                continue
            instance = self.get_service_instance(svc.path, Env.nodename)
            if not instance:
                continue
            frozen = instance.get("frozen", 0)
            if frozen:
                continue
            if self.instance_frozen(svc.path):
                continue
            smon = self.get_service_monitor(svc.path)
            if smon.global_expect == "thawed":
                continue
            for peer in svc.peers:
                if peer == Env.nodename:
                    continue
                instance = self.get_service_instance(svc.path, peer)
                if not instance:
                    continue
                frozen = instance.get("frozen", 0)
                if not isinstance(frozen, float):
                    continue
                if frozen > last_shutdown:
                    self.event("instance_freeze", data={
                        "reason": "merge_frozen",
                        "peer": peer,
                        "path": svc.path,
                    })
                    svc.freezer.freeze()
                    # reload the frozen state immediately so the monitor will
                    # not take action on this instance in the same loop.
                    self.reload_instance_frozen(svc.path)

    def reload_instance_frozen(self, path):
        try:
            self.node_data.set(["services", "status", path, "frozen"], shared.SERVICES[path].frozen())
        except Exception:
            pass

    def instance_frozen(self, path, nodename=None):
        nodename = nodename or Env.nodename
        return self.nodes_data.get([nodename, "services", "status", path, "frozen"], default=0)

    def kern_freeze(self):
        try:
            with open("/proc/cmdline", "r") as ofile:
                buff = ofile.read()
        except Exception:
            return
        if "osvc.freeze" in buff.split():
            self.event("node_freeze", data={"reason": "kern_freeze"})
            self.freezer.node_freeze()
            self.node_frozen = self.freezer.node_frozen()

    def missing_beating_peer_data(self):
        for node in self.cluster_nodes:
            if node == Env.nodename:
                continue
            if self.nodes_data.exists([node, "services"]):
                continue
            # node dataset is empty or a brief coming from a ping
            try:
                if any(shared.THREADS[thr_id].is_beating(node) for thr_id in shared.THREADS if thr_id.endswith(".rx")):
                    self.log.info("waiting for node %s dataset", node)
                    return True
            except Exception as exc:
                return True
        return False


    def update_node_gen(self, nodename, local=0, remote=0):
        shared.LOCAL_GEN[nodename] = local
        shared.REMOTE_GEN[nodename] = remote
        gdata = {
            nodename: remote,
            Env.nodename: local
        }
        if not self.nodes_data.exists([nodename]):
            self.nodes_data.set([nodename], {"gen": gdata})
        elif not self.nodes_data.exists([nodename, "gen"]):
            self.nodes_data.set([nodename, "gen"], gdata)
        else:
            self.nodes_data.merge([nodename, "gen"], gdata)

    def merge_rx(self):
        change = False
        while True:
            try:
                nodename, data, hbname = shared.RX.get_nowait()
            except queue.Empty:
                break
            change |= self._merge_rx(nodename, data, hbname)
        return change

    def _merge_rx(self, nodename, data, hbname):
        if data is None:
            self.log.info("drop corrupted rx data from %s", nodename)
        current_gen = shared.REMOTE_GEN.get(nodename, 0)
        our_gen_on_peer = data.get("gen", {}).get(Env.nodename, 0)
        kind = data.get("kind", "full")
        change = False
        #self.log.debug("received %s from node %s: current gen %d, our gen local:%s peer:%s",
        #               kind, nodename, current_gen, shared.LOCAL_GEN.get(nodename), our_gen_on_peer) # COMMENT
        if kind == "patch":
            if not self.nodes_data.exists([nodename]):
                # happens during init, or after join. ignore the patch, and ask for a full
                self.log.info("%s was not yet in nodes data view, ask for a full", nodename)
                if our_gen_on_peer == 0:
                    self.log.info("%s ignore us yet, will send a full", nodename)
                self.update_node_gen(nodename, remote=0, local=our_gen_on_peer)
                return False
            if current_gen == 0:
                # waiting for a full: ignore patches
                # self.log.debug("waiting for a full: ignore patch %s received from %s", list(data.get("deltas", [])), nodename) # COMMENT
                if shared.REMOTE_GEN.get(nodename) is None:
                    self.log.info("we don't know about last applied gen of %s, it says it has gen %s of us."
                                  "Ask for a full", nodename, our_gen_on_peer)
                    self.update_node_gen(nodename, remote=0, local=our_gen_on_peer)
                return False
            deltas = data.get("deltas", [])
            gens = sorted([int(gen) for gen in deltas])
            gens = [gen for gen in gens if gen > current_gen]
            if len(gens) == 0:
                #self.log.info("no more recent gen in received deltas")
                if our_gen_on_peer > shared.LOCAL_GEN[nodename]:
                    shared.LOCAL_GEN[nodename] = our_gen_on_peer
                    self.nodes_data.set([nodename, "gen", Env.nodename], our_gen_on_peer)
                return False
            nodes_info_change = False
            for gen in gens:
                #self.log.debug("patch node %s dataset gen %d over %d (%d diffs)", nodename, gen, current_gen, len(deltas[str(gen)])) # COMMENT
                if gen - 1 != current_gen:
                    if current_gen:
                        # don't be alarming on daemon start: it is normal we receive a out-of-sequence patch
                        self.log.warning("unsynchronized node %s dataset. local gen %d, received %d. "
                                         "ask for a full.", nodename, current_gen, gen)
                    self.update_node_gen(nodename, remote=0, local=our_gen_on_peer)
                    break
                try:
                    self.nodes_data.patch([nodename], deltas[str(gen)])
                    current_gen = gen
                    self.update_node_gen(nodename, remote=gen, local=our_gen_on_peer)
                    self.log.debug("patch node %s dataset to gen %d, peer has gen %d of our dataset",
                                   nodename, shared.REMOTE_GEN[nodename],
                                   shared.LOCAL_GEN[nodename])
                    if not nodes_info_change:
                        nodes_info_change |= self.patch_has_nodes_info_change(deltas[str(gen)])
                    change = True
                except Exception as exc:
                    self.log.warning("failed to apply node %s dataset gen %d patch: %s. "
                                     "ask for a full: %s", nodename, gen, deltas[str(gen)], exc)
                    self.update_node_gen(nodename, remote=0, local=our_gen_on_peer)
                    break
            if nodes_info_change:
                self.on_nodes_info_change()
            return change
        elif kind == "ping":
            self.update_node_gen(nodename, remote=0, local=our_gen_on_peer)
            self.nodes_data.set([nodename, "monitor"], data["monitor"])
            self.log.debug("reset node %s dataset gen, peer has gen %d of our dataset",
                           nodename, shared.LOCAL_GEN[nodename])
            change = True
        else:
            data_gen = data.get("gen", {}).get(nodename)
            if data_gen is None:
                self.log.debug("no 'gen' in full dataset from %s: drop", nodename)
                return False
            last_gen = shared.REMOTE_GEN.get(nodename)
            if last_gen is not None and last_gen >= data_gen:
                self.log.debug("already installed or beyond %s gen %d dataset: drop", nodename, data_gen)
                return False
            node_status = data.get("monitor", {}).get("status")
            if node_status in ("init", "maintenance", "upgrade") and self.nodes_data.exists([nodename]):
                for path, _, idata in self.iter_services_instances(nodenames=[nodename]):
                    if path in data["services"]["status"]:
                        continue
                    idata["preserved"] = True
                    data["services"]["status"][path] = idata

            self.nodes_data.set([nodename], data)
            new_gen = data.get("gen", {}).get(nodename, 0)
            self.update_node_gen(nodename, remote=new_gen, local=our_gen_on_peer)
            self.log.debug("install node %s full dataset gen %d, peer has gen %d of our dataset",
                           nodename, shared.REMOTE_GEN[nodename],
                           shared.LOCAL_GEN[nodename])
            self.on_nodes_info_change()
            change = True
        return change

