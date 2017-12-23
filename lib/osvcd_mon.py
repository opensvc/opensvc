"""
Monitor Thread
"""
import os
import sys
import time
import datetime
import codecs
import glob
import logging
import hashlib
import json
import tempfile
import shutil
from subprocess import Popen, PIPE

import osvcd_shared as shared
import rcExceptions as ex
from comm import Crypt
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import bdecode, purge_cache, fsum
from svcBuilder import build, fix_app_link, fix_exe_link

STARTED_STATES = [
    "n/a",
    "up",
]
STOPPED_STATES = [
    "n/a",
    "down",
    "stdby up",
    "stdby down",
]

class Monitor(shared.OsvcThread, Crypt):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
    monitor_period = 5
    default_stdby_nb_restart = 2

    def __init__(self):
        shared.OsvcThread.__init__(self)
        self._shutdown = False
        self.compat = True

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.last_run = 0
        self.log.info("monitor started")
        self.startup = datetime.datetime.utcnow()
        self.rejoin_grace_period_expired = False

        try:
            while True:
                self.do()
                if self.stopped():
                    self.join_threads()
                    self.terminate_procs()
                    sys.exit(0)
        except Exception as exc:
            self.log.exception(exc)

    def do(self):
        self.janitor_threads()
        self.janitor_procs()
        self.reload_config()
        self.merge_frozen()
        self.last_run = time.time()
        if self._shutdown:
            if len(self.procs) == 0:
                self.stop()
        else:
            self.merge_hb_data()
            self.orchestrator()
            self.sync_services_conf()
        self.update_hb_data()
        shared.wake_collector()

        with shared.MON_TICKER:
            shared.MON_TICKER.wait(self.monitor_period)

    def shutdown(self):
        with shared.SERVICES_LOCK:
            for svc in shared.SERVICES.values():
                self.service_shutdown(svc.svcname)
        self._shutdown = True
        shared.wake_monitor()

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
        for svcname, data in confs.items():
            new_service = False
            with shared.SERVICES_LOCK:
                if svcname not in shared.SERVICES:
                    new_service = True
            instance = self.get_service_instance(svcname, rcEnv.nodename)
            if instance:
                global_expect = instance.get("monitor", {}).get("global_expect")
                if global_expect in ("purged", "deleted"):
                    continue
            if rcEnv.nodename not in data:
                # need to check if we should have this config ?
                new_service = True
            if new_service:
                ref_conf = Storage({
                    "csum": "",
                    "updated": "0",
                })
                ref_nodename = rcEnv.nodename
            else:
                ref_conf = data[rcEnv.nodename]
                ref_nodename = rcEnv.nodename
            for nodename, conf in data.items():
                if rcEnv.nodename == nodename:
                    continue
                if rcEnv.nodename not in conf.get("scope", []):
                    # we are not a service node
                    continue
                if conf.csum != ref_conf.csum and \
                   conf.updated > ref_conf.updated:
                    ref_conf = conf
                    ref_nodename = nodename
            if ref_nodename == rcEnv.nodename:
                # we already have the most recent version
                continue
            with shared.SERVICES_LOCK:
                if svcname in shared.SERVICES and \
                   rcEnv.nodename in shared.SERVICES[svcname].nodes and \
                   ref_nodename in shared.SERVICES[svcname].drpnodes:
                    # don't fetch drp config from prd nodes
                    return
            self.log.info("node %s has the most recent service %s config",
                          ref_nodename, svcname)
            self.fetch_service_config(svcname, ref_nodename)
            if new_service:
                fix_exe_link(rcEnv.paths.svcmgr, svcname)
                fix_app_link(svcname)

    def fetch_service_config(self, svcname, nodename):
        """
        Fetch and install the most recent service configuration file, using
        the remote node listener.
        """
        request = {
            "action": "get_service_config",
            "options": {
                "svcname": svcname,
            },
        }
        resp = self.daemon_send(request, nodename=nodename)
        if resp.get("status", 1) != 0:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", svcname, nodename, resp)
            return
        with tempfile.NamedTemporaryFile(dir=rcEnv.paths.pathtmp, delete=False) as filep:
            tmpfpath = filep.name
        with codecs.open(tmpfpath, "w", "utf-8") as filep:
            filep.write(resp["data"])
        try:
            with shared.SERVICES_LOCK:
                if svcname in shared.SERVICES:
                    svc = shared.SERVICES[svcname]
                else:
                    svc = None
            if svc:
                try:
                    results = svc._validate_config(path=filep.name)
                except Exception as exc:
                    self.log.error("service %s fetched config validation "
                                   "error: %s", svcname, exc)
                    return
            else:
                results = {"errors": 0}
            if results["errors"] == 0:
                dst = os.path.join(rcEnv.paths.pathetc, svcname+".conf")
                shutil.copy(filep.name, dst)
            else:
                self.log.error("the service %s config fetched from node %s is "
                               "not valid", svcname, nodename)
                return
        finally:
            os.unlink(tmpfpath)
        shared.SERVICES[svcname] = build(svcname, node=shared.NODE)
        self.log.info("the service %s config fetched from node %s is now "
                      "installed", svcname, nodename)

    #########################################################################
    #
    # Node and Service Commands
    #
    #########################################################################
    def generic_callback(self, svcname, **kwargs):
        self.set_smon(svcname, **kwargs)
        self.update_hb_data()

    def node_stonith(self, node):
        proc = self.node_command(["stonith", "--node", node])
        self.push_proc(proc=proc)

    def service_start_resources(self, svcname, rids):
        self.set_smon(svcname, "restarting")
        proc = self.service_command(svcname, ["start", "--rid", ",".join(rids)])
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[svcname, rids],
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources_on_success(self, svcname, rids):
        self.set_smon(svcname, status="idle")
        for rid in rids:
            self.reset_smon_retries(svcname, rid)
        self.update_hb_data()

    def service_toc(self, svcname):
        proc = self.service_command(svcname, ["toc"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "toc failed"},
        )

    def service_start(self, svcname):
        self.set_smon(svcname, "starting")
        proc = self.service_command(svcname, ["start"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle", "local_expect": "started"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "start failed"},
        )

    def service_stop(self, svcname):
        self.set_smon(svcname, "stopping")
        proc = self.service_command(svcname, ["stop"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "stop failed"},
        )

    def service_shutdown(self, svcname):
        self.set_smon(svcname, "shutdown")
        proc = self.service_command(svcname, ["shutdown"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "shutdown failed"},
        )

    def service_delete(self, svcname):
        self.set_smon(svcname, "deleting", local_expect="unset")
        proc = self.service_command(svcname, ["delete"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "delete failed"},
        )

    def service_purge(self, svcname):
        self.set_smon(svcname, "unprovisioning")
        proc = self.service_command(svcname, ["unprovision"])
        self.push_proc(
            proc=proc,
            on_success="service_purge_on_success",
            on_success_args=[svcname],
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_purge_on_success(self, svcname):
        self.set_smon(svcname, "deleting", local_expect="unset")
        proc = self.service_command(svcname, ["delete"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_provision(self, svc):
        self.set_smon(svc.svcname, "provisioning")
        candidates = self.placement_candidates(svc, discard_frozen=False,
                                               discard_unprovisioned=False,
                                               discard_constraints_violation=False)
        cmd = ["provision"]
        if self.placement_leader(svc, candidates):
            cmd += ["--disable-rollback"]
        proc = self.service_command(svc.svcname, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svc.svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svc.svcname],
            on_error_kwargs={"status": "provision failed"},
        )

    def service_unprovision(self, svcname):
        self.set_smon(svcname, "unprovisioning", local_expect="unset")
        proc = self.service_command(svcname, ["unprovision"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "unprovision failed"},
        )

    def service_freeze(self, svcname):
        self.set_smon(svcname, "freezing")
        proc = self.service_command(svcname, ["freeze"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "idle"},
        )

    def service_thaw(self, svcname):
        self.set_smon(svcname, "thawing")
        proc = self.service_command(svcname, ["thaw"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcname],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "idle"},
        )


    #########################################################################
    #
    # Orchestration
    #
    #########################################################################
    def orchestrator(self):
        # node
        self.node_orchestrator()

        # services (iterate over deleting services too)
        svcnames = [svcname for svcname in shared.SMON_DATA]
        self.get_agg_services()
        for svcname in svcnames:
            svc = self.get_service(svcname)
            self.service_orchestrator(svcname, svc)
            self.resources_orchestrator(svcname, svc)

    def resources_orchestrator(self, svcname, svc):
        if svc is None:
            return
        if svc.frozen() or self.freezer.node_frozen():
            #self.log.info("resource %s orchestrator out (frozen)", svc.svcname)
            return
        if svc.disabled:
            #self.log.info("resource %s orchestrator out (disabled)", svc.svcname)
            return
        try:
            with shared.CLUSTER_DATA_LOCK:
                resources = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]["resources"]
        except KeyError:
            return

        def monitored_resource(svc, rid, resource):
            if not resource["monitor"]:
                return False
            if resource["disable"]:
                return False
            if smon.local_expect != "started":
                return False
            nb_restart = svc.get_resource(rid).nb_restart
            if nb_restart == 0 and resource.get("standby"):
                nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcname, rid)
                self.log.info("max retries (%d) reached for resource %s.%s",
                              nb_restart, svc.svcname, rid)
                self.service_toc(svc.svcname)
                return False
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("restart resource %s.%s, try %d/%d", svc.svcname, rid,
                          retries+1, nb_restart)
            return True

        def stdby_resource(svc, rid, resource):
            if resource.get("standby") is not True:
                return False
            nb_restart = svc.get_resource(rid).nb_restart
            if nb_restart < self.default_stdby_nb_restart:
                nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcname, rid)
                self.log.info("max retries (%d) reached for standby resource "
                              "%s.%s", nb_restart, svc.svcname, rid)
                return False
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("start standby resource %s.%s, try %d/%d", svc.svcname, rid,
                          retries+1, nb_restart)
            return True

        smon = self.get_service_monitor(svc.svcname)
        if smon.status != "idle":
            return

        rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("warn", "down", "stdby down"):
                continue
            if resource.get("provisioned", {}).get("state") is False:
                continue
            if monitored_resource(svc, rid, resource) or stdby_resource(svc, rid, resource):
                rids.append(rid)
                continue

        if len(rids) > 0:
            self.service_start_resources(svc.svcname, rids)

    def node_orchestrator(self):
        nmon = self.get_node_monitor()
        if nmon.status != "idle":
            return
        self.set_nmon_g_expect_from_status()
        if nmon.global_expect == "frozen":
            if not self.freezer.node_frozen():
                self.log.info("freeze node")
                self.freezer.node_freeze()
        elif nmon.global_expect == "thawed":
            if self.freezer.node_frozen():
                self.log.info("thaw node")
                self.freezer.node_thaw()

    def service_orchestrator(self, svcname, svc):
        smon = self.get_service_monitor(svcname)
        if svc is None:
            if smon and svcname in shared.AGG:
                # deleting service: unset global expect if done cluster-wide
                status = shared.AGG[svcname].avail
                self.set_smon_g_expect_from_status(svcname, smon, status)
            return
        if svc.disabled:
            #self.log.info("service %s orchestrator out (disabled)", svc.svcname)
            return
        if smon.global_expect != "aborted" and \
           smon.status not in ("ready", "idle", "wait children", "wait parents"):
            #self.log.info("service %s orchestrator out (mon status %s)", svc.svcname, smon.status)
            return
        status = shared.AGG[svc.svcname].avail
        self.set_smon_g_expect_from_status(svc.svcname, smon, status)
        if smon.global_expect:
            self.service_orchestrator_manual(svc, smon, status)
        else:
            self.service_orchestrator_auto(svc, smon, status)

    def service_orchestrator_auto(self, svc, smon, status):
        """
        Automatic instance start decision.
        Verifies hard and soft affinity and anti-affinity, then routes to
        failover and flex specific policies.
        """
        if not self.compat:
            return
        if smon.local_expect == "started":
            return
        if svc.frozen() or self.freezer.node_frozen():
            #self.log.info("service %s orchestrator out (frozen)", svc.svcname)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)",
            #              svc.svcname, status)
            return
        if self.service_orchestrator_auto_grace(svc):
            return
        if svc.hard_anti_affinity:
            intersection = set(self.get_local_svcnames()) & set(svc.hard_anti_affinity)
            if len(intersection) > 0:
                #self.log.info("service %s orchestrator out (hard anti-affinity with %s)",
                #              svc.svcname, ','.join(intersection))
                return
        if svc.hard_affinity:
            intersection = set(self.get_local_svcnames()) & set(svc.hard_affinity)
            if len(intersection) < len(set(svc.hard_affinity)):
                #self.log.info("service %s orchestrator out (hard affinity with %s)",
                #              svc.svcname, ','.join(intersection))
                return
        candidates = self.placement_candidates(svc)
        if candidates != [rcEnv.nodename]:
            # the local node is not the only candidate, we can apply soft
            # affinity filtering
            if svc.soft_anti_affinity:
                intersection = set(self.get_local_svcnames()) & set(svc.soft_anti_affinity)
                if len(intersection) > 0:
                    #self.log.info("service %s orchestrator out (soft anti-affinity with %s)",
                    #              svc.svcname, ','.join(intersection))
                    return
            if svc.soft_affinity:
                intersection = set(self.get_local_svcnames()) & set(svc.soft_affinity)
                if len(intersection) < len(set(svc.soft_affinity)):
                    #self.log.info("service %s orchestrator out (soft affinity with %s)",
                    #              svc.svcname, ','.join(intersection))
                    return

        if svc.topology == "failover":
            self.service_orchestrator_auto_failover(svc, smon, status, candidates)
        elif svc.topology == "flex":
            self.service_orchestrator_auto_flex(svc, smon, status, candidates)

    def service_orchestrator_auto_failover(self, svc, smon, status, candidates):
        if svc.orchestrate == "start":
            ranks = self.placement_ranks(svc, candidates=svc.peers)
            if ranks == []:
                return
            nodename = ranks[0]
            if nodename == rcEnv.nodename:
                # natural leader
                pass
            else:
                return
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        if smon.status in ("ready", "wait parents"):
            if instance.avail is "up":
                self.log.info("abort 'ready' because the local instance "
                              "has started")
                self.set_smon(svc.svcname, "idle")
                return
            if status == "up":
                self.log.info("abort 'ready' because an instance has started")
                self.set_smon(svc.svcname, "idle")
                return
            peer = self.better_peer_ready(svc, candidates)
            if peer:
                self.log.info("abort 'ready' because node %s has a better "
                              "placement score for service %s and is also "
                              "ready", peer, svc.svcname)
                self.set_smon(svc.svcname, "idle")
                return
            peer = self.peer_transitioning(svc)
            if peer:
                self.log.info("abort 'ready' because node %s is already "
                              "acting on service %s", peer, svc.svcname)
                self.set_smon(svc.svcname, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.svcname, status="idle")
                return
        elif smon.status == "ready":
            now = datetime.datetime.utcnow()
            if smon.status_updated < (now - self.ready_period):
                self.log.info("failover service %s status %s/ready for "
                              "%s", svc.svcname, status,
                              now-smon.status_updated)
                if smon.stonith and smon.stonith not in shared.CLUSTER_DATA:
                    # stale peer which previously ran the service
                    self.node_stonith(smon.stonith)
                self.service_start(svc.svcname)
                return
            self.log.info("service %s will start in %s",
                          svc.svcname,
                          str(smon.status_updated+self.ready_period-now))
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect != "started":
                return
            if status not in ("down", "stdby down", "stdby up"):
                return
            if not self.parents_available(svc):
                self.set_smon(svc.svcname, status="wait parents")
                return
            if len(svc.peers) == 1:
                self.log.info("failover service %s status %s/idle and "
                              "single node", svc.svcname, status)
                self.service_start(svc.svcname)
                return
            if not self.placement_leader(svc, candidates):
                return
            self.log.info("failover service %s status %s", svc.svcname,
                          status)
            self.set_smon(svc.svcname, "ready")

    def service_orchestrator_auto_flex(self, svc, smon, status, candidates):
        if svc.orchestrate == "start":
            ranks = self.placement_ranks(svc, candidates=svc.peers)
            if ranks == []:
                return
            try:
                idx = ranks.index(rcEnv.nodename)
            except ValueError:
                return
            if rcEnv.nodename not in ranks[:svc.flex_min_nodes]:
                # not a natural leader, skip orchestration
                return
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        up_nodes = self.up_service_instances(svc.svcname)
        n_up = len(up_nodes)
        if smon.status in ("ready", "wait parents"):
            if (n_up - 1) >= svc.flex_min_nodes:
                self.log.info("flex service %s instance count reached "
                              "required minimum while we were ready",
                              svc.svcname)
                self.set_smon(svc.svcname, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.svcname, status="idle")
                return
        if smon.status == "ready":
            now = datetime.datetime.utcnow()
            if smon.status_updated < (now - self.ready_period):
                self.log.info("flex service %s status %s/ready for %s",
                              svc.svcname, status, now-smon.status_updated)
                self.service_start(svc.svcname)
            else:
                self.log.info("service %s will start in %s", svc.svcname,
                              str(smon.status_updated+self.ready_period-now))
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect != "started":
                return
            self.log.info("%d/%d-%d", n_up, svc.flex_min_nodes, svc.flex_max_nodes)
            if n_up < svc.flex_min_nodes:
                if instance.avail not in STOPPED_STATES:
                    return
                if not self.placement_leader(svc, candidates):
                    return
                if not self.parents_available(svc):
                    self.set_smon(svc.svcname, status="wait parents")
                    return
                self.log.info("flex service %s started, starting or ready to "
                              "start instances: %d/%d-%d. local status %s",
                              svc.svcname, n_up, svc.flex_min_nodes,
                              svc.flex_max_nodes, instance.avail)
                self.set_smon(svc.svcname, "ready")
            elif n_up > svc.flex_max_nodes:
                if instance.avail not in STARTED_STATES:
                    return
                n_to_stop = n_up - svc.flex_max_nodes
                to_stop = self.placement_ranks(svc, candidates=up_nodes)[-n_to_stop:]
                self.log.info("%d nodes to stop to honor flex_max_nodes=%d. "
                              "choose %s", n_to_stop, svc.flex_max_nodes,
                              ", ".join(to_stop))
                if rcEnv.nodename not in to_stop:
                    return
                self.log.info("flex service %s started, starting or ready to "
                              "start instances: %d/%d-%d. local status %s",
                              svc.svcname, n_up, svc.flex_min_nodes,
                              svc.flex_max_nodes, instance.avail)
                self.service_stop(svc.svcname)

    def service_orchestrator_manual(self, svc, smon, status):
        """
        Take actions to meet global expect target, set by user or by
        service_orchestrator_auto()
        """
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        if smon.global_expect == "frozen":
            if not svc.frozen():
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
        elif smon.global_expect == "thawed":
            if svc.frozen():
                self.log.info("thaw service %s", svc.svcname)
                self.service_thaw(svc.svcname)
        elif smon.global_expect == "shutdown":
            if not self.children_down(svc):
                self.set_smon(svc.svcname, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcname, status="idle")

            if not svc.frozen():
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
            elif not self.is_instance_shutdown(instance):
                thawed_on = self.service_instances_thawed(svc.svcname)
                if thawed_on:
                    self.duplog("info", "service %(svcname)s still has thawed instances "
                                "on nodes %(thawed_on)s, delay shutdown",
                                svcname=svc.svcname,
                                thawed_on=", ".join(thawed_on))
                else:
                    self.service_shutdown(svc.svcname)
        elif smon.global_expect == "stopped":
            if not self.children_down(svc):
                self.set_smon(svc.svcname, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcname, status="idle")

            if not svc.frozen():
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
            elif instance.avail not in STOPPED_STATES:
                thawed_on = self.service_instances_thawed(svc.svcname)
                if thawed_on:
                    self.duplog("info", "service %(svcname)s still has thawed instances "
                                "on nodes %(thawed_on)s, delay stop",
                                svcname=svc.svcname,
                                thawed_on=", ".join(thawed_on))
                else:
                    self.service_stop(svc.svcname)
        elif smon.global_expect == "started":
            if svc.frozen():
                self.log.info("thaw service %s", svc.svcname)
                self.service_thaw(svc.svcname)
            elif status not in STARTED_STATES:
                self.service_orchestrator_auto(svc, smon, status)
        elif smon.global_expect == "unprovisioned":
            if not self.service_unprovisioned(instance) and \
               self.leader_first(svc, provisioned=False):
                self.service_unprovision(svc.svcname)
        elif smon.global_expect == "provisioned":
            if not self.service_provisioned(instance) and \
               self.leader_first(svc, provisioned=True):
                self.service_provision(svc)
        elif smon.global_expect == "deleted":
            if svc.svcname in shared.SERVICES:
                self.service_delete(svc.svcname)
        elif smon.global_expect == "purged" and \
             self.leader_first(svc, provisioned=False, deleted=False):
            if svc.svcname in shared.SERVICES and \
               (not self.service_unprovisioned(instance) or instance is not None):
                self.service_purge(svc.svcname)
        elif smon.global_expect == "aborted" and \
             smon.local_expect not in (None, "started"):
            self.set_smon(svc.svcname, local_expect="unset")

    def service_orchestrator_auto_grace(self, svc):
        """
        After daemon startup, wait for <rejoin_grace_period_expired> seconds
        before allowing service_orchestrator_auto() to proceed.
        """
        if self.rejoin_grace_period_expired:
            return False
        if len(self.cluster_nodes) == 1:
            self.rejoin_grace_period_expired = True
            self.duplog("info", "end of rejoin grace period: single node cluster", svcname="")
            return False
        n_reachable = len(shared.CLUSTER_DATA.keys())
        if n_reachable > 1:
            self.rejoin_grace_period_expired = True
            if n_reachable < len(self.cluster_nodes):
                self.freezer.node_freeze()
                self.duplog("info", "end of rejoin grace period: now rejoined "
                            "but frozen (cluster incomplete)", svcname="")
            else:
                self.duplog("info", "end of rejoin grace period: now rejoined", svcname="")
            return False
        now = datetime.datetime.utcnow()
        if now > self.startup + datetime.timedelta(seconds=self.rejoin_grace_period):
            self.rejoin_grace_period_expired = True
            self.duplog("info", "rejoin grace period expired", svcname="")
            return False
        if len(svc.peers) == 1:
            return False
        self.duplog("info", "in rejoin grace period", svcname="")
        return True

    def children_down(self, svc):
        missing = []
        if len(svc.children) == 0:
            return True
        for child in svc.children:
            if child == svc.svcname:
                continue
            try:
                avail = shared.AGG[child].avail
            except KeyError:
                avail = "unknown"
            if avail in STOPPED_STATES + ["unknown"]:
                continue
            missing.append(child)
        if len(missing) == 0:
            self.duplog("info", "service %(svcname)s children all avail down",
                        svcname=svc.svcname)
            return True
        self.duplog("info", "service %(svcname)s children still available:"
                    " %(missing)s", svcname=svc.svcname,
                    missing=" ".join(missing))
        return False

    def parents_available(self, svc):
        missing = []
        if len(svc.parents) == 0:
            return True
        for parent in svc.parents:
            if parent == svc.svcname:
                continue
            try:
                avail = shared.AGG[parent].avail
            except KeyError:
                avail = "unknown"
            if avail in STARTED_STATES + ["unknown"]:
                continue
            missing.append(parent)
        if len(missing) == 0:
            self.duplog("info", "service %(svcname)s parents all avail up",
                        svcname=svc.svcname)
            return True
        self.duplog("info", "service %(svcname)s parents not available:"
                    " %(missing)s", svcname=svc.svcname,
                    missing=" ".join(missing))
        return False

    def min_instances_reached(self, svc):
        instances = self.get_service_instances(svc.svcname, discard_empty=False)
        live_nodes = [nodename for nodename in shared.CLUSTER_DATA if shared.CLUSTER_DATA[nodename] is not None]
        min_instances = set(svc.peers) & set(live_nodes)
        return len(instances) >= len(min_instances)

    def leader_first(self, svc, provisioned=False, deleted=None):
        """
        Return True if the peer selected for anteriority is found to have
        reached the target status, or if the local node is the one with
        anteriority.

        Anteriority selection is done with these criteria:

        * choose the placement top node amongst node with a up instance
        * if none, choose the placement top node amongst all nodes,
          whatever their frozen, constraints, and current provisioning
          state.
        """
        if not self.min_instances_reached(svc):
            self.log.info("delay leader-first action until all nodes have fetched the service config")
            return False
        instances = self.get_service_instances(svc.svcname, discard_empty=True)
        candidates = [nodename for (nodename, data) in instances.items() \
                      if data.get("avail") in ("up", "warn")]
        if len(candidates) == 0:
            self.log.info("service %s has no up instance, relax candidates "
                          "constraints", svc.svcname)
            candidates = self.placement_candidates(svc, discard_frozen=False,
                                                   discard_unprovisioned=False)
        try:
            top = self.placement_ranks(svc, candidates=candidates)[0]
            self.log.info("elected %s as the first node to take action on "
                          "service %s", top, svc.svcname)
        except IndexError:
            self.log.error("service %s placement ranks list is empty", svc.svcname)
            return False
        if top == rcEnv.nodename:
            return True
        instance = self.get_service_instance(svc.svcname, top)
        if instance is None:
            return True
        if instance["provisioned"] is provisioned:
            return True
        self.log.info("delay leader-first action")
        return False

    def up_service_instances(self, svcname):
        nodenames = []
        for nodename, instance in self.get_service_instances(svcname).items():
            if instance["avail"] == "up":
                nodenames.append(nodename)
            elif instance["monitor"]["status"] in ("restarting", "starting", "wait children", "ready"):
                nodenames.append(nodename)
        return nodenames

    def peer_transitioning(self, svc):
        """
        Return the nodename of the first peer with the service in a transition
        state.
        """
        for nodename, instance in self.get_service_instances(svc.svcname).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"]["status"].endswith("ing"):
                return nodename

    def better_peer_ready(self, svc, candidates):
        """
        Return the nodename of the first peer with the service in ready state, or
        None if we are placement leader or no peer is in ready state.
        """
        if self.placement_leader(svc, candidates):
            return
        for nodename, instance in self.get_service_instances(svc.svcname).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"]["status"] == "ready":
                return nodename

    #########################################################################
    #
    # Cluster nodes aggregations
    #
    #########################################################################
    @staticmethod
    def get_clu_agg_frozen():
        fstatus = "undef"
        fstatus_l = []
        n_instances = 0
        with shared.CLUSTER_DATA_LOCK:
            for node in shared.CLUSTER_DATA.values():
                try:
                    fstatus_l.append(node["frozen"])
                except KeyError:
                    # sender daemon outdated
                    continue
                n_instances += 1
        n_frozen = fstatus_l.count(True)
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
    def get_agg_avail(self, svcname):
        try:
            instance = self.get_any_service_instance(svcname)
        except IndexError:
            instance = Storage()
        if instance is None:
            self.log.error("service %s has no instance", svcname)
            return "unknown"
        topology = instance.get("topology")
        if topology == "failover":
            avail = self.get_agg_avail_failover(svcname)
        elif topology == "flex":
            avail = self.get_agg_avail_flex(svcname)
        else:
            avail = "unknown"
        if instance.get("enslave_children"):
            avails = set([avail])
            for child in instance["children"]:
                try:
                    child_avail = shared.AGG[child]["avail"]
                except KeyError:
                    child_avail = "unknown"
                avails.add(child_avail)
            avails -= set(["n/a"])
            if len(avails) == 1:
                return list(avails)[0]
            return "warn"
        return avail

    def get_agg_overall(self, svcname):
        ostatus = 'undef'
        ostatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcname).values():
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
        return ostatus

    def get_agg_frozen(self, svcname):
        frozen = 0
        total = 0
        for instance in self.get_service_instances(svcname).values():
            if "frozen" not in instance:
                # deleting instance
                continue
            if instance["frozen"]:
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
            for resource in instance["resources"].values():
                if resource["standby"]:
                    return True
            return False
        _has_stdby = has_stdby(instance)
        if _has_stdby and instance["avail"] not in ("n/a", "stdby down") or \
           not _has_stdby and instance["avail"] not in ("n/a", "down"):
            return False
        return True

    def get_agg_shutdown(self, svcname):
        for instance in self.get_service_instances(svcname).values():
            if not self.is_instance_shutdown(instance):
                return False
        return True 

    def get_agg_avail_failover(self, svcname):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcname).values():
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
        elif 'warn' in astatus_l:
            return 'warn'
        elif n_up > 1:
            return 'warn'
        elif n_up == 1:
            return 'up'
        else:
            return 'down'

    def get_agg_avail_flex(self, svcname):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcname).values():
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
            return 'down'
        elif 'warn' in astatus_l:
            return 'warn'
        elif n_up > instance["flex_max_nodes"]:
            return 'warn'
        elif n_up < instance["flex_min_nodes"]:
            return 'warn'
        else:
            return 'up'

    def get_agg_placement(self, svcname):
        has_up = False
        placement = "optimal"
        for instance in self.get_service_instances(svcname).values():
            if "avail" not in instance:
                continue
            if instance["avail"] == "up":
                has_up = True
            elif instance["monitor"].get("placement") == "leader":
                placement = "non-optimal"
        if not has_up:
            return "n/a"
        return placement

    def get_agg_provisioned(self, svcname):
        provisioned = 0
        total = 0
        for instance in self.get_service_instances(svcname).values():
            if "provisioned" not in instance:
                continue
            total += 1
            if instance["provisioned"]:
                provisioned += 1
        if total == 0:
            return "n/a"
        elif provisioned == total:
            return True
        elif provisioned == 0:
            return False
        return "mixed"

    def get_agg_aborted(self, svcname):
        for inst in self.get_service_instances(svcname).values():
            try:
                global_expect = inst["monitor"]["global_expect"]
            except KeyError:
                global_expect = None
            if global_expect not in (None, "aborted"):
                return False
            try:
                local_expect = inst["monitor"]["local_expect"]
            except KeyError:
                local_expect = None
            if local_expect not in (None, "started"):
                return False
        return True

    def get_agg_deleted(self, svcname):
        if len([True for inst in self.get_service_instances(svcname).values() if "avail" in inst]) > 0:
            return False
        return True

    def get_agg_purged(self, provisioned, deleted):
        if deleted is False:
            return False
        if provisioned in (False, "mixed"):
            return False
        return True

    #
    def service_instances_frozen(self, svcname):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcname).items() if \
                instance["frozen"]]

    def service_instances_thawed(self, svcname):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcname).items() if \
                not instance["frozen"]]

    @staticmethod
    def get_local_svcnames():
        """
        Extract service instance names from the locally maintained hb data.
        """
        svcnames = []
        try:
            with shared.CLUSTER_DATA_LOCK:
                for svcname in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                    if shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["avail"] == "up":
                        svcnames.append(svcname)
        except KeyError:
            return []
        return svcnames

    @staticmethod
    def get_services_configs():
        """
        Return a hash indexed by svcname and nodename, containing the services
        configuration mtime and checksum.
        """
        data = {}
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                try:
                    for svcname in shared.CLUSTER_DATA[nodename]["services"]["config"]:
                        if svcname not in shared.CLUSTER_DATA[nodename]["services"]["status"] or \
                           "avail" not in shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]:
                            # deleting
                            continue
                        if svcname not in data:
                            data[svcname] = {}
                        data[svcname][nodename] = Storage(shared.CLUSTER_DATA[nodename]["services"]["config"][svcname])
                except KeyError:
                    pass
        return data

    @staticmethod
    def get_service_instances(svcname, discard_empty=False):
        """
        Return the specified service status structures on all nodes.
        """
        instances = {}
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                try:
                    if svcname in shared.CLUSTER_DATA[nodename]["services"]["status"]:
                        if discard_empty and shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]:
                            continue
                        instances[nodename] = shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]
                except KeyError:
                    continue
        return instances

    @staticmethod
    def get_any_service_instance(svcname):
        """
        Return the specified service status structure on any node.
        """
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                try:
                    if svcname in shared.CLUSTER_DATA[nodename]["services"]["status"]:
                        if shared.CLUSTER_DATA[nodename]["services"]["status"][svcname] in (None, ""):
                            continue
                        return shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]
                except KeyError:
                    continue

    @staticmethod
    def get_last_svc_config(svcname):
        with shared.CLUSTER_DATA_LOCK:
            try:
                return shared.CLUSTER_DATA[rcEnv.nodename]["services"]["config"][svcname]
            except KeyError:
                return

    def get_services_config(self):
        config = {}
        for cfg in glob.glob(os.path.join(rcEnv.paths.pathetc, "*.conf")):
            svcname = os.path.basename(cfg[:-5])
            if svcname == "node":
                continue
            linkp = os.path.join(rcEnv.paths.pathetc, svcname)
            if not os.path.exists(linkp):
                continue
            try:
                mtime = os.path.getmtime(cfg)
            except Exception as exc:
                self.log.warning("failed to get %s mtime: %s", cfg, str(exc))
                mtime = 0
            mtime = datetime.datetime.utcfromtimestamp(mtime)
            last_config = self.get_last_svc_config(svcname)
            if last_config is None or mtime > datetime.datetime.strptime(last_config["updated"], shared.DATEFMT):
                self.log.info("compute service %s config checksum", svcname)
                try:
                    csum = fsum(cfg)
                except (OSError, IOError) as exc:
                    self.log.info("service %s config checksum error: %s", svcname, exc)
                    continue
                try:
                    with shared.SERVICES_LOCK:
                        shared.SERVICES[svcname] = build(svcname, node=shared.NODE)
                except Exception as exc:
                    self.log.error("%s build error: %s", svcname, str(exc))
                    continue
            else:
                csum = last_config["csum"]
            if last_config is None:
                self.log.info("purge %s status cache" % svcname)
                shared.SERVICES[svcname].purge_status_caches()
            with shared.SERVICES_LOCK:
                scope = sorted(list(shared.SERVICES[svcname].nodes | shared.SERVICES[svcname].drpnodes))
            config[svcname] = {
                "updated": mtime.strftime(shared.DATEFMT),
                "csum": csum,
                "scope": scope,
            }

        # purge deleted services
        with shared.SERVICES_LOCK:
            for svcname in list(shared.SERVICES.keys()):
                if svcname not in config:
                    self.log.info("purge deleted service %s", svcname)
                    del shared.SERVICES[svcname]
                    try:
                        del shared.SMON_DATA[svcname]
                    except KeyError:
                        pass
        return config

    def get_last_svc_status_mtime(self, svcname):
        """
        Return the mtime of the specified service configuration file on the
        local node. If unknown, return 0.
        """
        instance = self.get_service_instance(svcname, rcEnv.nodename)
        if instance is None:
            return 0
        mtime = instance["mtime"]
        if mtime is None:
            return 0
        return mtime

    def service_status_fallback(self, svcname):
        """
        Return the specified service status structure fetched from an execution
        of svcmgr -s <svcname> json status". As we arrive here when the
        status.json doesn't exist, we don't have to specify --refresh.
        """
        self.log.info("slow path service status eval: %s", svcname)
        try:
            with shared.SERVICES_LOCK:
                return shared.SERVICES[svcname].print_status_data(mon_data=False)
        except KeyboardInterrupt:
            return
        except Exception as exc:
            self.log.warning("failed to evaluate service %s status", svcname)
            return

    def get_services_status(self, svcnames):
        """
        Return the local services status data, fetching data from status.json
        caches if their mtime changed or from CLUSTER_DATA[rcEnv.nodename] if
        not.

        Also update the monitor 'local_expect' field for each service.
        """

        # purge data cached by the @cache decorator
        purge_cache()

        # this data ends up in CLUSTER_DATA[rcEnv.nodename]["services"]["status"]
        data = {}

        for svcname in svcnames:
            fpath = os.path.join(rcEnv.paths.pathvar, "services", svcname, "status.json")
            last_mtime = self.get_last_svc_status_mtime(svcname)
            try:
                mtime = os.path.getmtime(fpath)
            except Exception:
                # preserve previous status data if any (an action might be running)
                mtime = 0
            if mtime > last_mtime + 0.0001:
                try:
                    with open(fpath, 'r') as filep:
                        try:
                            data[svcname] = json.load(filep)
                            #self.log.info("service %s status reloaded", svcname)
                        except ValueError as exc:
                            # json corrupted
                            pass
                except Exception as exc:
                    # json not found
                    pass


            if svcname not in data:
                if last_mtime > 0:
                    #self.log.info("service %s status preserved", svcname)
                    data[svcname] = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]
                else:
                    data[svcname] = self.service_status_fallback(svcname)

            if not data[svcname]:
                del data[svcname]
                continue

            # update the frozen instance attribute
            with shared.SERVICES_LOCK:
                data[svcname]["frozen"] = shared.SERVICES[svcname].frozen()

            # embed the updated smon data
            self.set_smon_l_expect_from_status(data, svcname)
            data[svcname]["monitor"] = self.get_service_monitor(svcname,
                                                                datestr=True)

            # forget the stonith target node if we run the service
            if data[svcname]["avail"] == "up" and "stonith" in data[svcname]["monitor"]:
                del data[svcname]["monitor"]["stonith"]

        # deleting services (still in SMON_DATA, no longer has cf).
        # emulate a status
        for svcname in set(shared.SMON_DATA.keys()) - set(svcnames):
            data[svcname] = {
                "monitor": self.get_service_monitor(svcname, datestr=True),
                "resources": {},
            }

        return data

    #########################################################################
    #
    # Service-specific monitor data helpers
    #
    #########################################################################
    @staticmethod
    def reset_smon_retries(svcname, rid):
        with shared.SMON_DATA_LOCK:
            if svcname not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[svcname]:
                return
            if rid in shared.SMON_DATA[svcname].restart:
                del shared.SMON_DATA[svcname].restart[rid]
            if len(shared.SMON_DATA[svcname].restart.keys()) == 0:
                del shared.SMON_DATA[svcname].restart

    @staticmethod
    def get_smon_retries(svcname, rid):
        with shared.SMON_DATA_LOCK:
            if svcname not in shared.SMON_DATA:
                return 0
            if "restart" not in shared.SMON_DATA[svcname]:
                return 0
            if rid not in shared.SMON_DATA[svcname].restart:
                return 0
            else:
                return shared.SMON_DATA[svcname].restart[rid]

    @staticmethod
    def inc_smon_retries(svcname, rid):
        with shared.SMON_DATA_LOCK:
            if svcname not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[svcname]:
                shared.SMON_DATA[svcname].restart = Storage()
            if rid not in shared.SMON_DATA[svcname].restart:
                shared.SMON_DATA[svcname].restart[rid] = 1
            else:
                shared.SMON_DATA[svcname].restart[rid] += 1

    def all_nodes_frozen(self):
        with shared.CLUSTER_DATA_LOCK:
             for data in shared.CLUSTER_DATA.values():
                 if not data["frozen"]:
                     return False
        return True

    def all_nodes_thawed(self):
        with shared.CLUSTER_DATA_LOCK:
             for data in shared.CLUSTER_DATA.values():
                 if data["frozen"]:
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

    def set_smon_g_expect_from_status(self, svcname, smon, status):
        """
        Align global_expect with the actual service states.
        """
        if smon.global_expect is None:
            return
        instance = self.get_service_instance(svcname, rcEnv.nodename)
        if instance is None:
            return
        local_frozen = instance["frozen"]
        frozen = shared.AGG[svcname].frozen
        provisioned = shared.AGG[svcname].provisioned
        deleted = self.get_agg_deleted(svcname)
        purged = self.get_agg_purged(provisioned, deleted)
        if smon.global_expect == "stopped" and status in STOPPED_STATES and \
           local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcname, smon.global_expect, status)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "shutdown" and self.get_agg_shutdown(svcname) and \
           local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcname, smon.global_expect, status)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "started" and \
             status in STARTED_STATES and not local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcname, smon.global_expect, status)
            self.set_smon(svcname, global_expect="unset")
        elif (smon.global_expect == "frozen" and frozen == "frozen") or \
             (smon.global_expect == "thawed" and frozen == "thawed") or \
             (smon.global_expect == "unprovisioned" and provisioned is False) or \
             (smon.global_expect == "provisioned" and provisioned is True):
            self.log.info("service %s global expect is %s, already is",
                          svcname, smon.global_expect)
            self.set_smon(svcname, global_expect="unset")
        elif (smon.global_expect == "purged" and purged is True) or \
             (smon.global_expect == "deleted" and deleted is True):
            self.log.info("service %s global expect is %s, already is",
                          svcname, smon.global_expect)
            with shared.SMON_DATA_LOCK:
                del shared.SMON_DATA[svcname]
        elif smon.global_expect == "aborted" and \
             self.get_agg_aborted(svcname):
            self.log.info("service %s action aborted", svcname)
            self.set_smon(svcname, global_expect="unset")

    def set_smon_l_expect_from_status(self, data, svcname):
        if svcname not in data:
            return
        with shared.SMON_DATA_LOCK:
            if svcname not in shared.SMON_DATA:
                return
            if data[svcname]["avail"] == "up" and \
               shared.SMON_DATA[svcname].global_expect is None and \
               shared.SMON_DATA[svcname].status == "idle" and \
               shared.SMON_DATA[svcname].local_expect != "started":
                self.log.info("service %s monitor local_expect change "
                              "%s => %s", svcname,
                              shared.SMON_DATA[svcname].local_expect, "started")
                shared.SMON_DATA[svcname].local_expect = "started"

    def getloadavg(self):
        try:
            return round(os.getloadavg()[2], 1)
        except:
            # None < 0 == True
            return

    def update_hb_data(self):
        """
        Update the heartbeat payload we send to other nodes.
        Crypt it so the tx threads don't have to do it on their own.
        """
        #self.log.info("update heartbeat data to send")
        load_avg = self.getloadavg()
        config = self.get_services_config()
        status = self.get_services_status(config.keys())

        # purge deleted service instances
        for svcname in status:
            if svcname not in config:
                self.log.info(config)
                del status[svcname]

        try:
            with shared.CLUSTER_DATA_LOCK:
                shared.CLUSTER_DATA[rcEnv.nodename] = {
                    "frozen": self.freezer.node_frozen(),
                    "compat": shared.COMPAT_VERSION,
                    "env": rcEnv.node_env,
                    "monitor": self.get_node_monitor(datestr=True),
                    "updated": datetime.datetime.utcfromtimestamp(time.time())\
                                                .strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "services": {
                        "config": config,
                        "status": status,
                    },
                    "load": {
                        "15m": load_avg,
                    },
                }
            with shared.HB_MSG_LOCK:
                shared.HB_MSG = self.encrypt(shared.CLUSTER_DATA[rcEnv.nodename])
                if shared.HB_MSG is None:
                    shared.HB_MSG_LEN = 0
                else:
                    shared.HB_MSG_LEN = len(shared.HB_MSG)
            shared.wake_heartbeat_tx()
        except ValueError:
            self.log.error("failed to refresh local cluster data: invalid json")

    def merge_hb_data(self):
        self.merge_hb_data_compat()
        self.merge_hb_data_monitor()
        self.merge_hb_data_provision()

    def merge_hb_data_compat(self):
        compat = [data.get("compat") for data in shared.CLUSTER_DATA.values()]
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
        with shared.CLUSTER_DATA_LOCK:
            nodenames = list(shared.CLUSTER_DATA.keys())
        if rcEnv.nodename not in nodenames:
            return
        nodenames.remove(rcEnv.nodename)
        with shared.CLUSTER_DATA_LOCK:
            # merge node monitors
            for nodename in nodenames:
                try:
                    global_expect = shared.CLUSTER_DATA[nodename]["monitor"].get("global_expect")
                except KeyError:
                    # sender daemon is outdated
                    continue
                if global_expect is None:
                    continue
                local_frozen = shared.CLUSTER_DATA[rcEnv.nodename]["frozen"]
                if (global_expect == "frozen" and not local_frozen) or \
                   (global_expect == "thawed" and local_frozen):
                    self.log.info("node %s wants local node %s", nodename, global_expect)
                    self.set_nmon(global_expect=global_expect)
                else:
                    self.log.info("node %s wants local node %s, already is", nodename, global_expect)

            # merge every service monitors
            for svcname, instance in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"].items():
                current_global_expect = instance["monitor"].get("global_expect")
                if current_global_expect == "aborted":
                    # refuse a new global expect if aborting
                    continue
                for nodename in nodenames:
                    rinstance = self.get_service_instance(svcname, nodename)
                    if rinstance is None:
                        continue
                    if rinstance.get("stonith") is True:
                        self.set_smon(svcname, stonith=nodename)
                    global_expect = rinstance["monitor"].get("global_expect")
                    if global_expect is None:
                        continue
                    if global_expect == current_global_expect:
                        self.log.info("node %s wants service %s %s, already targeting that",
                                      nodename, svcname, global_expect)
                        continue
                    if self.accept_g_expect(svcname, instance, global_expect):
                        self.log.info("node %s wants service %s %s", nodename, svcname, global_expect)
                        self.set_smon(svcname, global_expect=global_expect)
                    else:
                        self.log.info("node %s wants service %s %s, already is", nodename, svcname, global_expect)

    def accept_g_expect(self, svcname, instance, global_expect):
        if svcname not in shared.AGG:
            return False
        if global_expect == "stopped":
            local_avail = instance["avail"]
            local_frozen = instance["frozen"]
            if local_avail not in STOPPED_STATES or not local_frozen:
                return True
            else:
                return False
        if global_expect == "shutdown":
            return not self.get_agg_shutdown(svcname)
        if global_expect == "started":
            status = shared.AGG[svcname].avail
            local_frozen = instance["frozen"]
            if status not in STARTED_STATES or local_frozen:
                return True
            else:
                return False
        if global_expect == "frozen":
            frozen = shared.AGG[svcname].frozen
            if frozen != "frozen":
                return True
            else:
                return False
        if global_expect == "thawed":
            frozen = shared.AGG[svcname].frozen
            if frozen != "thawed":
                 return True
            else:
                return False
        if global_expect == "provisioned":
            provisioned = shared.AGG[svcname].provisioned
            if provisioned is not True:
                return True
            else:
                return False
        if global_expect == "unprovisioned":
            provisioned = shared.AGG[svcname].provisioned
            if provisioned is not False:
                return True
            else:
                return False
        if global_expect == "deleted":
            deleted = self.get_agg_deleted(svcname)
            if deleted is False:
                return True
            else:
                return False
        if global_expect == "purged":
            provisioned = shared.AGG[svcname].provisioned
            deleted = self.get_agg_deleted(svcname)
            purged = self.get_agg_purged(provisioned, deleted)
            if purged is False:
                return True
            else:
                return False
        if global_expect == "aborted":
            aborted = self.get_agg_aborted(svcname)
            if aborted is False:
                return True
            else:
                return False
        return False

    def merge_hb_data_provision(self):
        """
        Merge the resource provisioned state from the peer with the most
        up-to-date change time.
        """
        with shared.SERVICES_LOCK:
            with shared.CLUSTER_DATA_LOCK:
                for svc in shared.SERVICES.values():
                    changed = False
                    for resource in svc.shared_resources:
                        try:
                            local = Storage(shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]["resources"][resource.rid]["provisioned"])
                        except KeyError:
                            local = Storage()
                        for nodename in svc.peers:
                            if nodename == rcEnv.nodename:
                                continue
                            try:
                                remote = Storage(shared.CLUSTER_DATA[nodename]["services"]["status"][svc.svcname]["resources"][resource.rid]["provisioned"])
                            except KeyError:
                                continue
                            if remote is None or remote.state is None or remote.mtime is None:
                                continue
                            elif local.mtime is None or remote.mtime > local.mtime + 0.00001:
                                self.log.info("switch %s.%s provisioned flag to %s (merged from %s)",
                                              svc.svcname, resource.rid, str(remote.state), 
                                              nodename)
                                resource.write_is_provisioned_flag(remote.state, remote.mtime)
                                shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]["resources"][resource.rid]["provisioned"]["mtime"] = remote.mtime
                                changed = True
                    if changed:
                        svc.purge_status_data_dump()

    def service_provisioned(self, instance):
        return instance.get("provisioned")

    def service_unprovisioned(self, instance):
        for resource in instance["resources"].values():
            if resource.get("provisioned", {}).get("state") is True:
                return False
        return True

    def get_agg(self, svcname):
        data = Storage()
        data.avail = self.get_agg_avail(svcname)
        data.frozen = self.get_agg_frozen(svcname)
        data.overall = self.get_agg_overall(svcname)
        data.placement = self.get_agg_placement(svcname)
        data.provisioned = self.get_agg_provisioned(svcname)
        with shared.AGG_LOCK:
            shared.AGG[svcname] = data
        return data

    def get_agg_services(self):
        svcnames = set()
        for nodename, data in shared.CLUSTER_DATA.items():
            try:
                for svcname in data["services"]["config"]:
                    svcnames.add(svcname)
            except KeyError:
                continue
        data = {}
        for svcname in svcnames:
            data[svcname] = self.get_agg(svcname)
        return data

    def status(self, **kwargs):
        if kwargs.get("refresh"):
            self.update_hb_data()
        data = shared.OsvcThread.status(self, **kwargs)
        with shared.CLUSTER_DATA_LOCK:
            data.nodes = dict(shared.CLUSTER_DATA)
        data["compat"] = self.compat
        data["frozen"] = self.get_clu_agg_frozen()
        data["services"] = self.get_agg_services()
        return data

    def merge_frozen(self):
        """
        This method is only called during the grace period.

        It freezes the local services instances for services that have
        a live remote instance frozen. This prevents a node
        rejoining the cluster from taking over services that where frozen
        and stopped while we were not alive.
        """
        if self.rejoin_grace_period_expired:
            return
        for svc in shared.SERVICES.values():
            if svc.orchestrate == "no":
                continue
            if len(svc.peers) < 2:
                continue
            if svc.frozen():
                continue
            for peer in svc.peers:
                if peer == rcEnv.nodename:
                    continue
                try:
                    frozen = shared.CLUSTER_DATA[peer]["services"]["status"][svc.svcname]["frozen"]
                except:
                    continue
                if frozen:
                    svc.freeze()

