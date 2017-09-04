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
from subprocess import Popen, PIPE

import osvcd_shared as shared
from comm import Crypt
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import bdecode
from svcBuilder import build, fix_app_link, fix_exe_link

MON_WAIT_READY = datetime.timedelta(seconds=16)

STARTED_STATES = (
    "up",
)
STOPPED_STATES = (
    "down",
    "stdby up",
    "stdby down",
)

class Monitor(shared.OsvcThread, Crypt):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
    monitor_period = 5
    default_stdby_nb_restart = 2

    def __init__(self):
        shared.OsvcThread.__init__(self)
        self._shutdown = False

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.last_run = 0
        self.log.info("monitor started")

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.terminate_procs()
                sys.exit(0)

    def do(self):
        self.janitor_threads()
        self.janitor_procs()
        self.reload_config()
        self.last_run = time.time()
        if self._shutdown:
            if len(self.procs) == 0:
                self.stop()
        else:
            self.merge_hb_data()
            self.orchestrator()
            self.sync_services_conf()
        self.update_hb_data()

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
            if rcEnv.nodename not in data:
                # need to check if we should have this config ?
                new_service = True
            if new_service:
                ref_conf = Storage({
                    "cksum": "",
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
                if conf.cksum != ref_conf.cksum and \
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
                with shared.SERVICES_LOCK:
                    shared.SERVICES[svcname] = build(svcname, node=shared.NODE)

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
        import tempfile
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
                results = svc._validate_config(path=filep.name)
            else:
                results = {"errors": 0}
            if results["errors"] == 0:
                import shutil
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
    def service_start_resources(self, svcname, rids):
        self.set_smon(svcname, "restarting")
        proc = self.service_command(svcname, ["start", "--rid", ",".join(rids)])
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[svcname, rids],
            on_error="service_start_resources_on_error",
            on_error_args=[svcname, rids],
        )

    def service_start_resources_on_error(self, svcname, rids):
        self.set_smon(svcname, status="idle", local_expect="started")
        self.update_hb_data()

    def service_start_resources_on_success(self, svcname, rids):
        self.set_smon(svcname, status="idle", local_expect="started")
        for rid in rids:
            self.reset_smon_retries(svcname, rid)
        self.update_hb_data()

    def service_toc(self, svcname):
        proc = self.service_command(svcname, ["toc"])
        self.push_proc(
            proc=proc,
            on_success="service_toc_on_success", on_success_args=[svcname],
            on_error="service_toc_on_error", on_error_args=[svcname],
        )

    def service_toc_on_error(self, svcname):
        self.set_smon(svcname, "idle")
        self.update_hb_data()

    def service_toc_on_success(self, svcname):
        self.set_smon(svcname, status="idle")
        self.update_hb_data()

    def service_start(self, svcname):
        self.set_smon(svcname, "starting")
        proc = self.service_command(svcname, ["start"])
        self.push_proc(
            proc=proc,
            on_success="service_start_on_success", on_success_args=[svcname],
            on_error="service_start_on_error", on_error_args=[svcname],
        )

    def service_start_on_error(self, svcname):
        self.set_smon(svcname, "start failed")
        self.update_hb_data()

    def service_start_on_success(self, svcname):
        self.set_smon(svcname, status="idle", local_expect="started")
        self.update_hb_data()

    def service_stop(self, svcname):
        self.set_smon(svcname, "stopping")
        proc = self.service_command(svcname, ["stop"])
        self.push_proc(
            proc=proc,
            on_success="service_stop_on_success", on_success_args=[svcname],
            on_error="service_stop_on_error", on_error_args=[svcname],
        )

    def service_stop_on_error(self, svcname):
        self.set_smon(svcname, "stop failed")
        self.update_hb_data()

    def service_stop_on_success(self, svcname):
        self.set_smon(svcname, status="idle", local_expect="unset")
        self.update_hb_data()

    def service_shutdown(self, svcname):
        self.set_smon(svcname, "shutdown")
        proc = self.service_command(svcname, ["shutdown"])
        self.push_proc(
            proc=proc,
            on_success="service_shutdown_on_success", on_success_args=[svcname],
            on_error="service_shutdown_on_error", on_error_args=[svcname],
        )

    def service_shutdown_on_error(self, svcname):
        self.set_smon(svcname, "idle")
        self.update_hb_data()

    def service_shutdown_on_success(self, svcname):
        self.set_smon(svcname, status="idle", local_expect="unset")
        self.update_hb_data()

    def service_freeze(self, svcname):
        self.set_smon(svcname, "freezing")
        proc = self.service_command(svcname, ["freeze"])
        self.push_proc(
            proc=proc,
            on_success="service_freeze_on_success", on_success_args=[svcname],
            on_error="service_freeze_on_error", on_error_args=[svcname],
        )

    def service_freeze_on_error(self, svcname):
        self.set_smon(svcname, "idle")
        self.update_hb_data()

    def service_freeze_on_success(self, svcname):
        self.set_smon(svcname, status="idle", local_expect="unset")
        self.update_hb_data()

    def service_thaw(self, svcname):
        self.set_smon(svcname, "thawing")
        proc = self.service_command(svcname, ["thaw"])
        self.push_proc(
            proc=proc,
            on_success="service_thaw_on_success", on_success_args=[svcname],
            on_error="service_thaw_on_error", on_error_args=[svcname],
        )

    def service_thaw_on_error(self, svcname):
        self.set_smon(svcname, "idle")
        self.update_hb_data()

    def service_thaw_on_success(self, svcname):
        self.set_smon(svcname, status="idle", local_expect="unset")
        self.update_hb_data()


    #########################################################################
    #
    # Orchestration
    #
    #########################################################################
    def orchestrator(self):
        self.node_orchestrator()
        with shared.SERVICES_LOCK:
            svcs = shared.SERVICES.values()
        for svc in svcs:
            self.service_orchestrator(svc)
            self.resources_orchestrator(svc)

    def resources_orchestrator(self, svc):
        if svc.frozen():
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
                return []
            if resource["disable"]:
                return []
            if smon.local_expect != "started":
                return []
            nb_restart = svc.get_resource(rid).nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return []
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcname, rid)
                self.log.info("max retries (%d) reached for resource %s.%s",
                              nb_restart, svc.svcname, rid)
                self.service_toc(svc.svcname)
                return []
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("restart resource %s.%s %d/%d", svc.svcname, rid,
                          retries+1, nb_restart)
            return [rid]

        def stdby_resource(svc, rid):
            resource = svc.get_resource(rid)
            if resource is None or rcEnv.nodename not in resource.always_on:
                return []
            nb_restart = resource.nb_restart
            if nb_restart == 0:
                nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return []
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcname, rid)
                self.log.info("max retries (%d) reached for stdby resource "
                              "%s.%s", nb_restart, svc.svcname, rid)
                return []
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("start stdby resource %s.%s %d/%d", svc.svcname, rid,
                          retries+1, nb_restart)
            return [rid]

        smon = self.get_service_monitor(svc.svcname)
        if smon.status != "idle":
            return

        rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("down", "stdby down"):
                continue
            rids += monitored_resource(svc, rid, resource)
            rids += stdby_resource(svc, rid)

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

    def service_orchestrator(self, svc):
        if svc.disabled:
            #self.log.info("service %s orchestrator out (disabled)", svc.svcname)
            return
        smon = self.get_service_monitor(svc.svcname)
        if smon.status not in ("ready", "idle"):
            #self.log.info("service %s orchestrator out (mon status %s)", svc.svcname, smon.status)
            return
        status = self.get_agg_avail(svc.svcname)
        self.set_smon_g_expect_from_status(svc.svcname, smon, status)
        if smon.global_expect:
            self.service_orchestrator_manual(svc, smon, status)
        else:
            self.service_orchestrator_auto(svc, smon, status)

    def service_orchestrator_auto(self, svc, smon, status):
        if svc.frozen():
            #self.log.info("service %s orchestrator out (frozen)", svc.svcname)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)",
            #              svc.svcname, status)
            return
        if svc.anti_affinity:
            intersection = set(self.get_local_svcnames()) & set(svc.anti_affinity)
            if len(intersection) > 0:
                #self.log.info("service %s orchestrator out (anti-affinity with %s)",
                #              svc.svcname, ','.join(intersection))
                return
        if svc.affinity:
            intersection = set(self.get_local_svcnames()) & set(svc.affinity)
            if len(intersection) < len(set(svc.affinity)):
                #self.log.info("service %s orchestrator out (affinity with %s)",
                #              svc.svcname, ','.join(intersection))
                return

        now = datetime.datetime.utcnow()
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        if svc.clustertype == "failover":
            if smon.status == "ready":
                if instance.avail is "up":
                    self.log.info("abort 'ready' because the local instance "
                                  "has started")
                    self.set_smon(svc.svcname, "idle")
                    return
                if status == "up":
                    self.log.info("abort 'ready' because an instance has started")
                    self.set_smon(svc.svcname, "idle")
                    return
                peer = self.better_peer_ready(svc)
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
                if smon.status_updated < (now - MON_WAIT_READY):
                    self.log.info("failover service %s status %s/ready for "
                                  "%s", svc.svcname, status,
                                  now-smon.status_updated)
                    self.service_start(svc.svcname)
                    return
                self.log.info("service %s will start in %s",
                              svc.svcname,
                              str(smon.status_updated+MON_WAIT_READY-now))
            elif smon.status == "idle":
                if status not in ("down", "stdby down", "stdby up"):
                    return
                if len(svc.peers) == 1:
                    self.log.info("failover service %s status %s/idle and "
                                  "single node", svc.svcname, status)
                    self.service_start(svc.svcname)
                    return
                if not self.failover_placement_leader(svc):
                    return
                self.log.info("failover service %s status %s", svc.svcname,
                              status)
                self.set_smon(svc.svcname, "ready")
        elif svc.clustertype == "flex":
            n_up = self.count_up_service_instances(svc.svcname)
            if smon.status == "ready":
                if (n_up - 1) >= svc.flex_min_nodes:
                    self.log.info("flex service %s instance count reached "
                                  "required minimum while we were ready",
                                  svc.svcname)
                    self.set_smon(svc.svcname, "idle")
                    return
                if smon.status_updated < (now - MON_WAIT_READY):
                    self.log.info("flex service %s status %s/ready for %s",
                                  svc.svcname, status, now-smon.status_updated)
                    self.service_start(svc.svcname)
                else:
                    self.log.info("service %s will start in %s", svc.svcname,
                                  str(smon.status_updated+MON_WAIT_READY-now))
            elif smon.status == "idle":
                if n_up >= svc.flex_min_nodes:
                    return
                if instance.avail not in STOPPED_STATES:
                    return
                if not self.failover_placement_leader(svc):
                    return
                self.log.info("flex service %s started, starting or ready to "
                              "start instances: %d/%d. local status %s",
                              svc.svcname, n_up, svc.flex_min_nodes,
                              instance.avail)
                self.set_smon(svc.svcname, "ready")

    def service_orchestrator_manual(self, svc, smon, status):
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        if smon.global_expect == "frozen":
            if not svc.frozen():
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
        elif smon.global_expect == "thawed":
            if svc.frozen():
                self.log.info("thaw service %s", svc.svcname)
                self.service_thaw(svc.svcname)
        elif smon.global_expect == "stopped":
            if not svc.frozen():
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
            if instance.avail not in STOPPED_STATES:
                thawed_on = self.service_instances_thawed(svc.svcname)
                if thawed_on:
                    self.log.info("service %s still has thawed instances on "
                                  "nodes %s, delay stop", svc.svcname,
                                  ", ".join(thawed_on))
                else:
                    self.service_stop(svc.svcname)
        elif smon.global_expect == "started":
            if svc.frozen():
                self.log.info("thaw service %s", svc.svcname)
                self.service_thaw(svc.svcname)
            elif status not in STARTED_STATES:
                self.service_orchestrator_auto(svc, smon, status)

    def failover_placement_leader(self, svc):
        nodenames = []
        with shared.CLUSTER_DATA_LOCK:
            for nodename, data in shared.CLUSTER_DATA.items():
                if data == "unknown":
                    continue
                instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
                if instance is None:
                    continue
                constraints = instance.get("constraints", True)
                if constraints:
                    nodenames.append(nodename)
        if len(nodenames) == 0:
            self.log.info("placement constraints prevent us from starting "
                          "service %s on any node", svc.svcname)
            return False
        if rcEnv.nodename not in nodenames:
            self.log.info("placement constraints prevent us from starting "
                          "service %s on this node", svc.svcname)
            return False
        if len(nodenames) == 1:
            self.log.info("we have the greatest placement priority for "
                          "service %s (alone)", svc.svcname)
            return True
        if svc.placement == "load avg":
            return self.failover_placement_leader_load_avg(svc)
        elif svc.placement == "nodes order":
            return self.failover_placement_leader_nodes_order(svc)
        else:
            # unkown, random ?
            return True

    def failover_placement_leader_load_avg(self, svc):
        top_load = None
        top_node = None
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                instance = self.get_service_instance(svc.svcname, nodename)
                if instance is None:
                    continue
                if instance.frozen:
                    continue
                try:
                    load = shared.CLUSTER_DATA[nodename]["load"]["15m"]
                except KeyError:
                    continue
                if top_load is None or load < top_load:
                    top_load = load
                    top_node = nodename
        if top_node is None:
            return False
        if top_node == rcEnv.nodename:
            self.log.info("we have the highest 'load avg' placement priority "
                          "for service %s", svc.svcname)
            return True
        self.log.info("node %s is alive and has a higher 'load avg' placement "
                      "priority for service %s (%s)", top_node, svc.svcname,
                      str(top_load))
        return False

    def failover_placement_leader_nodes_order(self, svc):
        with shared.CLUSTER_DATA_LOCK:
            for nodename in svc.ordered_nodes:
                if nodename == rcEnv.nodename:
                    self.log.info("we have the highest 'nodes order' placement"
                                  " priority for service %s", svc.svcname)
                    return True
                elif nodename in shared.CLUSTER_DATA and \
                     shared.CLUSTER_DATA[nodename] != "unknown" and \
                     not svc.frozen():
                    self.log.info("node %s is alive and has a higher 'nodes "
                                  "order' placement priority for service %s",
                                  nodename, svc.svcname)
                    return False

    def count_up_service_instances(self, svcname):
        n_up = 0
        for instance in self.get_service_instances(svcname).values():
            if instance["avail"] == "up":
                n_up += 1
            elif instance["monitor"]["status"] in ("restarting", "starting", "ready"):
                n_up += 1
        return n_up

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

    def better_peer_ready(self, svc):
        """
        Return the nodename of the first peer with the service in ready state, or
        None if we are placement leader or no peer is in ready state.
        """
        if self.failover_placement_leader(svc):
            return
        for nodename, instance in self.get_service_instances(svc.svcname).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"]["status"] == "ready":
                return nodename

    @staticmethod
    def get_service(svcname):
        with shared.SERVICES_LOCK:
            if svcname not in shared.SERVICES:
                return
        return shared.SERVICES[svcname]

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
        svc = self.get_service(svcname)
        if svc is None:
            return "unknown"
        if svc.clustertype == "failover":
            return self.get_agg_avail_failover(svc)
        elif svc.clustertype == "flex":
            return self.get_agg_avail_flex(svc)
        else:
            return "unknown"

    def get_agg_overall(self, svcname):
        for instance in self.get_service_instances(svcname).values():
            if instance["overall"] == "warn":
                return "warn"
        return ""

    def get_agg_frozen(self, svcname):
        fstatus = "undef"
        fstatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcname).values():
            fstatus_l.append(instance["frozen"])
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

    def get_agg_avail_failover(self, svc):
        astatus = 'undef'
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svc.svcname).values():
            astatus_l.append(instance["avail"])
            n_instances += 1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_instances == 0:
            astatus = 'n/a'
        elif astatus_s == set(['n/a']):
            astatus = 'n/a'
        elif 'warn' in astatus_l:
            astatus = 'warn'
        elif n_up > 1:
            astatus = 'warn'
        elif n_up == 1:
            astatus = 'up'
        else:
            astatus = 'down'
        return astatus

    def get_agg_avail_flex(self, svc):
        astatus = 'undef'
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svc.svcname).values():
            astatus_l.append(instance["avail"])
            n_instances += 1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_instances == 0:
            astatus = 'n/a'
        elif astatus_s == set(['n/a']):
            astatus = 'n/a'
        elif n_up == 0:
            astatus = 'down'
        elif 'warn' in astatus_l:
            astatus = 'warn'
        elif n_up > svc.flex_max_nodes:
            astatus = 'warn'
        elif n_up < svc.flex_min_nodes:
            astatus = 'warn'
        else:
            astatus = 'up'
        return astatus

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
                        if svcname not in data:
                            data[svcname] = {}
                        data[svcname][nodename] = Storage(shared.CLUSTER_DATA[nodename]["services"]["config"][svcname])
                except KeyError:
                    pass
        return data

    @staticmethod
    def get_service_instance(svcname, nodename):
        """
        Return the specified service status structure on the specified node.
        """
        try:
            with shared.CLUSTER_DATA_LOCK:
                return Storage(shared.CLUSTER_DATA[nodename]["services"]["status"][svcname])
        except KeyError:
            return

    @staticmethod
    def get_service_instances(svcname):
        """
        Return the specified service status structures on all nodes.
        """
        instances = {}
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                try:
                    if svcname in shared.CLUSTER_DATA[nodename]["services"]["status"]:
                        instances[nodename] = shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]
                except KeyError:
                    continue
        return instances

    @staticmethod
    def fsum(fpath):
        """
        Return a file content checksum
        """
        with codecs.open(fpath, "r", "utf-8") as filep:
            buff = filep.read()
        cksum = hashlib.md5(buff.encode("utf-8"))
        return cksum.hexdigest()

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
                cksum = self.fsum(cfg)
                try:
                    with shared.SERVICES_LOCK:
                        shared.SERVICES[svcname] = build(svcname, node=shared.NODE)
                except Exception as exc:
                    self.log.error("%s build error: %s", svcname, str(exc))
                    continue
            else:
                cksum = last_config["cksum"]
            with shared.SERVICES_LOCK:
                scope = sorted(list(shared.SERVICES[svcname].nodes | shared.SERVICES[svcname].drpnodes))
            config[svcname] = {
                "updated": mtime.strftime(shared.DATEFMT),
                "cksum": cksum,
                "scope": scope,
            }
        return config

    def get_last_svc_status_mtime(self, svcname):
        """
        Return the mtime of the specified service configuration file on the
        local node. If unknown, return 0.
        """
        instance = self.get_service_instance(svcname, rcEnv.nodename)
        if instance is None:
            return 0
        return instance["mtime"]

    def service_status_fallback(self, svcname):
        """
        Return the specified service status structure fetched from an execution
        of svcmgr -s <svcname> json status". As we arrive here when the
        status.json doesn't exist, we don't have to specify --refresh.
        """
        self.log.info("slow path service status eval: %s", svcname)
        cmd = [rcEnv.paths.svcmgr, "-s", svcname, "json", "status"]
        try:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            out, _ = proc.communicate()
        except KeyboardInterrupt:
            return
        try:
            return json.loads(bdecode(out))
        except ValueError:
            return

    def get_services_status(self, svcnames):
        """
        Return the local services status data, fetching data from status.json
        caches if their mtime changed or from CLUSTER_DATA[rcEnv.nodename] if
        not.

        Also update the monitor 'local_expect' field for each service.
        """
        with shared.CLUSTER_DATA_LOCK:
            try:
                data = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"]
            except KeyError:
                data = {}
        for svcname in svcnames:
            fpath = os.path.join(rcEnv.paths.pathvar, svcname, "status.json")
            try:
                mtime = os.path.getmtime(fpath)
            except Exception:
                # force service status refresh
                mtime = time.time() + 1
            last_mtime = self.get_last_svc_status_mtime(svcname)
            if svcname not in data or mtime > last_mtime and svcname in data:
                #self.log.info("service %s status changed", svcname)
                try:
                    with open(fpath, 'r') as filep:
                        try:
                            data[svcname] = json.load(filep)
                        except ValueError:
                            data[svcname] = self.service_status_fallback(svcname)
                except Exception:
                    data[svcname] = self.service_status_fallback(svcname)
            if not data[svcname]:
                del data[svcname]
                continue
            with shared.SERVICES_LOCK:
                data[svcname]["frozen"] = shared.SERVICES[svcname].frozen()
            self.set_smon_l_expect_from_status(data, svcname)
            data[svcname]["monitor"] = self.get_service_monitor(svcname,
                                                                datestr=True)

        # purge deleted services
        for svcname in set(data.keys()) - set(svcnames):
            del data[svcname]

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

    def set_nmon_g_expect_from_status(self):
        nmon = self.get_node_monitor()
        if nmon.global_expect is None:
            return
        local_frozen = self.freezer.node_frozen()
        if nmon.global_expect == "frozen" and local_frozen:
            self.log.info("node global expect is %s and is frozen",
                          nmon.global_expect)
            self.set_nmon(global_expect="unset")
        elif nmon.global_expect == "thawed" and not local_frozen:
            self.log.info("node global expect is %s and is thawed",
                          nmon.global_expect)
            self.set_nmon(global_expect="unset")

    def set_smon_g_expect_from_status(self, svcname, smon, status):
        if smon.global_expect is None:
            return
        local_frozen = self.get_service_instance(svcname, rcEnv.nodename)["frozen"]
        if smon.global_expect == "stopped" and status in STOPPED_STATES and \
           local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcname, smon.global_expect, status)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "started" and \
             status in STARTED_STATES and not local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcname, smon.global_expect, status)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "frozen" and local_frozen:
            self.log.info("service %s global expect is %s and is frozen",
                          svcname, smon.global_expect)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "thawed" and not local_frozen:
            self.log.info("service %s global expect is %s and is thawed",
                          svcname, smon.global_expect)
            self.set_smon(svcname, global_expect="unset")

    def set_smon_l_expect_from_status(self, data, svcname):
        if svcname not in data:
            return
        with shared.SMON_DATA_LOCK:
            if svcname not in shared.SMON_DATA:
                return
            if data[svcname]["avail"] == "up" and \
               shared.SMON_DATA[svcname].local_expect != "started":
                self.log.info("service %s monitor local_expect change "
                              "%s => %s", svcname,
                              shared.SMON_DATA[svcname].local_expect, "started")
                shared.SMON_DATA[svcname].local_expect = "started"

    def update_hb_data(self):
        """
        Update the heartbeat payload we send to other nodes.
        Crypt it so the tx threads don't have to do it on their own.
        """
        #self.log.info("update heartbeat data to send")
        load_avg = os.getloadavg()
        config = self.get_services_config()
        status = self.get_services_status(config.keys())

        try:
            with shared.CLUSTER_DATA_LOCK:
                shared.CLUSTER_DATA[rcEnv.nodename] = {
                    "frozen": self.freezer.node_frozen(),
                    "env": rcEnv.node_env,
                    "monitor": self.get_node_monitor(datestr=True),
                    "updated": datetime.datetime.utcfromtimestamp(time.time())\
                                                .strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "services": {
                        "config": config,
                        "status": status,
                    },
                    "load": {
                        "1m": load_avg[0],
                        "5m": load_avg[1],
                        "15m": load_avg[2],
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
            for nodename in shared.CLUSTER_DATA:
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
            for svcname in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                for nodename in nodenames:
                    if nodename not in shared.CLUSTER_DATA:
                        continue
                    if svcname not in shared.CLUSTER_DATA[nodename]["services"]["status"]:
                        continue
                    global_expect = shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]["monitor"].get("global_expect")
                    if global_expect is None:
                        continue
                    local_avail = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["avail"]
                    local_frozen = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["frozen"]
                    status = self.get_agg_avail(svcname)
                    if (global_expect == "stopped" and (local_avail not in STOPPED_STATES or not local_frozen)) or \
                       (global_expect == "started" and (status not in STARTED_STATES or local_frozen)) or \
                       (global_expect == "frozen" and not local_frozen) or \
                       (global_expect == "thawed" and local_frozen):
                        self.log.info("node %s wants service %s %s", nodename, svcname, global_expect)
                        self.set_smon(svcname, global_expect=global_expect)
                    else:
                        self.log.info("node %s wants service %s %s, already is", nodename, svcname, global_expect)

    def status(self):
        self.update_hb_data()
        data = shared.OsvcThread.status(self)
        with shared.CLUSTER_DATA_LOCK:
            data.nodes = dict(shared.CLUSTER_DATA)
        data["services"] = {}
        data["frozen"] = self.get_clu_agg_frozen()
        for svcname in data.nodes[rcEnv.nodename]["services"]["config"]:
            if svcname not in data["services"]:
                data["services"][svcname] = Storage()
            data["services"][svcname].avail = self.get_agg_avail(svcname)
            data["services"][svcname].frozen = self.get_agg_frozen(svcname)
            data["services"][svcname].overall = self.get_agg_overall(svcname)
        return data
