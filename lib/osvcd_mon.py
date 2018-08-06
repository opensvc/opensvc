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
import re
import threading
from subprocess import Popen, PIPE
from distutils.version import LooseVersion

import osvcd_shared as shared
import rcExceptions as ex
import json_delta
from comm import Crypt
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import bdecode, purge_cache, fsum
from svcBuilder import build, fix_exe_link
from freezer import Freezer
from jsonpath_ng import jsonpath, parse

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
    monitor_period = 0.5
    arbitrators_check_period = 60
    max_shortloops = 30
    default_stdby_nb_restart = 2
    arbitrators_data = None

    def __init__(self):
        shared.OsvcThread.__init__(self)
        self._shutdown = False
        self.compat = True
        self.last_node_data = None
        self.status_threads = {}

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.log.info("monitor started")
        self.startup = datetime.datetime.utcnow()
        self.rejoin_grace_period_expired = False
        self.shortloops = 0
        self.unfreeze_when_all_nodes_joined = False

        shared.CLUSTER_DATA[rcEnv.nodename] = {
            "compat": shared.COMPAT_VERSION,
            "agent": shared.NODE.agent_version,
            "monitor": shared.NMON_DATA,
            "services": {},
        }

        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            if not self.freezer.node_frozen():
                self.log.info("freeze node until the cluster is complete")
                self.unfreeze_when_all_nodes_joined = True
                self.freezer.node_freeze()

        # send a first message without service status, so the peers know
        # we are in init state.
        self.update_hb_data()

        try:
            while True:
                self.do()
                if self.stopped():
                    self.join_status_threads()
                    self.join_threads()
                    self.kill_procs()
                    sys.exit(0)
        except Exception as exc:
            self.log.exception(exc)

    def transition_count(self):
        count = 0
        for svcname, data in shared.SMON_DATA.items():
            if data.status and data.status != "scaling" and data.status.endswith("ing"):
                count += 1
        return count

    def join_status_threads(self):
        """
        Initial service status eval is done is thread.
        Join those still not terminated.
        """
        for thr in self.status_threads.values():
            thr.join()

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

    def reconfigure(self):
        """
        The node config references may have changed, update the services objects.
        """
        with shared.SERVICES_LOCK:
            for svcname in shared.SERVICES:
                 try:
                     shared.SERVICES[svcname] = build(svcname, node=shared.NODE)
                 except Exception as exc:
                     continue

    def do(self):
        terminated_procs = self.janitor_procs()
        self.janitor_threads()
        if terminated_procs == 0 and not self.mon_changed() and self.shortloops < self.max_shortloops:
            self.shortloops += 1
            if not self.mon_changed():
                with shared.MON_TICKER:
                    shared.MON_TICKER.wait(self.monitor_period)
            return
        if self.mon_changed():
            with shared.MON_TICKER:
                self.log.debug("woken for:")
                for idx, reason in enumerate(shared.MON_CHANGED):
                    self.log.debug("%d. %s", idx, reason)
                self.unset_mon_changed()
        self.shortloops = 0
        self.reload_config()
        self.merge_frozen()
        if self._shutdown:
            if len(self.procs) == 0:
                self.stop()
        else:
            self.update_cluster_data()
            self.merge_hb_data()
            self.orchestrator()
        self.update_hb_data()
        shared.wake_collector()

    def shutdown(self):
        with shared.SERVICES_LOCK:
            for svc in shared.SERVICES.values():
                self.service_shutdown(svc.svcname)
        self._shutdown = True
        shared.wake_monitor("service %s shutdown terminated" % svc.svcname)

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
            instances = self.get_service_instances(svcname, rcEnv.nodename)
            for instance in instances:
                # block config file exchange while any instance is being deleted
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
            if new_service:
                Freezer(svcname).freeze()
            self.fetch_service_config(svcname, ref_nodename)
            if new_service:
                fix_exe_link(svcname)

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
        if resp is None:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", svcname, nodename, resp)
            return
        status = resp.get("status", 1)
        if status == 2:
            # peer is deleting this service
            self.log.info(resp.get("error", ""))
            return
        elif status != 0:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", svcname, nodename, resp)
            return
        with tempfile.NamedTemporaryFile(dir=rcEnv.paths.pathtmp, delete=False) as filep:
            tmpfpath = filep.name
        try:
            with codecs.open(tmpfpath, "w", "utf-8") as filep:
                filep.write(resp["data"])
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
                mtime = resp.get("mtime")
                if mtime:
                    os.utime(dst, (mtime, mtime))
            else:
                self.log.error("the service %s config fetched from node %s is "
                               "not valid", svcname, nodename)
                return
        finally:
            os.unlink(tmpfpath)
        try:
            shared.SERVICES[svcname] = build(svcname, node=shared.NODE)
        except Exception as exc:
            self.log.error("unbuildable service %s fetched: %s", svcname, exc)
            return

        try:
            shared.SERVICES[svcname].purge_status_data_dump()
            shared.SERVICES[svcname].print_status_data_eval()
        except Exception:
            # can happen when deleting the service
            pass

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

    def service_start_resources(self, svcname, rids, slave=None):
        self.set_smon(svcname, "restarting")
        cmd = ["start", "--rid", ",".join(rids)]
        if slave:
            cmd += ["--slave", slave]
        proc = self.service_command(svcname, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[svcname, rids],
            on_success_kwargs={"slave": slave},
            on_error="generic_callback",
            on_error_args=[svcname],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources_on_success(self, svcname, rids, slave=None):
        self.set_smon(svcname, status="idle")
        self.update_hb_data()
        changed = False
        for rid in rids:
            instance = self.get_service_instance(svcname, rcEnv.nodename)
            if instance is None:
                self.reset_smon_retries(svcname, rid)
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
            self.reset_smon_retries(svcname, rid)
        if changed:
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
        proc = self.service_command(svcname, ["delete", "--purge-collector"])
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
        proc = self.service_command(svcname, ["delete", "--purge-collector"])
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
                                               discard_overloaded=False,
                                               discard_unprovisioned=False,
                                               discard_constraints_violation=False)
        cmd = ["provision"]
        if self.placement_leader(svc, candidates):
            cmd += ["--disable-rollback"]
        proc = self.service_command(svc.svcname, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_thaw",
            on_success_args=[svc.svcname],
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

    def wait_global_expect_change(self, svcname, ref, timeout):
        for step in range(timeout):
            global_expect = shared.SMON_DATA.get(svcname, {}).get("global_expect")
            if global_expect != ref:
                return True
            time.sleep(1)
        return False

    def service_set_flex_instances(self, svcname, instances):
        cmd = [
            "set",
            "--kw", "flex_min_nodes=%d" % instances,
            "--kw", "flex_max_nodes=%d" % instances,
        ]
        proc = self.service_command(svcname, cmd)
        out, err = proc.communicate()
        return proc.returncode

    def service_create_scaler_slave(self, svcname, svc, data, instances=None):
        data["DEFAULT"]["scaler_slave"] = "true"
        if svc.topology == "flex" and instances is not None:
            data["DEFAULT"]["flex_min_nodes"] = instances
            data["DEFAULT"]["flex_max_nodes"] = instances
        for kw in ("scale", "id"):
            try:
                del data["DEFAULT"][kw]
            except KeyError:
                pass
        cmd = ["create"]
        proc = self.service_command(svcname, cmd, stdin=json.dumps(data))
        out, err = proc.communicate()
        if proc.returncode != 0:
            self.set_smon(svcname, "create failed")

        try:
            ret = self.wait_service_config_consensus(svcname, svc.peers)
        except Exception as exc:
            self.log.exception(exc)
            return

        self.service_status_fallback(svcname)
        self.set_smon(svcname, global_expect="thawed")
        self.wait_global_expect_change(svcname, "thawed", 600)

        self.set_smon(svcname, global_expect="provisioned")
        self.wait_global_expect_change(svcname, "provisioned", 600)

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
        if shared.NMON_DATA.status == "init":
            return

        # node
        self.node_orchestrator()

        # services (iterate over deleting services too)
        svcnames = [svcname for svcname in shared.SMON_DATA]
        self.get_agg_services()
        for svcname in svcnames:
            transitions = self.transition_count()
            if transitions > shared.NODE.max_parallel:
                self.duplog("info", "delay services orchestration: "
                            "%(transitions)d/%(max)d transitions already "
                            "in progress", transitions=transitions,
                            max=shared.NODE.max_parallel)
                break
            if self.status_older_than_cf(svcname):
                #self.log.info("%s status dump is older than its config file",
                #              svcname)
                continue
            svc = self.get_service(svcname)
            self.resources_orchestrator(svcname, svc)
            self.service_orchestrator(svcname, svc)
        self.sync_services_conf()

    def resources_orchestrator(self, svcname, svc):
        if svc is None:
            return
        if self.service_frozen(svcname) or self.freezer.node_frozen():
            #self.log.info("resource %s orchestrator out (frozen)", svc.svcname)
            return
        if svc.disabled:
            #self.log.info("resource %s orchestrator out (disabled)", svc.svcname)
            return

        def monitored_resource(svc, rid, resource):
            if resource.get("disable"):
                return False
            if smon.local_expect != "started":
                return False
            nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            if nb_restart == 0:
                if resource.get("standby"):
                    nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                if nb_restart > 0:
                    self.log.info("service %s max retries (%d) reached for "
                                  "resource %s (%s)", svc.svcname, nb_restart,
                                  rid, resource["label"])
                    svc.log.info("max retries (%d) reached for resource %s "
                                 "(%s)", nb_restart, rid, resource["label"])
                self.inc_smon_retries(svc.svcname, rid)
                if resource.get("monitor"):
                    candidates = self.placement_candidates(svc)
                    log = " ".join(resource.get("log", []))
                    if not log:
                        log = "no log"
                    if candidates != [rcEnv.nodename] and len(candidates) > 0:
                        self.log.info("toc for service %s rid %s (%s) %s (%s)",
                                      svc.svcname, rid, resource["label"],
                                      resource["status"], log)
                        svc.log.info("toc for rid %s (%s) %s (%s)", rid,
                                      resource["label"], resource["status"],
                                      log)
                        self.service_toc(svc.svcname)
                    else:
                        self.log.info("would toc for service %s rid %s (%s) %s (%s), but "
                                      "no node is candidate for takeover.",
                                      svc.svcname, rid, resource["label"],
                                      resource["status"], log)
                        svc.log.info("would toc for rid %s (%s) %s (%s), but "
                                     "no node is candidate for takeover.",
                                     rid, resource["label"],
                                     resource["status"], log)
                else:
                    self.log.info("service %s unmonitored rid %s (%s) went %s",
                                  svc.svcname, rid, resource["label"],
                                  resource["status"])
                    svc.log.info("unmonitored rid %s (%s) went %s",
                                 rid, resource["label"], resource["status"])
                return False
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("service %s restart resource %s (%s), try %d/%d",
                          svc.svcname, rid, resource["label"],
                          retries+1, nb_restart)
            svc.log.info("restart resource %s (%s), try %d/%d", rid,
                         resource["label"], retries+1, nb_restart)
            return True

        def stdby_resource(svc, rid, resource):
            if resource.get("standby") is not True:
                return False
            nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            if nb_restart < self.default_stdby_nb_restart:
                nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcname, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcname, rid)
                self.log.info("service %s max retries (%d) reached for standby "
                              "resource %s (%s)", svc.svcname, nb_restart, rid,
                              resource["label"])
                svc.log.info("max retries (%d) reached for standby resource "
                             "%s (%s)", nb_restart, rid, resource["label"])
                return False
            self.inc_smon_retries(svc.svcname, rid)
            self.log.info("service %s start standby resource %s (%s), try "
                          "%d/%d", svc.svcname, rid, resource["label"],
                          retries+1, nb_restart)
            svc.log.info("start standby resource %s (%s), try %d/%d", rid,
                         resource["label"], retries+1, nb_restart)
            return True

        smon = self.get_service_monitor(svc.svcname)
        if smon.status != "idle":
            return

        try:
            with shared.CLUSTER_DATA_LOCK:
                instance = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]
                if instance.get("encap") is True:
                    return
                resources = instance["resources"]
        except KeyError:
            return

        rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("warn", "down", "stdby down"):
                self.reset_smon_retries(svc.svcname, rid)
                continue
            if not resource.get("provisioned", {}).get("state"):
                continue
            if monitored_resource(svc, rid, resource) or stdby_resource(svc, rid, resource):
                rids.append(rid)
                continue

        if len(rids) > 0:
            self.service_start_resources(svc.svcname, rids)

        # same for encap resources
        rids = []
        for crid, cdata in instance.get("encap", {}).items():
            if cdata.get("frozen"):
                continue
            resources = cdata.get("resources", [])
            rids = []
            for rid, resource in resources.items():
                if resource["status"] not in ("warn", "down", "stdby down"):
                    self.reset_smon_retries(svc.svcname, rid)
                    continue
                if resource.get("provisioned", {}).get("state") is False:
                    continue
                if monitored_resource(svc, rid, resource) or stdby_resource(svc, rid, resource):
                    rids.append(rid)
                    continue
            if len(rids) > 0:
                self.service_start_resources(svc.svcname, rids, slave=crid)

    def node_orchestrator(self):
        self.orchestrator_auto_grace()
        nmon = self.get_node_monitor()
        node_frozen = self.freezer.node_frozen()
        if self.unfreeze_when_all_nodes_joined and node_frozen and len(self.cluster_nodes) == len(shared.CLUSTER_DATA):
            self.log.info("thaw node now the cluster is complete")
            self.freezer.node_thaw()
            self.unfreeze_when_all_nodes_joined = False
            node_frozen = False
        if nmon.status != "idle":
            return
        self.set_nmon_g_expect_from_status()
        if nmon.global_expect == "frozen":
            self.unfreeze_when_all_nodes_joined = False
            if not node_frozen:
                self.log.info("freeze node")
                self.freezer.node_freeze()
        elif nmon.global_expect == "thawed":
            self.unfreeze_when_all_nodes_joined = False
            if node_frozen:
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

    @staticmethod
    def scale_svcname(svcname, idx):
        return str(idx)+"."+svcname

    def service_orchestrator_auto(self, svc, smon, status):
        """
        Automatic instance start decision.
        Verifies hard and soft affinity and anti-affinity, then routes to
        failover and flex specific policies.
        """
        if svc.disabled:
            #self.log.info("service %s orchestrator out (disabled)", svc.svcname)
            return
        if not self.compat:
            return
        if svc.topology == "failover" and smon.local_expect == "started":
            # decide if the service local_expect=started should be reset
            if status == "up" and self.get_service_instance(svc.svcname, rcEnv.nodename).avail != "up":
                self.log.info("service '%s' is globally up but the local instance is "
                              "not and is in 'started' local expect. reset",
                              svc.svcname)
                self.set_smon(svc.svcname, local_expect="unset")
            elif self.service_started_instances_count(svc.svcname) > 1 and \
                 self.get_service_instance(svc.svcname, rcEnv.nodename).avail != "up" and \
                 not self.placement_leader(svc):
                self.log.info("service '%s' has multiple instance in 'started' "
                              "local expect and we are not leader. reset",
                              svc.svcname)
                self.set_smon(svc.svcname, local_expect="unset")
            elif status != "up" and \
                 self.get_service_instance(svc.svcname, rcEnv.nodename).avail in ("down", "stdby down", "undef", "n/a") and \
                 not self.resources_orchestrator_will_handle(svc):
                self.log.info("service '%s' is not up and no resource monitor "
                              "action will be attempted, but "
                              "is in 'started' local expect. reset",
                              svc.svcname)
                self.set_smon(svc.svcname, local_expect="unset")
            else:
                return
        if self.service_frozen(svc.svcname) or self.freezer.node_frozen():
            #self.log.info("service %s orchestrator out (frozen)", svc.svcname)
            return
        if not self.rejoin_grace_period_expired:
            return
        if svc.scale_target is not None and smon.global_expect is None:
            self.service_orchestrator_scaler(svc)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)",
            #              svc.svcname, status)
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
            peer = self.peer_transitioning(svc.svcname)
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
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.log.info("failover service %s status %s/ready for "
                              "%d seconds", svc.svcname, status,
                              now-smon.status_updated)
                if smon.stonith and smon.stonith not in shared.CLUSTER_DATA:
                    # stale peer which previously ran the service
                    self.node_stonith(smon.stonith)
                self.service_start(svc.svcname)
                return
            tmo = int(smon.status_updated + self.ready_period - now) + 1
            self.log.info("service %s will start in %d seconds",
                          svc.svcname, tmo)
            self.set_next(tmo)
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
            peer = self.peer_transitioning(svc.svcname)
            if peer:
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
        n_missing = svc.flex_min_nodes - n_up

        if smon.status in ("ready", "wait parents"):
            if n_up > svc.flex_min_nodes:
                self.log.info("flex service %s instance count reached "
                              "required minimum while we were ready",
                              svc.svcname)
                self.set_smon(svc.svcname, "idle")
                return
            better_peers = self.better_peers_ready(svc);
            if n_missing > 0 and len(better_peers) >= n_missing:
                self.log.info("abort 'ready' because nodes %s have a better "
                              "placement score for service %s and are also "
                              "ready", ','.join(better_peers), svc.svcname)
                self.set_smon(svc.svcname, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.svcname, status="idle")
                return
        if smon.status == "ready":
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.log.info("flex service %s status %s/ready for %d seconds",
                              svc.svcname, status, now-smon.status_updated)
                self.service_start(svc.svcname)
            else:
                tmo = int(smon.status_updated + self.ready_period - now) + 1
                self.log.info("service %s will start in %d seconds",
                              svc.svcname, tmo)
                self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect != "started":
                return
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
                if instance is None:
                    return
                if instance.avail not in STARTED_STATES:
                    return
                n_to_stop = n_up - svc.flex_max_nodes
                overloaded_up_nodes = self.overloaded_up_service_instances(svc.svcname)
                to_stop = self.placement_ranks(svc, candidates=overloaded_up_nodes)[-n_to_stop:]
                n_to_stop -= len(to_stop)
                if n_to_stop > 0:
                    to_stop += self.placement_ranks(svc, candidates=set(up_nodes)-set(overloaded_up_nodes))[-n_to_stop:]
                self.log.info("%d nodes to stop to honor service %s "
                              "flex_max_nodes=%d. choose %s",
                              n_to_stop, svc.svcname, svc.flex_max_nodes,
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
            if self.service_frozen(svc.svcname) is False:
                self.log.info("freeze service %s", svc.svcname)
                self.service_freeze(svc.svcname)
        elif smon.global_expect == "thawed":
            if self.service_frozen(svc.svcname):
                self.log.info("thaw service %s", svc.svcname)
                self.service_thaw(svc.svcname)
        elif smon.global_expect == "shutdown":
            if not self.children_down(svc):
                self.set_smon(svc.svcname, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcname, status="idle")

            if not self.service_frozen(svc.svcname):
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

            if not self.service_frozen(svc.svcname):
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
            if self.service_frozen(svc.svcname):
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
             self.leader_first(svc, provisioned=False, deleted=True, check_min_instances_reached=False):
            if svc.svcname in shared.SERVICES and \
               (not self.service_unprovisioned(instance) or instance is not None):
                self.service_purge(svc.svcname)
        elif smon.global_expect == "aborted" and \
             smon.local_expect not in (None, "started"):
            self.set_smon(svc.svcname, local_expect="unset")
        elif smon.global_expect == "placed":
            if instance["monitor"].get("placement") != "leader":
                if instance.avail not in STOPPED_STATES:
                    self.service_stop(svc.svcname)
            elif self.non_leaders_stopped(svc) and \
                 (shared.AGG[svc.svcname].placement not in ("optimal", "n/a") or shared.AGG[svc.svcname].avail != "up") and \
                 instance.avail in STOPPED_STATES:
                self.service_start(svc.svcname)

    def scaler_current_slaves(self, svcname):
        return [slave for slave in shared.SERVICES \
                if re.match("^[0-9]+\."+svcname+"$", slave)]

    def service_orchestrator_scaler(self, svc):
        smon = self.get_service_monitor(svc.svcname)
        if smon.status != "idle":
            return
        peer = self.peer_transitioning(svc.svcname)
        if peer:
            return
        candidates = self.placement_candidates(
            svc, discard_frozen=False,
            discard_overloaded=False,
            discard_unprovisioned=False,
            discard_constraints_violation=False,
            discard_start_failed=False
        )
        ranks = self.placement_ranks(svc, candidates)
        if not ranks:
            return
        if ranks[0] != rcEnv.nodename:
            # not natural leader
            return
        current_slaves = self.scaler_current_slaves(svc.svcname)
        n_slots = self.scaler_slots(current_slaves)
        if n_slots == svc.scale_target:
            return
        missing = svc.scale_target - n_slots
        self.log.info("service %s scale delta %d on target %d", svc.svcname,
                      missing, svc.scale_target)
        if missing > 0:
            self.service_orchestrator_scaler_up(svc, missing, current_slaves)
        else:
            self.service_orchestrator_scaler_down(svc, missing, current_slaves)

    def service_orchestrator_scaler_up(self, svc, missing, current_slaves):
        if svc.topology == "flex":
            self.service_orchestrator_scaler_up_flex(svc, missing, current_slaves)
        else:
            self.service_orchestrator_scaler_up_failover(svc, missing, current_slaves)

    def service_orchestrator_scaler_down(self, svc, missing, current_slaves):
        if svc.topology == "flex":
            self.service_orchestrator_scaler_down_flex(svc, missing, current_slaves)
        else:
            self.service_orchestrator_scaler_down_failover(svc, missing, current_slaves)

    def service_orchestrator_scaler_up_flex(self, svc, missing, current_slaves):
        candidates = self.placement_candidates(svc, discard_preserved=False)
        width = len(candidates)
        if width == 0:
            return

        # start fill-up the current slaves that might have holes due to
        # previous scaling while some nodes where overloaded
        n_current_slaves = len(current_slaves)
        current_slaves = sorted(current_slaves, key=LooseVersion)
        for slavename in current_slaves:
            slave = shared.SERVICES[slavename]
            if slave.flex_max_nodes >= width:
                continue
            remain = width - slave.flex_max_nodes
            if remain > missing:
                pad = remain - missing
                new_width = slave.flex_max_nodes + pad
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
        for slavename in [str(idx)+"."+svc.svcname for idx in range(n_current_slaves)]:
            if slavename in current_slaves:
                continue
            to_add.append([slavename, width])
            slaves_count -= 1
            if slaves_count == 0:
                break

        to_add += [[str(n_current_slaves+idx)+"."+svc.svcname, width] for idx in range(slaves_count)]
        if left != 0 and len(to_add):
            to_add[-1][1] = left
        to_add = to_add[:max_burst]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale service %s: %s", svc.svcname, delta)
        self.set_smon(svc.svcname, status="scaling")
        thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
        thr.start()
        self.threads.append(thr)

    def service_orchestrator_scaler_down_flex(self, svc, missing, current_slaves):
        to_remove = []
        excess = -missing
        for slavename in sorted(current_slaves, key=LooseVersion, reverse=True):
            slave = shared.SERVICES[slavename]
            n_slots = slave.flex_min_nodes
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
        self.log.info("scale service %s: %s", svc.svcname, delta)
        self.set_smon(svc.svcname, status="scaling")
        thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
        thr.start()
        self.threads.append(thr)

    def service_orchestrator_scaler_up_failover(self, svc, missing, current_slaves):
        slaves_count = missing
        n_current_slaves = len(current_slaves)
        new_slaves_list = [str(n_current_slaves+idx)+"."+svc.svcname for idx in range(slaves_count)]

        to_add = sorted(new_slaves_list, key=LooseVersion)
        to_add = [[svcname, None] for svcname in to_add]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale service %s: %s", svc.svcname, delta)
        self.set_smon(svc.svcname, status="scaling")
        thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
        thr.start()
        self.threads.append(thr)

    def service_orchestrator_scaler_down_failover(self, svc, missing, current_slaves):
        slaves_count = -missing
        n_current_slaves = len(current_slaves)
        slaves_list = [str(n_current_slaves-1-idx)+"."+svc.svcname for idx in range(slaves_count)]

        to_remove = sorted(slaves_list, key=LooseVersion)
        to_remove = [svcname for svcname in to_remove]
        delta = "delete " + ",".join([elem[0] for elem in to_remove])
        self.log.info("scale service %s: %s", svc.svcname, delta)
        self.set_smon(svc.svcname, status="scaling")
        thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
        thr.start()
        self.threads.append(thr)

    def scaling_worker(self, svc, to_add, to_remove):
        threads = []
        for svcname, instances in to_add:
            if svcname in shared.SERVICES:
                continue
            data = svc.print_config_data()
            thr = threading.Thread(
                target=self.service_create_scaler_slave,
                args=(svcname, svc, data, instances)
            )
            thr.start()
            threads.append(thr)
        for svcname in to_remove:
            if svcname not in shared.SERVICES:
                continue
            self.set_smon(svcname, global_expect="purged")
        for svcname in to_remove:
            self.wait_global_expect_change(svcname, "purged", 300)
        while True:
            for thr in threads:
                thr.join(0)
            if any(thr.is_alive() for thr in threads):
                time.sleep(1)
                if self.stopped():
                    break
                continue
            break
        self.set_smon(svc.svcname, global_expect="unset", status="idle")

    def end_rejoin_grace_period(self, reason=""):
        self.rejoin_grace_period_expired = True
        self.duplog("info", "end of rejoin grace period: %s" % reason,
                    nodename="")
        nmon = self.get_node_monitor()
        if nmon.status == "rejoin":
            self.set_nmon(status="idle")

    def orchestrator_auto_grace(self):
        """
        After daemon startup, wait for <rejoin_grace_period_expired> seconds
        before allowing service_orchestrator_auto() to proceed.
        """
        if self.rejoin_grace_period_expired:
            return False
        if len(self.cluster_nodes) == 1:
            self.end_rejoin_grace_period("single node cluster")
            return False
        n_idle = len([1 for node in shared.CLUSTER_DATA.values() if node.get("monitor", {}).get("status") in ("idle", "rejoin")])
        if n_idle >= len(self.cluster_nodes):
            self.end_rejoin_grace_period("now rejoined")
            return False
        now = datetime.datetime.utcnow()
        if now > self.startup + datetime.timedelta(seconds=self.rejoin_grace_period):
            self.end_rejoin_grace_period("expired, but some nodes are still "
                                         "unreacheable. freeze node.")
            self.freezer.node_freeze()
            return False
        self.duplog("info", "in rejoin grace period", nodename="")
        return True

    def children_down(self, svc):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
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

    def non_leaders_stopped(self, svc):
        for nodename, instance in self.get_service_instances(svc.svcname).items():
            if instance["monitor"].get("placement") == "leader":
                continue
            if instance.get("avail") not in STOPPED_STATES:
                self.log.info("service '%s' instance node '%s' is not stopped yet",
                              svc.svcname, nodename)
                return False
        return True

    def leader_first(self, svc, provisioned=False, deleted=None, check_min_instances_reached=True):
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
        if check_min_instances_reached and not self.min_instances_reached(svc):
            self.log.info("delay leader-first action on service %s until all "
                          "nodes have fetched the service config", svc.svcname)
            return False
        instances = self.get_service_instances(svc.svcname, discard_empty=True)
        candidates = [nodename for (nodename, data) in instances.items() \
                      if data.get("avail") in ("up", "warn")]
        if len(candidates) == 0:
            self.log.info("service %s has no up instance, relax candidates "
                          "constraints", svc.svcname)
            candidates = self.placement_candidates(
                svc, discard_frozen=False,
                discard_unprovisioned=False,
            )
        try:
            top = self.placement_ranks(svc, candidates=candidates)[0]
            self.log.info("elected %s as the first node to take action on "
                          "service %s", top, svc.svcname)
        except IndexError:
            self.log.error("service %s placement ranks list is empty", svc.svcname)
            return True
        if top == rcEnv.nodename:
            return True
        instance = self.get_service_instance(svc.svcname, top)
        if instance is None and deleted:
            return True
        if instance["provisioned"] is provisioned:
            return True
        self.log.info("delay leader-first action on service %s", svc.svcname)
        return False

    def overloaded_up_service_instances(self, svcname):
        return [nodename for nodename in self.up_service_instances(svcname) if self.node_overloaded(nodename)]

    def scaler_slots(self, svcnames):
        count = 0
        for svcname in svcnames:
            svc = shared.SERVICES[svcname]
            if svc.topology == "flex":
                width = len([1 for nodename in svc.peers if nodename in shared.CLUSTER_DATA])
                count += min(width, svc.flex_min_nodes)
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
                status = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]["resources"][res.rid]["status"]
            except KeyError:
                continue
            if status in ("up", "stdby up", "n/a", "undef"):
                continue
            if res.nb_restart and self.get_smon_retries(svc.svcname, res.rid) < res.nb_restart:
                return True
            if res.monitor:
                return True
        return False

    def service_started_instances_count(self, svcname):
        """
        Count the number of service instances in 'started' local expect state.
        """
        jsonpath_expr = parse("*.services.status.'%s'.monitor.local_expect" % svcname)
        try:
            count = len([True for match in jsonpath_expr.find(shared.CLUSTER_DATA) if match.value == "started"])
            return count
        except Exception as exc:
            self.log.warning(exc)
            return 0

    def up_service_instances(self, svcname):
        nodenames = []
        for nodename, instance in self.get_service_instances(svcname).items():
            if instance["avail"] == "up":
                nodenames.append(nodename)
            elif instance["monitor"]["status"] in ("restarting", "starting", "wait children", "provisioning"):
                nodenames.append(nodename)
        return nodenames

    def peer_transitioning(self, svcname):
        """
        Return the nodename of the first peer with the service in a transition
        state.
        """
        for nodename, instance in self.get_service_instances(svcname).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"]["status"].endswith("ing"):
                return nodename

    def better_peers_ready(self, svc):
        ranks = self.placement_ranks(svc, candidates=svc.peers)
        peers = []
        for nodename in ranks:
            if nodename == rcEnv.nodename:
                return peers
            instance = self.get_service_instance(svc.svcname, nodename)
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
        for nodename, instance in self.get_service_instances(svc.svcname).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"].get("status") == "ready":
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
                    fstatus_l.append(node.get("frozen"))
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
            # during init for example
            return "unknown"
        topology = instance.get("topology")
        if topology == "failover":
            avail = self.get_agg_avail_failover(svcname)
        elif topology == "flex":
            avail = self.get_agg_avail_flex(svcname)
        else:
            avail = "unknown"

        if instance.get("scale") is not None:
            n_up = 0
            for slave in self.scaler_current_slaves(svcname):
                n_up += len(self.up_service_instances(slave))
            if n_up > 0 and n_up < instance.get("scale"):
                return "warn"

        slaves = instance.get("slaves", [])
        slaves += instance.get("scaler_slaves", [])
        if slaves:
            avails = set([avail])
            for child in slaves:
                try:
                    child_avail = shared.AGG[child]["avail"]
                except KeyError:
                    child_avail = "unknown"
                avails.add(child_avail)
            if avails == set(["n/a"]):
                return "n/a"
            avails -= set(["n/a"])
            if len(avails) == 1:
                return list(avails)[0]
            return "warn"
        elif instance.get("scale") is not None:
            # scaler without slaves
            return "n/a"
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
            for resource in instance["resources"].values():
                if resource.get("standby"):
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
            if "warn" in astatus_l:
                return "warn"
            else:
                return 'down'
        elif n_up > instance.get("flex_max_nodes", n_instances):
            return 'warn'
        elif n_up < instance.get("flex_min_nodes", 1) and not instance.get("scaler_slave"):
            # scaler slaves are allowed to go under-target: the scaler will pop more slices
            # to reach the target. This is what happens when a node goes does.
            return 'warn'
        else:
            return 'up'

    def get_agg_placement(self, svcname):
        try:
            if shared.SERVICES[svcname].placement == "none":
                return "n/a"
        except KeyError:
            pass
        instances = [instance for instance in self.get_service_instances(svcname).values() \
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
            if avail == "up":
                has_up = True
                if not leader:
                    placement = "non-optimal"
            elif leader:
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

    #########################################################################
    #
    # Convenience methods
    #
    #########################################################################
    def status_older_than_cf(self, svcname):
        status_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("status", {}).get(svcname, {}).get("updated", "0")
        config_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("config", {}).get(svcname, {}).get("updated", "0")
        return status_age < config_age

    def service_instances_frozen(self, svcname):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcname).items() if \
                instance.get("frozen")]

    def service_instances_thawed(self, svcname):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcname).items() if \
                not instance.get("frozen")]

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

    def wait_service_config_consensus(self, svcname, peers, timeout=60):
        if len(peers) < 2:
            return True
        self.log.info("wait for service %s consensus on config amongst peers %s",
                      svcname, ",".join(peers))
        for _ in range(timeout):
            if self.service_config_consensus(svcname, peers):
                return True
            time.sleep(1)
        self.log.error("service %s couldn't reach config consensus in %d seconds",
                       svcname, timeout)
        return False

    def service_config_consensus(self, svcname, peers):
        if len(peers) < 2:
            self.log.debug("%s auto consensus. peers: %s", svcname, peers)
            return True
        ref_csum = None
        for peer in peers:
            if peer not in shared.CLUSTER_DATA:
                # discard unreachable nodes from the consensus
                continue
            try:
                csum = shared.CLUSTER_DATA[peer]["services"]["config"][svcname]["csum"]
            except KeyError:
                #self.log.debug("service %s peer %s has no config cksum yet", svcname, peer)
                return False
            except Exception as exc:
                self.log.exception(exc)
                return False
            if ref_csum is None:
                ref_csum = csum
            if ref_csum is not None and ref_csum != csum:
                #self.log.debug("service %s peer %s has a different config cksum", svcname, peer)
                return False
        self.log.info("service %s config consensus reached", svcname)
        return True

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
                self.log.debug("purge %s status cache" % svcname)
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
                    try:
                        del shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]
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
                return shared.SERVICES[svcname].print_status_data(mon_data=False,
                                                                  refresh=True)
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
                # preserve previous status data if any (an action may be running)
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
                elif shared.NMON_DATA.status == "init":
                    thr = threading.Thread(target=self.service_status_fallback, args=(svcname,))
                    thr.start()
                    self.status_threads[svcname] = thr
                    data[svcname] = {}
                else:
                    data[svcname] = self.service_status_fallback(svcname)

            if svcname in self.status_threads:
                thr = self.status_threads[svcname]
                thr.join(0)
                if not thr.is_alive():
                    del self.status_threads[svcname]

            if not data[svcname]:
                del data[svcname]
                continue

            # update the frozen instance attribute
            with shared.SERVICES_LOCK:
                data[svcname]["frozen"] = shared.SERVICES[svcname].frozen()

            # embed the updated smon data
            self.set_smon_l_expect_from_status(data, svcname)
            data[svcname]["monitor"] = self.get_service_monitor(svcname)

            # forget the stonith target node if we run the service
            if data[svcname]["avail"] == "up" and "stonith" in data[svcname]["monitor"]:
                del data[svcname]["monitor"]["stonith"]

        # deleting services (still in SMON_DATA, no longer has cf).
        # emulate a status
        for svcname in set(shared.SMON_DATA.keys()) - set(svcnames):
            data[svcname] = {
                "monitor": self.get_service_monitor(svcname),
                "resources": {},
            }

        if shared.NMON_DATA.status == "init" and len(self.status_threads) == 0:
            if not self.rejoin_grace_period_expired:
                self.set_nmon(status="rejoin")
            else:
                self.set_nmon(status="idle")
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
                 if not data.get("frozen", False):
                     return False
        return True

    def all_nodes_thawed(self):
        with shared.CLUSTER_DATA_LOCK:
             for data in shared.CLUSTER_DATA.values():
                 if data.get("frozen", False):
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
        local_frozen = instance.get("frozen", False)
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
             (smon.global_expect == "unprovisioned" and provisioned is False):
            self.log.info("service %s global expect is %s, already is",
                          svcname, smon.global_expect)
            self.set_smon(svcname, global_expect="unset")
        elif smon.global_expect == "provisioned" and provisioned is True:
            if shared.AGG[svcname].avail in ("up", "n/a"):
                # provision success, thaw
                self.set_smon(svcname, global_expect="thawed")
            else:
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
        elif smon.global_expect == "placed" and \
             shared.AGG[svcname].placement in ("optimal", "n/a") and \
             shared.AGG[svcname].avail == "up":
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

    def get_arbitrators_data(self):
        if self.arbitrators_data is None or self.last_arbitrator_ping < time.time() - self.arbitrators_check_period:
            votes = self.arbitrators_votes()
            self.last_arbitrator_ping = time.time()
            self.arbitrators_data = {}
            for arbitrator in self.arbitrators:
                self.arbitrators_data[arbitrator["id"]] = {
                    "name": arbitrator["name"],
                    "status": "up" if arbitrator["name"] in votes else "down"
                }
        return self.arbitrators_data

    def update_cluster_data(self):
        """
        Rescan services config and status.
        """
        data = shared.CLUSTER_DATA[rcEnv.nodename]
        data["stats"] = shared.NODE.stats()
        data["frozen"] = self.freezer.node_frozen()
        data["env"] = shared.NODE.env
        data["speaker"] = self.speaker()
        data["min_avail_mem"] = shared.NODE.min_avail_mem
        data["min_avail_swap"] = shared.NODE.min_avail_swap
        data["services"]["config"] = self.get_services_config()
        data["services"]["status"] = self.get_services_status(data["services"]["config"].keys())

        if self.quorum:
            data["arbitrators"] = self.get_arbitrators_data()

        # purge deleted service instances
        for svcname in list(data["services"]["status"].keys()):
            if svcname not in data["services"]["config"]:
                self.log.debug("purge deleted service %s from status data", svcname)
                try:
                    del data["services"]["status"][svcname]
                except KeyError:
                    pass

    def update_hb_data(self):
        """
        Prepare the heartbeat data we send to other nodes.
        """
        now = time.time()

        if self.mon_changed():
            self.update_cluster_data()
        data = shared.CLUSTER_DATA[rcEnv.nodename]

        for key in ("updated", "gen"):
            # exclude from the diff
            try:
                del data[key]
            except KeyError:
                pass

        if self.last_node_data is not None:
            diff = json_delta.diff(
                self.last_node_data, data,
                verbose=False, array_align=False, compare_lengths=False
            )
        else:
            # first run
            self.last_node_data = json.loads(json.dumps(data))
            data["gen"] = self.get_gen(inc=True)
            data["updated"] = now
            return

        if len(diff) == 0:
            data["gen"] = self.get_gen(inc=False)
            data["updated"] = now
            return

        self.last_node_data = json.loads(json.dumps(data))
        data["gen"] = self.get_gen(inc=True)
        data["updated"] = now
        diff.append([["updated"], data["updated"]])

        shared.EVENT_Q.put({
            "nodename": rcEnv.nodename,
            "kind": "patch",
            "data": diff,
        })

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

    def merge_hb_data(self):
        self.merge_hb_data_compat()
        self.merge_hb_data_monitor()
        self.merge_hb_data_provision()

    def merge_hb_data_compat(self):
        compat = [data.get("compat") for data in shared.CLUSTER_DATA.values() if "compat" in data]
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
                local_frozen = shared.CLUSTER_DATA[rcEnv.nodename].get("frozen", False)
                if (global_expect == "frozen" and not local_frozen) or \
                   (global_expect == "thawed" and local_frozen):
                    self.log.info("node %s wants local node %s", nodename, global_expect)
                    self.set_nmon(global_expect=global_expect)
                #else:
                #    self.log.info("node %s wants local node %s, already is", nodename, global_expect)

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
                    if svcname in shared.SERVICES and shared.SERVICES[svcname].disabled and \
                       global_expect not in ("frozen", "thawed", "aborted", "deleted"):
                        continue
                    if global_expect == current_global_expect:
                        self.log.debug("node %s wants service %s %s, already targeting that",
                                       nodename, svcname, global_expect)
                        continue
                    if self.accept_g_expect(svcname, instance, global_expect):
                        self.log.info("node %s wants service %s %s", nodename, svcname, global_expect)
                        self.set_smon(svcname, global_expect=global_expect)
                    #else:
                    #    self.log.info("node %s wants service %s %s, already is", nodename, svcname, global_expect)

    def accept_g_expect(self, svcname, instance, global_expect):
        if svcname not in shared.AGG:
            return False
        if global_expect == "stopped":
            local_avail = instance["avail"]
            local_frozen = instance.get("frozen", False)
            if local_avail not in STOPPED_STATES or not local_frozen:
                return True
            else:
                return False
        elif global_expect == "shutdown":
            return not self.get_agg_shutdown(svcname)
        elif global_expect == "started":
            status = shared.AGG[svcname].avail
            local_frozen = instance.get("frozen", False)
            if status not in STARTED_STATES or local_frozen:
                return True
            else:
                return False
        elif global_expect == "frozen":
            frozen = shared.AGG[svcname].frozen
            if frozen != "frozen":
                return True
            else:
                return False
        elif global_expect == "thawed":
            frozen = shared.AGG[svcname].frozen
            if frozen != "thawed":
                 return True
            else:
                return False
        elif global_expect == "provisioned":
            provisioned = shared.AGG[svcname].provisioned
            if provisioned is not True:
                return True
            else:
                return False
        elif global_expect == "unprovisioned":
            provisioned = shared.AGG[svcname].provisioned
            if provisioned is not False:
                return True
            else:
                return False
        elif global_expect == "deleted":
            deleted = self.get_agg_deleted(svcname)
            if deleted is False:
                return True
            else:
                return False
        elif global_expect == "purged":
            provisioned = shared.AGG[svcname].provisioned
            deleted = self.get_agg_deleted(svcname)
            purged = self.get_agg_purged(provisioned, deleted)
            if purged is False:
                return True
            else:
                return False
        elif global_expect == "aborted":
            aborted = self.get_agg_aborted(svcname)
            if aborted is False:
                return True
            else:
                return False
        elif global_expect == "placed":
            placement = shared.AGG[svcname].placement
            if placement == "non-optimal":
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
                                try:
                                    shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcname]["resources"][resource.rid]["provisioned"]["mtime"] = remote.mtime
                                except KeyError:
                                    continue
                                changed = True
                    if changed:
                        try:
                            svc.print_status_data_eval(refresh=True)
                        except Exception:
                            # can happen when deleting the service
                            pass

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
        with shared.CLUSTER_DATA_LOCK:
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
        data["transitions"] = self.transition_count()
        data["frozen"] = self.get_clu_agg_frozen()
        data["services"] = self.get_agg_services()
        return data

    def merge_frozen(self):
        """
        This method is only called during the rejoin grace period.

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
            if self.service_frozen(svc.svcname):
                continue
            for peer in svc.peers:
                if peer == rcEnv.nodename:
                    continue
                smon = self.get_service_monitor(svc.svcname)
                if smon.global_expect == "thawed":
                    continue
                try:
                    frozen = shared.CLUSTER_DATA[peer]["services"]["status"][svc.svcname].get("frozen", False)
                except:
                    continue
                if frozen:
                    self.log.info("merge service '%s' frozen state from node '%s'",
                                  svc.svcname, peer)
                    svc.freezer.freeze()

    def service_frozen(self, svcname):
        try:
            return shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["frozen"]
        except:
            return

