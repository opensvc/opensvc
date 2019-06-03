"""
Monitor Thread
"""
import copy
import os
import sys
import time
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
from itertools import chain

import osvcd_shared as shared
import rcExceptions as ex
import json_delta
from rcGlobalEnv import rcEnv
from storage import Storage
from rcUtilities import bdecode, purge_cache, fsum, \
                        svc_pathetc, svc_pathvar, makedirs, split_svcpath, \
                        list_services, svc_pathcf, fmt_svcpath, \
                        resolve_svcpath, factory
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

ETC_NS_SKIP = len(os.path.join(rcEnv.paths.pathetcns, ""))

class Monitor(shared.OsvcThread):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
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

    def run(self):
        self.set_tid()
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.event("monitor_started")
        self.startup = time.time()
        self.rejoin_grace_period_expired = False
        self.shortloops = 0
        self.unfreeze_when_all_nodes_joined = False

        with shared.CLUSTER_DATA_LOCK:
            shared.CLUSTER_DATA[rcEnv.nodename] = {
                "compat": shared.COMPAT_VERSION,
                "agent": shared.NODE.agent_version,
                "monitor": dict(shared.NMON_DATA),
                "labels": shared.NODE.labels,
                "targets": shared.NODE.targets,
                "services": {},
            }

        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            if not self.freezer.node_frozen():
                self.event("node_freeze", data={"reason": "upgrade"})
                self.unfreeze_when_all_nodes_joined = True
                self.freezer.node_freeze()

        last_boot_id = shared.NODE.last_boot_id()
        boot_id = shared.NODE.asset.get_boot_id()
        try:
            # align float precision (py2/3 use different precision for mtime)
            last_boot_id = "%.02f" % float(last_boot_id)
            boot_id = "%.02f" % float(boot_id)
        except (TypeError, ValueError):
            pass
        self.log.info("boot id %s, last %s", boot_id, last_boot_id)
        if last_boot_id in (None, boot_id):
            self.services_init_status()
        else:
            self.kern_freeze()
            self.services_init_boot()
        shared.NODE.write_boot_id()

        # send a first message without service status, so the peers know
        # we are in init state.
        self.update_hb_data()

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
        for data in shared.SMON_DATA.values():
            if data.status and data.status != "scaling" and data.status.endswith("ing"):
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

    def reconfigure(self):
        """
        The node config references may have changed, update the services objects.
        """
        for svcpath in shared.SERVICES:
            try:
                name, namespace, kind = split_svcpath(svcpath)
                svc = factory(kind)(name, namespace, node=shared.NODE)
            except Exception as exc:
                continue
            with shared.SERVICES_LOCK:
                shared.SERVICES[svcpath] = svc

    def do(self):
        terminated = self.janitor_procs() + self.janitor_threads()
        changed = self.mon_changed()
        if terminated == 0 and not changed and self.shortloops < self.max_shortloops:
            self.shortloops += 1
            if self.shortloops == self.max_shortloops:
                # we're very idle, take time to ...
                self.update_completions()
            with shared.MON_TICKER:
                shared.MON_TICKER.wait(self.monitor_period)
            return
        if changed:
            with shared.MON_TICKER:
                self.log.debug("woken for:")
                for idx, reason in enumerate(shared.MON_CHANGED):
                    self.log.debug("%d. %s", idx, reason)
                self.unset_mon_changed()
        self.shortloops = 0
        self.reload_config()
        if self._shutdown:
            if len(self.procs) == 0:
                self.stop()
        else:
            self.update_cluster_data()
            self.merge_hb_data()
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
        for svcpath, data in confs.items():
            new_service = False
            with shared.SERVICES_LOCK:
                if svcpath not in shared.SERVICES:
                    new_service = True
            if self.has_instance_with(svcpath, global_expect=["purged", "deleted"]):
                continue
            if rcEnv.nodename not in data:
                # need to check if we should have this config ?
                new_service = True
            if new_service:
                ref_conf = Storage({
                    "csum": "",
                    "updated": 0,
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
                if svcpath in shared.SERVICES and \
                   rcEnv.nodename in shared.SERVICES[svcpath].nodes and \
                   ref_nodename in shared.SERVICES[svcpath].drpnodes:
                    # don't fetch drp config from prd nodes
                    return
            self.log.info("node %s has the most recent service %s config",
                          ref_nodename, svcpath)
            self.fetch_service_config(svcpath, ref_nodename)
            if new_service:
                self.init_new_service(svcpath)

    def init_new_service(self, svcpath):
        name, namespace, kind = split_svcpath(svcpath)
        try:
            shared.SERVICES[svcpath] = factory(kind)(name, namespace, node=shared.NODE)
        except Exception as exc:
            self.log.error("unbuildable service %s fetched: %s", svcpath, exc)
            return

        try:
            svc = shared.SERVICES[svcpath]
            if svc.kind == "svc":
                self.event("instance_freeze", {
                    "reason": "install",
                    "svcpath": svc.svcpath,
                })
                Freezer(svcpath).freeze()
            if not os.path.exists(svc.paths.cf):
                return
            self.service_status_fallback(svc.svcpath)
        except Exception:
            # can happen when deleting the service
            pass

    def fetch_service_config(self, svcpath, nodename):
        """
        Fetch and install the most recent service configuration file, using
        the remote node listener.
        """
        request = {
            "action": "get_service_config",
            "options": {
                "svcpath": svcpath,
            },
        }
        resp = self.daemon_send(request, nodename=nodename)
        if resp is None:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", svcpath, nodename, resp)
            return
        status = resp.get("status", 1)
        if status == 2:
            # peer is deleting this service
            self.log.info(resp.get("error", ""))
            return
        elif status != 0:
            self.log.error("unable to fetch service %s config from node %s: "
                           "received %s", svcpath, nodename, resp)
            return
        with tempfile.NamedTemporaryFile(dir=rcEnv.paths.pathtmp, delete=False) as filep:
            tmpfpath = filep.name
        try:
            with codecs.open(tmpfpath, "w", "utf-8") as filep:
                filep.write(resp["data"])
            with shared.SERVICES_LOCK:
                if svcpath in shared.SERVICES:
                    svc = shared.SERVICES[svcpath]
                else:
                    svc = None
            if svc:
                try:
                    results = svc._validate_config(path=filep.name)
                except Exception as exc:
                    self.log.error("service %s fetched config validation "
                                   "error: %s", svcpath, exc)
                    return
                try:
                    svc.postinstall()
                except Exception as exc:
                    self.log.error("service %s postinstall failed: %s", svcpath, exc)
            else:
                results = {"errors": 0}
            if results["errors"] == 0:
                dst = svc_pathcf(svcpath)
                makedirs(os.path.dirname(dst))
                shutil.copy(filep.name, dst)
                mtime = resp.get("mtime")
                if mtime:
                    os.utime(dst, (mtime, mtime))
            else:
                self.log.error("the service %s config fetched from node %s is "
                               "not valid", svcpath, nodename)
                return
        finally:
            os.unlink(tmpfpath)

        self.event("service_config_installed", {
            "svcpath": svcpath,
            "from": nodename
        })

    #########################################################################
    #
    # Node and Service Commands
    #
    #########################################################################
    def generic_callback(self, svcpath, **kwargs):
        self.set_smon(svcpath, **kwargs)
        self.update_hb_data()

    def node_stonith(self, node):
        proc = self.node_command(["stonith", "--node", node])

        # make sure we won't redo the stonith for another service
        with shared.SMON_DATA_LOCK:
             for svcpath, smon in shared.SMON_DATA.items():
                 if smon.stonith == node:
                     del shared.SMON_DATA[svcpath]["stonith"]

        # wait for 10sec before giving up
        for step in range(10):
            ret = proc.poll()
            if ret is not None:
                return ret
            time.sleep(1)

        # timeout, free the caller
        self.push_proc(proc=proc)

    def service_startstandby_resources(self, svcpath, rids, slave=None):
        self.set_smon(svcpath, "restarting")
        cmd = ["startstandby", "--rid", ",".join(rids)]
        if slave:
            cmd += ["--slave", slave]
        proc = self.service_command(svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[svcpath, rids],
            on_success_kwargs={"slave": slave},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources(self, svcpath, rids, slave=None):
        self.set_smon(svcpath, "restarting")
        cmd = ["start", "--rid", ",".join(rids)]
        if slave:
            cmd += ["--slave", slave]
        proc = self.service_command(svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_start_resources_on_success",
            on_success_args=[svcpath, rids],
            on_success_kwargs={"slave": slave},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "idle"},
        )

    def service_start_resources_on_success(self, svcpath, rids, slave=None):
        self.set_smon(svcpath, status="idle")
        self.update_hb_data()
        changed = False
        for rid in rids:
            instance = self.get_service_instance(svcpath, rcEnv.nodename)
            if instance is None:
                self.reset_smon_retries(svcpath, rid)
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
            self.reset_smon_retries(svcpath, rid)
        if changed:
            self.update_hb_data()

    def service_status(self, svcpath):
        if rcEnv.nodename not in shared.SERVICES[svcpath].nodes:
            self.log.info("skip service status refresh on foreign service")
            return
        smon = self.get_service_monitor(svcpath)
        if smon.status and smon.status.endswith("ing"):
            # no need to run status, the running action will refresh the status earlier
            return
        cmd = ["status", "--refresh", "--waitlock=0"]
        if self.has_proc(cmd):
            # no need to run status twice
            return
        proc = self.service_command(svcpath, cmd, local=False)
        self.push_proc(
            proc=proc,
            cmd=cmd,
        )

    def service_toc(self, svcpath):
        proc = self.service_command(svcpath, ["toc"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "toc failed"},
        )

    def service_start(self, svcpath, err_status="start failed"):
        self.set_smon(svcpath, "starting")
        proc = self.service_command(svcpath, ["start"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle", "local_expect": "started"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": err_status},
        )

    def service_stop(self, svcpath, force=False):
        self.set_smon(svcpath, "stopping")
        cmd = ["stop"]
        if force:
            cmd.append("--force")
        proc = self.service_command(svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "stop failed"},
        )

    def service_shutdown(self, svcpath):
        self.set_smon(svcpath, "shutdown")
        proc = self.service_command(svcpath, ["shutdown"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "shutdown failed", "local_expect": "unset"},
        )

    def service_delete(self, svcpath):
        self.set_smon(svcpath, "deleting", local_expect="unset")
        proc = self.service_command(svcpath, ["delete", "--purge-collector"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "delete failed"},
        )

    def service_purge(self, svc, leader=None):
        self.set_smon(svc.svcpath, "unprovisioning")
        if leader is None:
            candidates = self.placement_candidates(svc, discard_frozen=False,
                                                   discard_overloaded=False,
                                                   discard_unprovisioned=False,
                                                   discard_constraints_violation=False)
            leader = self.placement_leader(svc, candidates)
        else:
            leader = rcEnv.nodename == leader
        cmd = ["unprovision"]
        if leader:
            cmd += ["--leader"]
        proc = self.service_command(svc.svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_purge_on_success",
            on_success_args=[svc.svcpath],
            on_error="generic_callback",
            on_error_args=[svc.svcpath],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_purge_on_success(self, svcpath):
        self.set_smon(svcpath, "deleting", local_expect="unset")
        proc = self.service_command(svcpath, ["delete", "--purge-collector"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "purge failed"},
        )

    def service_provision(self, svc):
        self.set_smon(svc.svcpath, "provisioning")
        candidates = self.placement_candidates(svc, discard_frozen=False,
                                               discard_overloaded=False,
                                               discard_unprovisioned=False,
                                               discard_constraints_violation=False)
        cmd = ["provision"]
        if self.placement_leader(svc, candidates):
            cmd += ["--leader", "--disable-rollback"]
        proc = self.service_command(svc.svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="service_thaw",
            on_success_args=[svc.svcpath],
            on_success_kwargs={"slaves": True},
            on_error="generic_callback",
            on_error_args=[svc.svcpath],
            on_error_kwargs={"status": "provision failed"},
        )

    def service_unprovision(self, svc, leader=None):
        self.set_smon(svc.svcpath, "unprovisioning", local_expect="unset")
        if leader is None:
            candidates = self.placement_candidates(svc, discard_frozen=False,
                                                   discard_overloaded=False,
                                                   discard_unprovisioned=False,
                                                   discard_constraints_violation=False)
            leader = self.placement_leader(svc, candidates)
        else:
            leader = rcEnv.nodename == leader
        cmd = ["unprovision"]
        if leader:
            cmd += ["--leader"]
        proc = self.service_command(svc.svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svc.svcpath],
            on_success_kwargs={"status": "idle", "local_expect": "unset"},
            on_error="generic_callback",
            on_error_args=[svc.svcpath],
            on_error_kwargs={"status": "unprovision failed"},
        )

    def wait_global_expect_change(self, svcpath, ref, timeout):
        for step in range(timeout):
            global_expect = shared.SMON_DATA.get(svcpath, {}).get("global_expect")
            if global_expect != ref:
                return True
            time.sleep(1)
        return False

    def service_set_flex_instances(self, svcpath, instances):
        cmd = [
            "set",
            "--kw", "flex_min_nodes=%d" % instances,
            "--kw", "flex_max_nodes=%d" % instances,
        ]
        proc = self.service_command(svcpath, cmd)
        out, err = proc.communicate()
        return proc.returncode

    def service_create_scaler_slave(self, svcpath, svc, data, instances=None):
        data["DEFAULT"]["scaler_slave"] = "true"
        if svc.topology == "flex" and instances is not None:
            data["DEFAULT"]["flex_min_nodes"] = instances
            data["DEFAULT"]["flex_max_nodes"] = instances
        for kw in ("scale", "id"):
            try:
                del data["DEFAULT"][kw]
            except KeyError:
                pass
        cmd = ["create", "--config=-"]
        proc = self.service_command(svcpath, cmd, stdin=json.dumps(data))
        out, err = proc.communicate()
        if proc.returncode != 0:
            self.set_smon(svcpath, "create failed")
        self.service_status_fallback(svcpath)

        try:
            ret = self.wait_service_config_consensus(svcpath, svc.peers)
        except Exception as exc:
            self.log.exception(exc)
            return

        self.set_smon(svcpath, global_expect="thawed")
        self.wait_global_expect_change(svcpath, "thawed", 600)

        self.set_smon(svcpath, global_expect="provisioned")
        self.wait_global_expect_change(svcpath, "provisioned", 600)

    def service_freeze(self, svcpath):
        self.set_smon(svcpath, "freezing")
        proc = self.service_command(svcpath, ["freeze"])
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "idle"},
        )

    def service_thaw(self, svcpath, slaves=False):
        self.set_smon(svcpath, "thawing")
        cmd = ["thaw"]
        if slaves:
            cmd += ["--master", "--slaves"]
        proc = self.service_command(svcpath, cmd)
        self.push_proc(
            proc=proc,
            on_success="generic_callback",
            on_success_args=[svcpath],
            on_success_kwargs={"status": "idle"},
            on_error="generic_callback",
            on_error_args=[svcpath],
            on_error_kwargs={"status": "idle"},
        )

    def services_init_status(self):
        proc = self.service_command(",".join(list_services()), ["status", "--parallel", "--refresh"], local=False)
        self.push_proc(
            proc=proc,
            on_success="services_init_status_callback",
            on_error="services_init_status_callback",
        )

    def services_init_boot(self):
        proc = self.service_command(",".join(list_services()), ["boot", "--parallel"])
        self.push_proc(
            proc=proc,
            on_success="services_init_status_callback",
            on_error="services_init_status_callback",
        )

    def services_init_status_callback(self, *args, **kwargs):
        self.update_hb_data()
        if shared.NMON_DATA.status == "init":
            self.set_nmon(status="rejoin")
        self.rejoin_grace_period_expired = False


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
        svcpaths = [svcpath for svcpath in shared.SMON_DATA]
        self.get_agg_services()
        for svcpath in svcpaths:
            transitions = self.transition_count()
            if transitions > shared.NODE.max_parallel:
                self.duplog("info", "delay services orchestration: "
                            "%(transitions)d/%(max)d transitions already "
                            "in progress", transitions=transitions,
                            max=shared.NODE.max_parallel)
                break
            if self.status_older_than_cf(svcpath):
                #self.log.info("%s status dump is older than its config file",
                #              svcpath)
                self.service_status(svcpath)
                continue
            svc = self.get_service(svcpath)
            self.resources_orchestrator(svcpath, svc)
            self.service_orchestrator(svcpath, svc)
        self.sync_services_conf()

    def resources_orchestrator(self, svcpath, svc):
        if shared.NMON_DATA.status == "shutting":
            return
        if svc is None:
            return
        if self.instance_frozen(svcpath) or self.freezer.node_frozen():
            #self.log.info("resource %s orchestrator out (frozen)", svc.svcpath)
            return
        if svc.disabled:
            #self.log.info("resource %s orchestrator out (disabled)", svc.svcpath)
            return

        def monitored_resource(svc, rid, resource):
            if resource.get("disable"):
                return False
            if smon.local_expect != "started":
                return False
            try:
                nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            except AttributeError:
                nb_restart = 0
            retries = self.get_smon_retries(svc.svcpath, rid)

            if retries > nb_restart:
                return False
            elif retries == nb_restart:
                if nb_restart > 0:
                    self.event("max_resource_restart", {
                        "svcpath": svc.svcpath,
                        "rid": rid,
                        "resource": resource,
                        "restart": nb_restart,
                    })
                self.inc_smon_retries(svc.svcpath, rid)
                if resource.get("monitor"):
                    candidates = self.placement_candidates(svc)
                    if candidates != [rcEnv.nodename] and len(candidates) > 0:
                        self.event("resource_toc", {
                            "svcpath": svc.svcpath,
                            "rid": rid,
                            "resource": resource,
                        })
                        self.service_toc(svc.svcpath)
                    else:
                        self.event("resource_would_toc", {
                            "reason": "no_candidate",
                            "svcpath": svc.svcpath,
                            "rid": rid,
                            "resource": resource,
                        })
                else:
                    self.event("resource_degraded", {
                        "svcpath": svc.svcpath,
                        "rid": rid,
                        "resource": resource,
                    })
                    return False
            else:
                self.inc_smon_retries(svc.svcpath, rid)
                self.event("resource_restart", {
                    "svcpath": svc.svcpath,
                    "rid": rid,
                    "resource": resource,
                    "restart": nb_restart,
                    "try": retries+1,
                })
                return True

        def stdby_resource(svc, rid, resource):
            if resource.get("standby") is not True:
                return False
            nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            if nb_restart < self.default_stdby_nb_restart:
                nb_restart = self.default_stdby_nb_restart
            retries = self.get_smon_retries(svc.svcpath, rid)
            if retries > nb_restart:
                return False
            if retries >= nb_restart:
                self.inc_smon_retries(svc.svcpath, rid)
                self.event("max_stdby_resource_restart", {
                    "svcpath": svc.svcpath,
                    "rid": rid,
                    "resource": resource,
                    "restart": nb_restart,
                })
                return False
            self.inc_smon_retries(svc.svcpath, rid)
            self.event("stdby_resource_restart", {
                "svcpath": svc.svcpath,
                "rid": rid,
                "resource": resource,
                "restart": nb_restart,
                "try": retries+1,
            })
            return True

        smon = self.get_service_monitor(svc.svcpath)
        if smon.status != "idle":
            return
        if smon.global_expect in ("unprovisioned", "purged", "deleted", "frozen"):
            return

        try:
            with shared.CLUSTER_DATA_LOCK:
                instance = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcpath]
                if instance.get("encap") is True:
                    return
                resources = instance.get("resources", {})
        except KeyError:
            return

        mon_rids = []
        stdby_rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("warn", "down", "stdby down"):
                self.reset_smon_retries(svc.svcpath, rid)
                continue
            if resource.get("provisioned", {}).get("state") is False:
                continue
            if monitored_resource(svc, rid, resource):
                mon_rids.append(rid)
            elif stdby_resource(svc, rid, resource):
                stdby_rids.append(rid)

        if len(mon_rids) > 0:
            self.service_start_resources(svc.svcpath, mon_rids)
        if len(stdby_rids) > 0:
            self.service_startstandby_resources(svc.svcpath, stdby_rids)

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
                    self.reset_smon_retries(svc.svcpath, rid)
                    continue
                if resource.get("provisioned", {}).get("state") is False:
                    continue
                if monitored_resource(svc, rid, resource):
                    mon_rids.append(rid)
                elif stdby_resource(svc, rid, resource):
                    stdby_rids.append(rid)
            if len(mon_rids) > 0:
                self.service_start_resources(svc.svcpath, mon_rids, slave=crid)
            if len(stdby_rids) > 0:
                self.service_startstandby_resources(svc.svcpath, stdby_rids, slave=crid)

    def node_orchestrator(self):
        if shared.NMON_DATA.status == "shutting":
            return
        self.orchestrator_auto_grace()
        nmon = self.get_node_monitor()
        node_frozen = self.freezer.node_frozen()
        if self.unfreeze_when_all_nodes_joined and node_frozen and len(self.cluster_nodes) == len(shared.CLUSTER_DATA):
            self.event("node_thaw", data={"reason": "upgrade"})
            self.freezer.node_thaw()
            self.unfreeze_when_all_nodes_joined = False
            node_frozen = 0
        if nmon.status != "idle":
            return
        self.set_nmon_g_expect_from_status()
        if nmon.global_expect == "frozen":
            self.unfreeze_when_all_nodes_joined = False
            if not node_frozen:
                self.event("node_freeze", {"reason": "target"})
                self.freezer.node_freeze()
        elif nmon.global_expect == "thawed":
            self.unfreeze_when_all_nodes_joined = False
            if node_frozen:
                self.event("node_thaw", {"reason": "target"})
                self.freezer.node_thaw()

    def service_orchestrator(self, svcpath, svc):
        smon = self.get_service_monitor(svcpath)
        if svc is None:
            if smon and svcpath in shared.AGG:
                # deleting service: unset global expect if done cluster-wide
                status = shared.AGG[svcpath].avail
                self.set_smon_g_expect_from_status(svcpath, smon, status)
            return
        if self.peer_init(svc):
            return
        if smon.global_expect and smon.global_expect != "aborted":
            if "failed" in smon.status:
                if self.abort_state(smon.status, smon.global_expect, smon.placement):
                    self.set_smon(svcpath, global_expect="unset")
                    return
            elif smon.status not in ORCHESTRATE_STATES:
                #self.log.info("service %s orchestrator out (mon status %s)", svc.svcpath, smon.status)
                return
        status = shared.AGG[svc.svcpath].avail
        self.set_smon_g_expect_from_status(svc.svcpath, smon, status)
        if shared.NMON_DATA.status == "shutting":
            self.service_orchestrator_shutting(svc, smon, status)
        elif smon.global_expect:
            self.service_orchestrator_manual(svc, smon, status)
        else:
            self.service_orchestrator_auto(svc, smon, status)

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
    def scale_svcpath(svcpath, idx):
        name, namespace, kind = split_svcpath(svcpath)
        return fmt_svcpath(str(idx)+"."+name, namespace, kind)

    def service_orchestrator_auto(self, svc, smon, status):
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
            #self.log.info("service %s orchestrator out (disabled)", svc.svcpath)
            return
        if not self.compat:
            return
        if svc.topology == "failover" and smon.local_expect == "started":
            # decide if the service local_expect=started should be reset
            if status == "up" and self.get_service_instance(svc.svcpath, rcEnv.nodename).avail != "up":
                self.log.info("service '%s' is globally up but the local instance is "
                              "not and is in 'started' local expect. reset",
                              svc.svcpath)
                self.set_smon(svc.svcpath, local_expect="unset")
            elif self.service_started_instances_count(svc.svcpath) > 1 and \
                 self.get_service_instance(svc.svcpath, rcEnv.nodename).avail != "up" and \
                 not self.placement_leader(svc):
                self.log.info("service '%s' has multiple instance in 'started' "
                              "local expect and we are not leader. reset",
                              svc.svcpath)
                self.set_smon(svc.svcpath, local_expect="unset")
            elif status != "up" and \
                 self.get_service_instance(svc.svcpath, rcEnv.nodename).avail in ("down", "stdby down", "undef", "n/a") and \
                 not self.resources_orchestrator_will_handle(svc):
                self.log.info("service '%s' is not up and no resource monitor "
                              "action will be attempted, but "
                              "is in 'started' local expect. reset",
                              svc.svcpath)
                self.set_smon(svc.svcpath, local_expect="unset")
            else:
                return
        if self.instance_frozen(svc.svcpath) or self.freezer.node_frozen():
            #self.log.info("service %s orchestrator out (frozen)", svc.svcpath)
            return
        if not self.rejoin_grace_period_expired:
            return
        if svc.scale_target is not None and smon.global_expect is None:
            self.service_orchestrator_scaler(svc)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)",
            #              svc.svcpath, status)
            return

        if not self.pass_hard_affinities(svc):
            return

        candidates = self.placement_candidates(svc)
        if not self.pass_soft_affinities(svc, candidates):
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
            if nodename != rcEnv.nodename:
                # after loosing the placement leader status, the smon state
                # may need a reset
                if smon.status in ("ready", "wait parents"):
                    self.set_smon(svc.svcpath, "idle")
                # not natural leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.svcpath, rcEnv.nodename)
        if smon.global_expect in ("started", "placed"):
            allowed_status = ("down", "stdby down", "stdby up", "warn")
        else:
            allowed_status = ("down", "stdby down", "stdby up")
        if smon.status in ("ready", "wait parents"):
            if instance.avail == "up":
                self.log.info("abort '%s' because the local instance "
                              "has started", smon.status)
                self.set_smon(svc.svcpath, "idle")
                return
            if status not in allowed_status or \
               self.peer_warn(svc.svcpath):
                self.log.info("abort '%s' because the aggregated status has "
                              "gone %s", smon.status, status)
                self.set_smon(svc.svcpath, "idle")
                return
            peer = self.better_peer_ready(svc, candidates)
            if peer:
                self.log.info("abort '%s' because node %s has a better "
                              "placement score for service %s and is also "
                              "ready", smon.status, peer, svc.svcpath)
                self.set_smon(svc.svcpath, "idle")
                return
            peer = self.peer_transitioning(svc.svcpath)
            if peer:
                self.log.info("abort '%s' because node %s is already "
                              "acting on service %s", smon.status, peer,
                              svc.svcpath)
                self.set_smon(svc.svcpath, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.svcpath, status="idle")
                return
        elif smon.status == "ready":
            if self.parent_transitioning(svc):
                self.log.info("abort 'ready' because a parent is transitioning")
                self.set_smon(svc.svcpath, "idle")
                return
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.event("instance_start", {
                    "reason": "from_ready",
                    "svcpath": svc.svcpath,
                    "since": int(now-smon.status_updated),
                })
                if smon.stonith and smon.stonith not in shared.CLUSTER_DATA:
                    # stale peer which previously ran the service
                    self.node_stonith(smon.stonith)
                self.service_start(svc.svcpath, err_status="place failed" if smon.global_expect == "placed" else "start failed")
                return
            tmo = int(smon.status_updated + self.ready_period - now) + 1
            self.log.info("service %s will start in %d seconds",
                          svc.svcpath, tmo)
            self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect not in ("started", "placed"):
                return
            if status not in allowed_status:
                return
            if self.peer_warn(svc.svcpath):
                return
            if svc.disable_rollback and self.peer_start_failed(svc.svcpath):
                return
            peer = self.peer_transitioning(svc.svcpath)
            if peer:
                return
            if not self.placement_leader(svc, candidates):
                return
            if not self.parents_available(svc) or self.parent_transitioning(svc):
                self.set_smon(svc.svcpath, status="wait parents")
                return
            if len(svc.peers) == 1:
                self.event("instance_start", {
                    "reason": "single_node",
                    "svcpath": svc.svcpath,
                })
                self.service_start(svc.svcpath)
                return
            self.log.info("failover service %s status %s", svc.svcpath,
                          status)
            self.set_smon(svc.svcpath, "ready")

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
                # after loosing the placement leader status, the smon state
                # may need a reset
                if smon.status in ("ready", "wait parents"):
                    self.set_smon(svc.svcpath, "idle")
                # natural not a leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.svcpath, rcEnv.nodename)
        up_nodes = self.up_service_instances(svc.svcpath)
        n_up = len(up_nodes)
        n_missing = svc.flex_min_nodes - n_up

        if smon.status in ("ready", "wait parents"):
            if n_up > svc.flex_min_nodes:
                self.log.info("flex service %s instance count reached "
                              "required minimum while we were ready",
                              svc.svcpath)
                self.set_smon(svc.svcpath, "idle")
                return
            better_peers = self.better_peers_ready(svc);
            if n_missing > 0 and len(better_peers) >= n_missing:
                self.log.info("abort 'ready' because nodes %s have a better "
                              "placement score for service %s and are also "
                              "ready", ','.join(better_peers), svc.svcpath)
                self.set_smon(svc.svcpath, "idle")
                return
        if smon.status == "wait parents":
            if self.parents_available(svc):
                self.set_smon(svc.svcpath, status="idle")
                return
        if smon.status == "ready":
            now = time.time()
            if smon.status_updated < (now - self.ready_period):
                self.event("instance_start", {
                    "reason": "from_ready",
                    "svcpath": svc.svcpath,
                    "since": now-smon.status_updated,
                })
                self.service_start(svc.svcpath)
            else:
                tmo = int(smon.status_updated + self.ready_period - now) + 1
                self.log.info("service %s will start in %d seconds",
                              svc.svcpath, tmo)
                self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect not in ("started", "placed"):
                return
            if n_up < svc.flex_min_nodes:
                if smon.global_expect in ("started", "placed"):
                    allowed_avail = STOPPED_STATES + ["warn"]
                else:
                    allowed_avail = STOPPED_STATES
                if instance.avail not in allowed_avail:
                    return
                if not self.placement_leader(svc, candidates):
                    return
                if not self.parents_available(svc):
                    self.set_smon(svc.svcpath, status="wait parents")
                    return
                self.log.info("flex service %s started, starting or ready to "
                              "start instances: %d/%d-%d. local status %s",
                              svc.svcpath, n_up, svc.flex_min_nodes,
                              svc.flex_max_nodes, instance.avail)
                self.set_smon(svc.svcpath, "ready")
            elif n_up > svc.flex_max_nodes:
                if instance is None:
                    return
                if instance.avail not in STARTED_STATES:
                    return
                n_to_stop = n_up - svc.flex_max_nodes
                overloaded_up_nodes = self.overloaded_up_service_instances(svc.svcpath)
                to_stop = self.placement_ranks(svc, candidates=overloaded_up_nodes)[-n_to_stop:]
                n_to_stop -= len(to_stop)
                if n_to_stop > 0:
                    to_stop += self.placement_ranks(svc, candidates=set(up_nodes)-set(overloaded_up_nodes))[-n_to_stop:]
                self.log.info("%d nodes to stop to honor service %s "
                              "flex_max_nodes=%d. choose %s",
                              n_to_stop, svc.svcpath, svc.flex_max_nodes,
                              ", ".join(to_stop))
                if rcEnv.nodename not in to_stop:
                    return
                self.event("instance_stop", {
                    "reason": "flex_threshold",
                    "svcpath": svc.svcpath,
                    "up": n_up,
                })
                self.service_stop(svc.svcpath)

    def service_orchestrator_shutting(self, svc, smon, status):
        """
        Take actions to shutdown all local services instances marked with
        local_expect == "shutdown", even if frozen.

        Honor parents/children sequencing.
        """
        instance = self.get_service_instance(svc.svcpath, rcEnv.nodename)
        if smon.local_expect == "shutdown":
            if smon.status in ("shutdown", "shutdown failed"):
                return
            if self.is_instance_shutdown(instance):
                self.set_smon(svc.svcpath, local_expect="unset")
                return
            if not self.local_children_down(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcpath, status="idle")
            self.service_shutdown(svc.svcpath)

    def service_orchestrator_manual(self, svc, smon, status):
        """
        Take actions to meet global expect target, set by user or by
        service_orchestrator_auto()
        """
        instance = self.get_service_instance(svc.svcpath, rcEnv.nodename)
        if smon.global_expect == "frozen":
            if not self.instance_frozen(svc.svcpath):
                self.event("instance_freeze", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                    "monitor": smon,
                })
                self.service_freeze(svc.svcpath)
        elif smon.global_expect == "thawed":
            if self.instance_frozen(svc.svcpath):
                self.event("instance_thaw", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                    "monitor": smon,
                })
                self.service_thaw(svc.svcpath)
        elif smon.global_expect == "shutdown":
            if not self.children_down(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcpath, status="idle")

            if not self.instance_frozen(svc.svcpath):
                self.event("instance_freeze", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_freeze(svc.svcpath)
            elif not self.is_instance_shutdown(instance):
                thawed_on = self.service_instances_thawed(svc.svcpath)
                if thawed_on:
                    self.duplog("info", "service %(svcpath)s still has thawed "
                                "instances on nodes %(thawed_on)s, delay "
                                "shutdown",
                                svcpath=svc.svcpath,
                                thawed_on=", ".join(thawed_on))
                else:
                    self.service_shutdown(svc.svcpath)
        elif smon.global_expect == "stopped":
            if not self.children_down(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcpath, status="idle")

            if not self.instance_frozen(svc.svcpath):
                self.log.info("freeze service %s", svc.svcpath)
                self.event("instance_freeze", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_freeze(svc.svcpath)
            elif instance.avail not in STOPPED_STATES:
                thawed_on = self.service_instances_thawed(svc.svcpath)
                if thawed_on:
                    self.duplog("info", "service %(svcpath)s still has thawed instances "
                                "on nodes %(thawed_on)s, delay stop",
                                svcpath=svc.svcpath,
                                thawed_on=", ".join(thawed_on))
                else:
                    self.event("instance_stop", {
                        "reason": "target",
                        "svcpath": svc.svcpath,
                    })
                    self.service_stop(svc.svcpath)
        elif smon.global_expect == "started":
            if not self.parents_available(svc):
                self.set_smon(svc.svcpath, status="wait parents")
                return
            elif smon.status == "wait parents":
                self.set_smon(svc.svcpath, status="idle")
            if self.instance_frozen(svc.svcpath):
                self.event("instance_thaw", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_thaw(svc.svcpath)
            elif status not in STARTED_STATES:
                if shared.AGG[svc.svcpath].frozen != "thawed":
                    return
                self.service_orchestrator_auto(svc, smon, status)
        elif smon.global_expect == "unprovisioned":
            if smon.status in ("unprovisioning", "stopping"):
                return
            if instance.avail not in STOPPED_STATES:
                self.event("instance_stop", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_stop(svc.svcpath, force=True)
                return
            if shared.AGG[svc.svcpath].avail not in STOPPED_STATES:
                return
            if smon.status == "wait children":
                if not self.children_unprovisioned(svc):
                    return
            elif smon.status == "wait non-leader":
                if not self.leader_last(svc, provisioned=False, silent=True):
                    self.log.info("service %s still waiting non leaders", svc.svcpath)
                    return
            if svc.svcpath not in shared.SERVICES or self.instance_unprovisioned(instance):
                if smon.status != "idle":
                    self.set_smon(svc.svcpath, status="idle")
                return
            if not self.children_unprovisioned(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            leader = self.leader_last(svc, provisioned=False)
            if not leader:
                self.set_smon(svc.svcpath, status="wait non-leader")
                return
            self.event("instance_unprovision", {
                "reason": "target",
                "svcpath": svc.svcpath,
            })
            self.service_unprovision(svc, leader)
        elif smon.global_expect == "provisioned":
            if smon.status == "wait parents":
                if not self.parents_available(svc):
                    return
            elif smon.status == "wait leader":
                if not self.leader_first(svc, provisioned=True, silent=True):
                    return
            elif smon.status == "wait sync":
                if not self.min_instances_reached(svc):
                    return
            if self.instance_provisioned(instance):
                self.set_smon(svc.svcpath, status="idle")
                return
            if not self.min_instances_reached(svc):
                self.set_smon(svc.svcpath, status="wait sync")
                return
            if not self.parents_available(svc):
                self.set_smon(svc.svcpath, status="wait parents")
                return
            if not self.leader_first(svc, provisioned=True):
                self.set_smon(svc.svcpath, status="wait leader")
                return
            self.event("instance_provision", {
                "reason": "target",
                "svcpath": svc.svcpath,
            })
            self.service_provision(svc)
        elif smon.global_expect == "deleted":
            if not self.children_down(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.svcpath, status="idle")
            if svc.svcpath in shared.SERVICES:
                self.event("instance_delete", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_delete(svc.svcpath)
        elif smon.global_expect == "purged":
            if smon.status in ("purging", "deleting", "stopping"):
                return
            if instance.avail not in STOPPED_STATES:
                self.event("instance_stop", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_stop(svc.svcpath, force=True)
                return
            if shared.AGG[svc.svcpath].avail not in STOPPED_STATES:
                return
            if smon.status == "wait children":
                if not self.children_unprovisioned(svc):
                    return
            elif smon.status == "wait non-leader":
                if not self.leader_last(svc, provisioned=False, silent=True, deleted=True):
                    return
            if not self.children_unprovisioned(svc):
                self.set_smon(svc.svcpath, status="wait children")
                return
            leader = self.leader_last(svc, provisioned=False, deleted=True)
            if not leader:
                self.set_smon(svc.svcpath, status="wait non-leader")
                return
            if svc.svcpath in shared.SERVICES and svc.kind not in ("vol", "svc"):
                # base services do not implement the purge action
                self.event("instance_delete", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_delete(svc.svcpath)
                return
            if svc.svcpath not in shared.SERVICES or not instance:
                if smon.status != "idle":
                    self.set_smon(svc.svcpath, status="idle")
                return
            self.event("instance_purge", {
                "reason": "target",
                "svcpath": svc.svcpath,
            })
            self.service_purge(svc, leader)
        elif smon.global_expect == "aborted" and \
             smon.local_expect not in (None, "started"):
            self.event("instance_abort", {
                "reason": "target",
                "svcpath": svc.svcpath,
            })
            self.set_smon(svc.svcpath, local_expect="unset")
        elif smon.global_expect == "placed":
            # refresh smon for placement attr change caused by a clear
            smon = self.get_service_monitor(svc.svcpath)
            if self.instance_frozen(svc.svcpath):
                self.event("instance_thaw", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_thaw(svc.svcpath)
            elif smon.placement != "leader":
                if not self.has_leader(svc):
                    # avoid stopping the instance if no peer node can takeover
                    return
                if instance.avail not in STOPPED_STATES:
                    self.event("instance_stop", {
                        "reason": "target",
                        "svcpath": svc.svcpath,
                    })
                    self.service_stop(svc.svcpath)
            elif self.non_leaders_stopped(svc.svcpath) and \
                 (shared.AGG[svc.svcpath].placement not in ("optimal", "n/a") or shared.AGG[svc.svcpath].avail != "up") and \
                 instance.avail not in STARTED_STATES:
                self.service_orchestrator_auto(svc, smon, status)
        elif smon.global_expect.startswith("placed@"):
            target = smon.global_expect.split("@")[-1].split(",")
            candidates = self.placement_candidates(
                svc, discard_frozen=False,
                discard_overloaded=False,
                discard_unprovisioned=False,
                discard_constraints_violation=False,
                discard_start_failed=False
            )
            if self.instance_frozen(svc.svcpath):
                self.event("instance_thaw", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_thaw(svc.svcpath)
            elif rcEnv.nodename not in target:
                if smon.status == "stop failed":
                    return
                if instance.avail not in STOPPED_STATES and (set(target) & set(candidates)):
                    self.event("instance_stop", {
                        "reason": "target",
                        "svcpath": svc.svcpath,
                    })
                    self.service_stop(svc.svcpath)
            elif self.instances_stopped(svc.svcpath, set(svc.peers) - set(target)) and \
                 rcEnv.nodename in target and \
                 instance.avail in STOPPED_STATES + ["warn"]:
                if smon.status in ("start failed", "place failed"):
                    return
                self.event("instance_start", {
                    "reason": "target",
                    "svcpath": svc.svcpath,
                })
                self.service_start(svc.svcpath, err_status="place failed" if smon.global_expect == "placed" else "start failed")

    def scaler_current_slaves(self, svcpath):
        name, namespace, kind = split_svcpath(svcpath)
        pattern = "[0-9]+\." + name + "$"
        if namespace:
            pattern = "^%s/%s/%s" % (namespace, kind, pattern)
        else:
            pattern = "^%s" % pattern
        return [slave for slave in shared.SERVICES if re.match(pattern, slave)]

    def service_orchestrator_scaler(self, svc):
        smon = self.get_service_monitor(svc.svcpath)
        if smon.status != "idle":
            return
        peer = self.peer_transitioning(svc.svcpath)
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
        current_slaves = self.scaler_current_slaves(svc.svcpath)
        n_slots = self.scaler_slots(current_slaves)
        if n_slots == svc.scale_target:
            return
        missing = svc.scale_target - n_slots
        if missing > 0:
            self.event("scale_up", {
               "svcpath": svc.svcpath,
               "delta": missing,
            })
            self.service_orchestrator_scaler_up(svc, missing, current_slaves)
        else:
            self.event("scale_down", {
               "svcpath": svc.svcpath,
               "delta": -missing,
            })
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
        current_slaves = self.sort_scaler_slaves(current_slaves, reverse=True)
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
        for slavename in [self.scale_svcpath(svc.svcpath, idx) for idx in range(n_current_slaves)]:
            if slavename in current_slaves:
                continue
            to_add.append([slavename, width])
            slaves_count -= 1
            if slaves_count == 0:
                break

        to_add += [[self.scale_svcpath(svc.svcpath, n_current_slaves+idx), width] for idx in range(slaves_count)]
        if left != 0 and len(to_add):
            to_add[-1][1] = left
        to_add = to_add[:max_burst]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale service %s: %s", svc.svcpath, delta)
        self.set_smon(svc.svcpath, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.svcpath, exc)

    def service_orchestrator_scaler_down_flex(self, svc, missing, current_slaves):
        to_remove = []
        excess = -missing
        for slavename in self.sort_scaler_slaves(current_slaves, reverse=True):
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
        self.log.info("scale service %s: %s", svc.svcpath, delta)
        self.set_smon(svc.svcpath, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.svcpath, exc)

    @staticmethod
    def sort_scaler_slaves(slaves, reverse=False):
        return sorted(slaves, key=lambda x: int(x.split("/")[-1].split(".")[0]), reverse=reverse)

    def service_orchestrator_scaler_up_failover(self, svc, missing, current_slaves):
        slaves_count = missing
        n_current_slaves = len(current_slaves)
        new_slaves_list = [self.scale_svcpath(svc.svcpath, n_current_slaves+idx) for idx in range(slaves_count)]

        to_add = self.sort_scaler_slaves(new_slaves_list)
        to_add = [[svcpath, None] for svcpath in to_add]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale service %s: %s", svc.svcpath, delta)
        self.set_smon(svc.svcpath, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.svcpath, exc)

    def service_orchestrator_scaler_down_failover(self, svc, missing, current_slaves):
        slaves_count = -missing
        n_current_slaves = len(current_slaves)
        slaves_list = [self.scale_svcpath(svc.svcpath, n_current_slaves-1-idx) for idx in range(slaves_count)]

        to_remove = self.sort_scaler_slaves(slaves_list)
        to_remove = [svcpath for svcpath in to_remove]
        delta = "delete " + ",".join([elem[0] for elem in to_remove])
        self.log.info("scale service %s: %s", svc.svcpath, delta)
        self.set_smon(svc.svcpath, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.svcpath, exc)

    def scaling_worker(self, svc, to_add, to_remove):
        threads = []
        for svcpath, instances in to_add:
            if svcpath in shared.SERVICES:
                continue
            data = svc.print_config_data()
            try:
                thr = threading.Thread(
                    target=self.service_create_scaler_slave,
                    args=(svcpath, svc, data, instances)
                )
                thr.start()
                threads.append(thr)
            except RuntimeError as exc:
                self.log.warning("failed to start a scaling thread for "
                                 "service %s: %s", svc.svcpath, exc)
        for svcpath in to_remove:
            if svcpath not in shared.SERVICES:
                continue
            self.set_smon(svcpath, global_expect="purged")
        for svcpath in to_remove:
            self.wait_global_expect_change(svcpath, "purged", 300)
        while True:
            for thr in threads:
                thr.join(0)
            if any(thr.is_alive() for thr in threads):
                time.sleep(1)
                if self.stopped():
                    break
                continue
            break
        self.set_smon(svc.svcpath, global_expect="unset", status="idle")

    def pass_hard_affinities(self, svc):
        if svc.hard_anti_affinity:
            intersection = set(self.get_local_svcpaths()) & set(svc.hard_anti_affinity)
            if len(intersection) > 0:
                #self.log.info("service %s orchestrator out (hard anti-affinity with %s)",
                #              svc.svcpath, ','.join(intersection))
                return False
        if svc.hard_affinity:
            intersection = set(self.get_local_svcpaths()) & set(svc.hard_affinity)
            if len(intersection) < len(set(svc.hard_affinity)):
                #self.log.info("service %s orchestrator out (hard affinity with %s)",
                #              svc.svcpath, ','.join(intersection))
                return False
        return True

    def pass_soft_affinities(self, svc, candidates):
        if candidates != [rcEnv.nodename]:
            # the local node is not the only candidate, we can apply soft
            # affinity filtering
            if svc.soft_anti_affinity:
                intersection = set(self.get_local_svcpaths()) & set(svc.soft_anti_affinity)
                if len(intersection) > 0:
                    #self.log.info("service %s orchestrator out (soft anti-affinity with %s)",
                    #              svc.svcpath, ','.join(intersection))
                    return False
            if svc.soft_affinity:
                intersection = set(self.get_local_svcpaths()) & set(svc.soft_affinity)
                if len(intersection) < len(set(svc.soft_affinity)):
                    #self.log.info("service %s orchestrator out (soft affinity with %s)",
                    #              svc.svcpath, ','.join(intersection))
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
        n_idle = len([1 for node in shared.CLUSTER_DATA.values() if node.get("monitor", {}).get("status") in ("idle", "rejoin") and "services" in node])
        if n_idle >= len(self.cluster_nodes):
            self.end_rejoin_grace_period("now rejoined")
            return False
        now = time.time()
        if now > self.startup + self.rejoin_grace_period:
            self.end_rejoin_grace_period("expired, but some nodes are still "
                                         "unreacheable. freeze node.")
            self.event("node_freeze", data={"reason": "rejoin_expire"})
            self.freezer.node_freeze()
            return False
        self.duplog("info", "in rejoin grace period", nodename="")
        return True

    def local_children_down(self, svc):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
            if child == svc.svcpath:
                continue
            instance = self.get_service_instance(child, rcEnv.nodename)
            if not instance:
                continue
            avail = instance.get("avail", "unknown")
            if avail in STOPPED_STATES + ["unknown"]:
                continue
            missing.append(child)
        if len(missing) == 0:
            self.duplog("info", "service %(svcpath)s local children all avail down",
                        svcpath=svc.svcpath)
            return True
        self.duplog("info", "service %(svcpath)s local children still available:"
                    " %(missing)s", svcpath=svc.svcpath,
                    missing=" ".join(missing))
        return False

    def children_unprovisioned(self, svc):
        return self.children_down(svc, unprovisioned=True)

    def children_down(self, svc, unprovisioned=None):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
            child = resolve_svcpath(child, svc.namespace)
            if child == svc.svcpath:
                continue
            try:
                avail = shared.AGG[child].avail
            except KeyError:
                avail = "unknown"
            if avail not in STOPPED_STATES + ["unknown"]:
                missing.append(child)
                continue
            if unprovisioned:
                try:
                    prov = shared.AGG[child].provisioned
                except KeyError:
                    prov = "unknown"
                if prov not in [False, "unknown"]:
                    # mixed or true
                    missing.append(child)
        if len(missing) == 0:
            state = "avail down"
            if unprovisioned:
                state += " and unprovisioned"
            self.duplog("info", "service %(svcpath)s children all %(state)s:"
                        " %(children)s", svcpath=svc.svcpath, state=state,
                        children=" ".join(svc.children_and_slaves))
            return True
        state = "available"
        if unprovisioned:
            state += " or provisioned"
        self.duplog("info", "service %(svcpath)s children still %(state)s:"
                    " %(missing)s", svcpath=svc.svcpath, state=state,
                    missing=" ".join(missing))
        return False

    def parents_available(self, svc):
        missing = []
        if len(svc.parents) == 0:
            return True
        for parent in svc.parents:
            try:
                parent, nodename = parent.split("@")
            except ValueError:
                nodename = None
            parent = resolve_svcpath(parent, svc.namespace)
            if parent == svc.svcpath:
                continue
            if nodename:
                instance = self.get_service_instance(parent, nodename)
            else:
                instance = None
            if instance:
                avail = instance["avail"] 
            else:
                try:
                    avail = shared.AGG[parent].avail
                except KeyError:
                    avail = "unknown"
            if avail in STARTED_STATES + ["unknown"]:
                continue
            missing.append(parent)
        if len(missing) == 0:
            self.duplog("info", "service %(svcpath)s parents all avail up",
                        svcpath=svc.svcpath)
            return True
        self.duplog("info", "service %(svcpath)s parents not available:"
                    " %(missing)s", svcpath=svc.svcpath,
                    missing=" ".join(missing))
        return False

    def min_instances_reached(self, svc):
        instances = self.get_service_instances(svc.svcpath, discard_empty=False)
        live_nodes = [nodename for nodename in shared.CLUSTER_DATA if shared.CLUSTER_DATA[nodename] is not None]
        min_instances = set(svc.peers) & set(live_nodes)
        return len(instances) >= len(min_instances)

    def instances_started_or_start_failed(self, svcpath, nodes):
        for nodename in nodes:
            instance = self.get_service_instance(svcpath, nodename)
            if instance is None:
                continue
            if instance.get("avail") in STOPPED_STATES and instance["monitor"].get("status") != "start failed":
                return False
        self.log.info("service '%s' instances on nodes '%s' are stopped",
            svcpath, ", ".join(nodes))
        return True

    def instances_stopped(self, svcpath, nodes):
        for nodename in nodes:
            instance = self.get_service_instance(svcpath, nodename)
            if instance is None:
                continue
            if instance.get("avail") not in STOPPED_STATES:
                self.log.info("service '%s' instance node '%s' is not stopped yet",
                              svcpath, nodename)
                return False
        return True

    def has_leader(self, svc):
        for nodename, instance in self.get_service_instances(svc.svcpath).items():
            if instance["monitor"].get("placement") == "leader":
                return True
        return False

    def non_leaders_stopped(self, svcpath, exclude_status=None):
        svc = self.get_service(svcpath)
        if svc is None:
            return True
        if exclude_status is None:
            exclude_status = []
        for nodename in svc.peers:
            if nodename == rcEnv.nodename:
                continue
            instance = self.get_service_instance(svc.svcpath, nodename)
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
                self.log.info("service '%s' non leader instance on node '%s' "
                              "is not stopped yet %s",
                              svc.svcpath, nodename, extra)
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
        if shared.AGG[svc.svcpath].avail is None:
            # base services can be unprovisioned and purged in parallel
            return rcEnv.nodename
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
                              "service %s", top, svc.svcpath)
        except IndexError:
            if not silent:
                self.log.info("unblock service %s leader last action (placement ranks empty)", svc.svcpath)
            return rcEnv.nodename
        if top != rcEnv.nodename:
            if not silent:
                self.log.info("unblock service %s leader last action (not leader)",
                              svc.svcpath)
            return top
        for node in svc.peers:
            if node == rcEnv.nodename:
                continue
            instance = self.get_service_instance(svc.svcpath, node)
            if instance is None:
                continue
            elif deleted:
                if not silent:
                    self.log.info("delay leader-last action on service %s: "
                                  "node %s is still not deleted", svc.svcpath, node)
                return
            if instance.get("provisioned", False) is not provisioned:
                if not silent:
                    self.log.info("delay leader-last action on service %s: "
                                  "node %s is still %s", svc.svcpath, node,
                                  "unprovisioned" if provisioned else "provisioned")
                return
        self.log.info("unblock service %s leader last action (leader)",
                      svc.svcpath)
        return rcEnv.nodename

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
        instances = self.get_service_instances(svc.svcpath, discard_empty=True)
        candidates = [nodename for (nodename, data) in instances.items() \
                      if data.get("avail") in ("up", "warn")]
        if len(candidates) == 0:
            if not silent:
                self.log.info("service %s has no up instance, relax candidates "
                              "constraints", svc.svcpath)
            candidates = self.placement_candidates(
                svc, discard_frozen=False,
                discard_unprovisioned=False,
            )
        try:
            top = self.placement_ranks(svc, candidates=candidates)[0]
            if not silent:
                self.log.info("elected %s as the first node to take action on "
                              "service %s", top, svc.svcpath)
        except IndexError:
            if not silent:
                self.log.error("service %s placement ranks list is empty", svc.svcpath)
            return True
        if top == rcEnv.nodename:
            return True
        instance = self.get_service_instance(svc.svcpath, top)
        if instance is None and deleted:
            return True
        if instance.get("provisioned", True) is provisioned:
            return True
        if not silent:
            self.log.info("delay leader-first action on service %s", svc.svcpath)
        return False

    def overloaded_up_service_instances(self, svcpath):
        return [nodename for nodename in self.up_service_instances(svcpath) if self.node_overloaded(nodename)]

    def scaler_slots(self, svcpaths):
        count = 0
        for svcpath in svcpaths:
            svc = shared.SERVICES[svcpath]
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
                status = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcpath]["resources"][res.rid]["status"]
            except KeyError:
                continue
            if status in ("up", "stdby up", "n/a", "undef"):
                continue
            if res.nb_restart and self.get_smon_retries(svc.svcpath, res.rid) < res.nb_restart:
                return True
            if res.monitor:
                return True
        return False

    def service_started_instances_count(self, svcpath):
        """
        Count the number of service instances in 'started' local expect state.
        """
        jsonpath_expr = parse("*.services.status.'%s'.monitor.local_expect" % svcpath)
        try:
            count = len([True for match in jsonpath_expr.find(shared.CLUSTER_DATA) if match.value == "started"])
            return count
        except Exception as exc:
            self.log.warning(exc)
            return 0

    def up_service_instances(self, svcpath):
        nodenames = []
        for nodename, instance in self.get_service_instances(svcpath).items():
            if instance["avail"] == "up":
                nodenames.append(nodename)
            elif instance["monitor"].get("status") in ("restarting", "starting", "wait children", "provisioning", "placing"):
                nodenames.append(nodename)
        return nodenames

    def parent_transitioning(self, svc):
        if len(svc.parents) == 0:
            return False
        for parent in svc.parents:
            if parent == svc.svcpath:
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

    def peer_warn(self, svcpath, with_self=False):
        """
        For failover services, return the nodename of the first peer with the
        service in warn avail status.
        """
        try:
            if shared.SERVICES[svcpath].topology != "failover":
                return
        except:
            return
        for nodename, instance in self.get_service_instances(svcpath).items():
            if not with_self and nodename == rcEnv.nodename:
                continue
            if instance["avail"] == "warn" and not instance["monitor"].get("status").endswith("ing"):
                return nodename

    def peers_options(self, svcpath, candidates, status):
        """
        Return the nodes in <candidates> that are a viable start/place
        orchestration option.

        This method is used to determine if the global expect should be
        reset if no options are left.
        """
        nodenames = []
        for nodename in candidates:
            instance = self.get_service_instance(svcpath, nodename)
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

    def peer_transitioning(self, svcpath, discard_local=True):
        """
        Return the nodename of the first peer with the service in a transition
        state.
        """
        for nodename, instance in self.get_service_instances(svcpath).items():
            if discard_local and nodename == rcEnv.nodename:
                continue
            if instance["monitor"].get("status", "").endswith("ing"):
                return nodename

    def peer_start_failed(self, svcpath):
        """
        Return the nodename of the first peer with the service in a start failed
        state.
        """
        for nodename, instance in self.get_service_instances(svcpath).items():
            if nodename == rcEnv.nodename:
                continue
            if instance["monitor"].get("status") == "start failed":
                return nodename

    def better_peers_ready(self, svc):
        ranks = self.placement_ranks(svc, candidates=svc.peers)
        peers = []
        for nodename in ranks:
            if nodename == rcEnv.nodename:
                return peers
            instance = self.get_service_instance(svc.svcpath, nodename)
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
        for nodename, instance in self.get_service_instances(svc.svcpath).items():
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
    def get_agg_avail(self, svcpath):
        try:
            instance = self.get_any_service_instance(svcpath)
        except IndexError:
            instance = None
        if instance is None:
            # during init for example
            return "unknown"
        topology = instance.get("topology")
        if topology == "failover":
            avail = self.get_agg_avail_failover(svcpath)
        elif topology == "flex":
            avail = self.get_agg_avail_flex(svcpath)
        else:
            avail = "unknown"

        if instance.get("scale") is not None:
            n_up = 0
            for slave in self.scaler_current_slaves(svcpath):
                n_up += len(self.up_service_instances(slave))
            if n_up == 0:
                return "n/a"
            if n_up > 0 and n_up < instance.get("scale"):
                return "warn"

        slaves = instance.get("slaves", [])
        slaves += instance.get("scaler_slaves", [])
        if slaves:
            _, namespace, _ = split_svcpath(svcpath)
            avails = set([avail])
            for child in slaves:
                child = resolve_svcpath(child, namespace)
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

    def get_agg_overall(self, svcpath):
        ostatus = 'undef'
        ostatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcpath).values():
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
            instance = self.get_any_service_instance(svcpath)
        except IndexError:
            instance = Storage()
        if instance is None:
            # during init for example
            return "unknown"
        slaves = instance.get("slaves", [])
        slaves += instance.get("scaler_slaves", [])
        if slaves:
            _, namespace, _ = split_svcpath(svcpath)
            avails = set([ostatus])
            for child in slaves:
                child = resolve_svcpath(child, namespace)
                try:
                    child_status = shared.AGG[child]["overall"]
                except KeyError:
                    child_status = "unknown"
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

    def get_agg_frozen(self, svcpath):
        frozen = 0
        total = 0
        for instance in self.get_service_instances(svcpath).values():
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
            for resource in instance.get("resources", {}).values():
                if resource.get("standby"):
                    return True
            return False
        _has_stdby = has_stdby(instance)
        if _has_stdby and instance["avail"] not in ("n/a", "stdby down") or \
           not _has_stdby and instance["avail"] not in ("n/a", "down"):
            return False
        return True

    def get_agg_shutdown(self, svcpath):
        for instance in self.get_service_instances(svcpath).values():
            if not self.is_instance_shutdown(instance):
                return False
        return True

    def get_agg_avail_failover(self, svcpath):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcpath).values():
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

    def get_agg_avail_flex(self, svcpath):
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svcpath).values():
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

    def get_agg_placement(self, svcpath):
        try:
            if shared.SERVICES[svcpath].placement == "none":
                return "n/a"
            if shared.SERVICES[svcpath].topology == "flex" and shared.SERVICES[svcpath].flex_min_nodes == 0:
                return "n/a"
        except KeyError:
            pass
        instances = [instance for instance in self.get_service_instances(svcpath).values() \
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

    def get_agg_provisioned(self, svcpath):
        provisioned = 0
        total = 0
        for instance in self.get_service_instances(svcpath).values():
            if "provisioned" not in instance:
                continue
            total += 1
            if instance.get("provisioned", True):
                provisioned += 1
        if total == 0:
            return "n/a"
        elif provisioned == total:
            return True
        elif provisioned == 0:
            return False
        return "mixed"

    def get_agg_aborted(self, svcpath):
        for inst in self.get_service_instances(svcpath).values():
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

    def get_agg_deleted(self, svcpath):
        if len([True for inst in self.get_service_instances(svcpath).values() if "updated" in inst]) > 0:
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
    def status_older_than_cf(self, svcpath):
        """
        Return True if the instance status data is older than its config data
        or if one of the age is not a timestamp float.

        Returning True skips orchestration of the instance.
        """
        status_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("status", {}).get(svcpath, {}).get("updated", 0)
        config_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("config", {}).get(svcpath, {}).get("updated", 0)
        try:
            return status_age < config_age
        except TypeError:
            return True

    def service_instances_frozen(self, svcpath):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcpath).items() if \
                instance.get("frozen")]

    def service_instances_thawed(self, svcpath):
        """
        Return the nodenames with a frozen instance of the specified service.
        """
        return [nodename for (nodename, instance) in \
                self.get_service_instances(svcpath).items() if \
                not instance.get("frozen")]

    def has_instance_with(self, svcpath, global_expect=None):
        """
        Return True if an instance of the specified service is in the
        specified state.
        """
        nodenames = []
        if shared.SMON_DATA.get(svcpath, {}).get("global_expect") in global_expect:
            # relayed smon may no longer have an instance
            return True
        for nodename, instance in self.get_service_instances(svcpath).items():
            if global_expect and instance.get("monitor", {}).get("global_expect") in global_expect:
                return True
        return False

    @staticmethod
    def get_local_svcpaths():
        """
        Extract service instance names from the locally maintained hb data.
        """
        svcpaths = []
        try:
            with shared.CLUSTER_DATA_LOCK:
                for svcpath in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                    if shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcpath]["avail"] == "up":
                        svcpaths.append(svcpath)
        except KeyError:
            return []
        return svcpaths

    @staticmethod
    def get_services_configs():
        """
        Return a hash indexed by svcpath and nodename, containing the services
        configuration mtime and checksum.
        """
        data = {}
        with shared.CLUSTER_DATA_LOCK:
            for nodename, ndata in shared.CLUSTER_DATA.items():
                try:
                    configs = ndata["services"]["config"]
                except (TypeError, KeyError):
                    continue
                for svcpath, config in configs.items():
                    if svcpath not in data:
                        data[svcpath] = {}
                    data[svcpath][nodename] = Storage(config)
        return data

    @staticmethod
    def get_any_service_instance(svcpath):
        """
        Return the specified service status structure on any node.
        """
        with shared.CLUSTER_DATA_LOCK:
            for nodename in shared.CLUSTER_DATA:
                try:
                    if svcpath in shared.CLUSTER_DATA[nodename]["services"]["status"]:
                        if shared.CLUSTER_DATA[nodename]["services"]["status"][svcpath] in (None, ""):
                            continue
                        return shared.CLUSTER_DATA[nodename]["services"]["status"][svcpath]
                except KeyError:
                    continue

    @staticmethod
    def get_last_svc_config(svcpath):
        with shared.CLUSTER_DATA_LOCK:
            try:
                return shared.CLUSTER_DATA[rcEnv.nodename]["services"]["config"][svcpath]
            except KeyError:
                return

    def wait_service_config_consensus(self, svcpath, peers, timeout=60):
        if len(peers) < 2:
            return True
        self.log.info("wait for service %s consensus on config amongst peers %s",
                      svcpath, ",".join(peers))
        for _ in range(timeout):
            if self.service_config_consensus(svcpath, peers):
                return True
            time.sleep(1)
        self.log.error("service %s couldn't reach config consensus in %d seconds",
                       svcpath, timeout)
        return False

    def service_config_consensus(self, svcpath, peers):
        if len(peers) < 2:
            self.log.debug("%s auto consensus. peers: %s", svcpath, peers)
            return True
        ref_csum = None
        for peer in peers:
            if peer not in shared.CLUSTER_DATA:
                # discard unreachable nodes from the consensus
                continue
            try:
                csum = shared.CLUSTER_DATA[peer]["services"]["config"][svcpath]["csum"]
            except KeyError:
                #self.log.debug("service %s peer %s has no config cksum yet", svcpath, peer)
                return False
            except Exception as exc:
                self.log.exception(exc)
                return False
            if ref_csum is None:
                ref_csum = csum
            if ref_csum is not None and ref_csum != csum:
                #self.log.debug("service %s peer %s has a different config cksum", svcpath, peer)
                return False
        self.log.info("service %s config consensus reached", svcpath)
        return True

    def get_services_config(self):
        config = {}
        for svcpath in list_services():
            cfg = svc_pathcf(svcpath)
            try:
                config_mtime = os.path.getmtime(cfg)
            except Exception as exc:
                self.log.warning("failed to get %s mtime: %s", cfg, str(exc))
                config_mtime = 0
            last_config = self.get_last_svc_config(svcpath)
            if last_config is None or config_mtime > last_config["updated"]:
                self.log.debug("compute service %s config checksum", svcpath)
                try:
                    csum = fsum(cfg)
                except (OSError, IOError) as exc:
                    self.log.warning("service %s config checksum error: %s", svcpath, exc)
                    continue
                try:
                    with shared.SERVICES_LOCK:
                        name, namespace, kind = split_svcpath(svcpath)
                        shared.SERVICES[svcpath] = factory(kind)(name, namespace, node=shared.NODE)
                except Exception as exc:
                    self.log.error("%s build error: %s", svcpath, str(exc))
                    continue
            else:
                csum = last_config["csum"]
            if last_config is None or last_config["csum"] != csum:
                if last_config is not None:
                    self.log.info("service %s configuration change" % svcpath)
                try:
                    status_mtime = os.path.getmtime(shared.SERVICES[svcpath].status_data_dump)
                    if config_mtime > status_mtime:
                        self.log.info("service %s refresh instance status older than config", svcpath)
                        self.service_status(svcpath)
                except OSError:
                    pass
            with shared.SERVICES_LOCK:
                scope = sorted(list(shared.SERVICES[svcpath].nodes | shared.SERVICES[svcpath].drpnodes))
            config[svcpath] = {
                "updated": config_mtime,
                "csum": csum,
                "scope": scope,
            }

        # purge deleted services
        with shared.SERVICES_LOCK:
            for svcpath in list(shared.SERVICES.keys()):
                if svcpath not in config:
                    self.log.info("purge deleted service %s", svcpath)
                    del shared.SERVICES[svcpath]
                    try:
                        del shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcpath]
                    except KeyError:
                        pass
        return config

    def get_last_svc_status_mtime(self, svcpath):
        """
        Return the mtime of the specified service configuration file on the
        local node. If unknown, return 0.
        """
        instance = self.get_service_instance(svcpath, rcEnv.nodename)
        if instance is None:
            return 0
        mtime = instance["updated"]
        if mtime is None:
            return 0
        return mtime

    def service_status_fallback(self, svcpath):
        """
        Return the specified service status structure fetched from an execution
        of svcmgr -s <svcpath> json status". As we arrive here when the
        status.json doesn't exist, we don't have to specify --refresh.
        """
        self.log.info("synchronous service status eval: %s", svcpath)
        cmd = ["status", "--refresh"]
        proc = self.service_command(svcpath, cmd, local=False)
        self.push_proc(proc=proc)
        proc.communicate()
        fpath = svc_pathvar(svcpath, "status.json")
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

    def get_services_status(self, svcpaths):
        """
        Return the local services status data, fetching data from status.json
        caches if their mtime changed or from CLUSTER_DATA[rcEnv.nodename] if
        not.

        Also update the monitor 'local_expect' field for each service.
        """

        if shared.NMON_DATA.status == "init":
            return {}

        # purge data cached by the @cache decorator
        purge_cache()

        # this data ends up in CLUSTER_DATA[rcEnv.nodename]["services"]["status"]
        data = {}

        for svcpath in svcpaths:
            idata = None
            last_mtime = self.get_last_svc_status_mtime(svcpath)
            fpath = svc_pathvar(svcpath, "status.json")
            try:
                mtime = os.path.getmtime(fpath)
            except Exception as exc:
                # preserve previous status data if any (an action may be running)
                mtime = 0

            try:
               need_load = mtime > last_mtime + 0.0001
            except TypeError:
               need_load = True

            if need_load:
                # status.json changed
                #  => load
                idata = self.load_instance_status_cache(fpath)

            if not idata and last_mtime > 0:
                # the status.json did not change or failed to load
                #  => preserve current data
                idata = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcpath]

            if idata:
                data[svcpath] = idata
            else:
                self.service_status(svcpath)
                continue

            # update the frozen instance attribute
            with shared.SERVICES_LOCK:
                data[svcpath]["frozen"] = shared.SERVICES[svcpath].frozen()

            # embed the updated smon data
            self.set_smon_l_expect_from_status(data, svcpath)
            data[svcpath]["monitor"] = dict(self.get_service_monitor(svcpath))

            # forget the stonith target node if we run the service
            if data[svcpath].get("avail", "n/a") == "up":
                try:
                    del data[svcpath]["monitor"]["stonith"]
                except KeyError:
                    pass

        # deleting services (still in SMON_DATA, no longer has cf).
        # emulate a status
        for svcpath in set(shared.SMON_DATA.keys()) - set(svcpaths):
            data[svcpath] = {
                "monitor": dict(self.get_service_monitor(svcpath)),
                "resources": {},
            }

        return data

    #########################################################################
    #
    # Service-specific monitor data helpers
    #
    #########################################################################
    @staticmethod
    def reset_smon_retries(svcpath, rid):
        with shared.SMON_DATA_LOCK:
            if svcpath not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[svcpath]:
                return
            if rid in shared.SMON_DATA[svcpath].restart:
                del shared.SMON_DATA[svcpath].restart[rid]
            if len(shared.SMON_DATA[svcpath].restart.keys()) == 0:
                del shared.SMON_DATA[svcpath].restart

    @staticmethod
    def get_smon_retries(svcpath, rid):
        with shared.SMON_DATA_LOCK:
            if svcpath not in shared.SMON_DATA:
                return 0
            if "restart" not in shared.SMON_DATA[svcpath]:
                return 0
            if rid not in shared.SMON_DATA[svcpath].restart:
                return 0
            else:
                return shared.SMON_DATA[svcpath].restart[rid]

    @staticmethod
    def inc_smon_retries(svcpath, rid):
        with shared.SMON_DATA_LOCK:
            if svcpath not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[svcpath]:
                shared.SMON_DATA[svcpath].restart = {}
            if rid not in shared.SMON_DATA[svcpath].restart:
                shared.SMON_DATA[svcpath].restart[rid] = 1
            else:
                shared.SMON_DATA[svcpath].restart[rid] += 1

    def all_nodes_frozen(self):
        with shared.CLUSTER_DATA_LOCK:
             for data in shared.CLUSTER_DATA.values():
                 if not data.get("frozen"):
                     return False
        return True

    def all_nodes_thawed(self):
        with shared.CLUSTER_DATA_LOCK:
             for data in shared.CLUSTER_DATA.values():
                 if data.get("frozen"):
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

    def set_smon_g_expect_from_status(self, svcpath, smon, status):
        """
        Align global_expect with the actual service states.
        """
        if smon.global_expect is None:
            return
        instance = self.get_service_instance(svcpath, rcEnv.nodename)
        if instance is None:
            return
        local_frozen = instance.get("frozen", 0)
        frozen = shared.AGG[svcpath].frozen
        provisioned = shared.AGG[svcpath].provisioned
        deleted = self.get_agg_deleted(svcpath)
        purged = self.get_agg_purged(provisioned, deleted)
        stopped = status in STOPPED_STATES
        if smon.global_expect == "stopped" and stopped and local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcpath, smon.global_expect, status)
            self.set_smon(svcpath, global_expect="unset")
        elif smon.global_expect == "shutdown" and self.get_agg_shutdown(svcpath) and \
           local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", svcpath, smon.global_expect, status)
            self.set_smon(svcpath, global_expect="unset")
        elif smon.global_expect == "started":
            if smon.placement == "none":
                self.set_smon(svcpath, global_expect="unset")
            if status in STARTED_STATES and not local_frozen:
                self.log.info("service %s global expect is %s and its global "
                              "status is %s", svcpath, smon.global_expect, status)
                self.set_smon(svcpath, global_expect="unset")
                return
            if frozen != "thawed":
                return
            svc = self.get_service(svcpath)
            if self.peer_warn(svcpath, with_self=True):
                self.set_smon(svcpath, global_expect="unset")
                return
        elif (smon.global_expect == "frozen" and frozen == "frozen") or \
             (smon.global_expect == "thawed" and frozen == "thawed") or \
             (smon.global_expect == "unprovisioned" and provisioned in (False, "n/a") and stopped):
            self.log.debug("service %s global expect is %s, already is",
                           svcpath, smon.global_expect)
            self.set_smon(svcpath, global_expect="unset")
        elif smon.global_expect == "provisioned" and provisioned in (True, "n/a"):
            if smon.placement == "none":
                self.set_smon(svcpath, global_expect="unset")
            if shared.AGG[svcpath].avail in ("up", "n/a"):
                # provision success, thaw
                self.set_smon(svcpath, global_expect="thawed")
            else:
                self.set_smon(svcpath, global_expect="started")
        elif (smon.global_expect == "purged" and purged is True) or \
             (smon.global_expect == "deleted" and deleted is True):
            self.log.debug("service %s global expect is %s, already is",
                           svcpath, smon.global_expect)
            with shared.SMON_DATA_LOCK:
                del shared.SMON_DATA[svcpath]
        elif smon.global_expect == "aborted" and \
             self.get_agg_aborted(svcpath):
            self.log.info("service %s action aborted", svcpath)
            self.set_smon(svcpath, global_expect="unset")
        elif smon.global_expect == "placed":
            if shared.AGG[svcpath].placement in ("optimal", "n/a") and \
               shared.AGG[svcpath].avail == "up":
                self.set_smon(svcpath, global_expect="unset")
                return
            if frozen != "thawed":
                return
            svc = self.get_service(svcpath)
            if svc is None:
                # foreign
                return
            candidates = self.placement_candidates(svc, discard_start_failed=False, discard_frozen=False)
            candidates = self.placement_leaders(svc, candidates=candidates)
            peers = self.peers_options(svcpath, candidates, ["place failed"])
            if not peers and self.non_leaders_stopped(svcpath, ["place failed"]):
                self.log.info("service %s global expect is %s, not optimal "
                              "and no options left", svcpath, smon.global_expect)
                self.set_smon(svcpath, global_expect="unset")
                return
        elif smon.global_expect.startswith("placed@"):
            target = smon.global_expect.split("@")[-1].split(",")
            if self.instances_started_or_start_failed(svcpath, target):
                self.set_smon(svcpath, global_expect="unset")

    def set_smon_l_expect_from_status(self, data, svcpath):
        if svcpath not in data:
            return
        if data.get(svcpath, {}).get("avail") != "up":
            return
        with shared.SMON_DATA_LOCK:
            if svcpath not in shared.SMON_DATA:
                return
            if shared.SMON_DATA[svcpath].global_expect is not None or \
               shared.SMON_DATA[svcpath].status != "idle" or \
               shared.SMON_DATA[svcpath].local_expect in ("started", "shutdown"):
                return
            self.log.info("service %s monitor local_expect "
                          "%s => %s", svcpath,
                          shared.SMON_DATA[svcpath].local_expect, "started")
            shared.SMON_DATA[svcpath].local_expect = "started"

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

    def purge_left_nodes(self):
        left = set([node for node in shared.CLUSTER_DATA]) - set(self.cluster_nodes)
        for node in left:
            self.log.info("purge left node %s data", node)
            try:
                del shared.CLUSTER_DATA[node]
            except Exception:
                pass

    def update_node_data(self):
        """
        Rescan services config and status.
        """
        data = shared.CLUSTER_DATA[rcEnv.nodename]
        data["stats"] = shared.NODE.stats()
        data["frozen"] = self.freezer.node_frozen()
        data["env"] = shared.NODE.env
        data["labels"] = shared.NODE.labels
        data["targets"] = shared.NODE.targets
        data["locks"] = shared.LOCKS
        data["speaker"] = self.speaker()
        data["min_avail_mem"] = shared.NODE.min_avail_mem
        data["min_avail_swap"] = shared.NODE.min_avail_swap
        data["services"]["config"] = self.get_services_config()
        data["services"]["status"] = self.get_services_status(data["services"]["config"].keys())

        if self.quorum:
            data["arbitrators"] = self.get_arbitrators_data()

        # purge deleted service instances
        for svcpath in set(chain(data["services"]["status"].keys(), shared.SMON_DATA.keys())):
            if svcpath in data["services"]["config"]:
                continue
            try:
                smon = shared.SMON_DATA[svcpath]
                global_expect = smon.get("global_expect")
                global_expect_updated = smon.get("global_expect_updated", 0)
                if global_expect is not None and time.time() < global_expect_updated + 3:
                    # keep the smon around for a while
                    self.log.info("relay foreign service %s global expect %s",
                                  svcpath, global_expect)
                    continue
                else:
                    del shared.SMON_DATA[svcpath]
            except KeyError:
                pass
            try:
                del data["services"]["status"][svcpath]
                self.log.debug("purge deleted service %s from status data", svcpath)
            except KeyError:
                pass

    def update_hb_data(self):
        """
        Prepare the heartbeat data we send to other nodes.
        """

        if self.mon_changed():
            self.update_cluster_data()

        with shared.CLUSTER_DATA_LOCK:
            diff = self._update_hb_data_locked()

        if diff is None:
            return

        shared.EVENT_Q.put({
            "nodename": rcEnv.nodename,
            "kind": "patch",
            "ts": time.time(),
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

    def _update_hb_data_locked(self):
        now = time.time()
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
        return diff

    def merge_hb_data(self):
        self.merge_hb_data_locks()
        self.merge_hb_data_compat()
        self.merge_hb_data_monitor()

    def merge_hb_data_locks(self):
        with shared.LOCKS_LOCK:
            self._merge_hb_data_locks()

    def _merge_hb_data_locks(self):
        for nodename, node in shared.CLUSTER_DATA.items():
            if nodename == rcEnv.nodename:
                continue
            for name, lock in node.get("locks", {}).items():
                if lock["requester"] == rcEnv.nodename and name not in shared.LOCKS:
                    # don't re-merge a released lock emitted by this node
                    continue
                if name not in shared.LOCKS:
                    self.log.info("merge lock %s from node %s", name, nodename)
                    shared.LOCKS[name] = lock
                    continue
                if lock["requested"] < shared.LOCKS[name]["requested"] and \
                   lock["requester"] != rcEnv.nodename and \
                   lock["requester"] == nodename:
                    self.log.info("merge older lock %s from node %s", name, nodename)
                    shared.LOCKS[name] = lock
                    continue
        delete = []
        for name, lock in shared.LOCKS.items():
            if rcEnv.nodename == lock["requester"]:
                continue
            if shared.CLUSTER_DATA.get(lock["requester"], {}).get("locks", {}).get(name) is None:
                self.log.info("drop lock %s from node %s", name, nodename)
                delete.append(name)
        for name in delete:
            del shared.LOCKS[name]

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
                local_frozen = shared.CLUSTER_DATA[rcEnv.nodename].get("frozen", 0)
                if (global_expect == "frozen" and not local_frozen) or \
                   (global_expect == "thawed" and local_frozen):
                    self.log.info("node %s wants local node %s", nodename, global_expect)
                    self.set_nmon(global_expect=global_expect)
                #else:
                #    self.log.info("node %s wants local node %s, already is", nodename, global_expect)

            # merge every service monitors
            for svcpath, instance in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"].items():
                if instance is None:
                    continue
                current_global_expect = instance["monitor"].get("global_expect")
                if current_global_expect == "aborted":
                    # refuse a new global expect if aborting
                    continue
                current_global_expect_updated = instance["monitor"].get("global_expect_updated")
                for nodename in nodenames:
                    rinstance = self.get_service_instance(svcpath, nodename)
                    if rinstance is None:
                        continue
                    if rinstance.get("stonith") is True and \
                       instance["monitor"].get("stonith") != nodename:
                        self.set_smon(svcpath, stonith=nodename)
                    global_expect = rinstance["monitor"].get("global_expect")
                    if global_expect is None:
                        continue
                    global_expect_updated = rinstance["monitor"].get("global_expect_updated")
                    if current_global_expect and global_expect_updated and \
                       current_global_expect_updated and \
                       global_expect_updated < current_global_expect_updated:
                        # we have a more recent update
                        continue
                    if svcpath in shared.SERVICES and shared.SERVICES[svcpath].disabled and \
                       global_expect not in ("frozen", "thawed", "aborted", "deleted", "purged"):
                        continue
                    if global_expect == current_global_expect:
                        self.log.debug("node %s wants service %s %s, already targeting that",
                                       nodename, svcpath, global_expect)
                        continue
                    #else:
                    #    self.log.info("node %s wants service %s %s, already is", nodename, svcpath, global_expect)
                    if self.accept_g_expect(svcpath, instance, global_expect):
                        self.log.info("node %s wants service %s %s", nodename, svcpath, global_expect)
                        self.set_smon(svcpath, global_expect=global_expect)

    def accept_g_expect(self, svcpath, instance, global_expect):
        if svcpath in shared.AGG:
            agg = shared.AGG[svcpath]
        else:
            agg = Storage()
        smon = self.get_service_monitor(svcpath)
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
            return not self.get_agg_shutdown(svcpath)
        elif global_expect == "started":
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
            deleted = self.get_agg_deleted(svcpath)
            if deleted is False:
                return True
            else:
                return False
        elif global_expect == "purged":
            if smon.placement == "none":
                return False
            deleted = self.get_agg_deleted(svcpath)
            purged = self.get_agg_purged(agg.provisioned, deleted)
            if purged is False:
                return True
            else:
                return False
        elif global_expect == "aborted":
            aborted = self.get_agg_aborted(svcpath)
            if aborted is False:
                return True
            else:
                return False
        elif global_expect == "placed":
            if smon.placement == "none":
                return False
            if agg.placement == "non-optimal" or agg.avail != "up" or agg.frozen == "frozen":
                svc = shared.SERVICES.get(svcpath)
                if svc is None:
                    return True
                candidates = self.placement_candidates(svc, discard_start_failed=False, discard_frozen=False)
                candidates = self.placement_leaders(svc, candidates=candidates)
                peers = self.peers_options(svcpath, candidates, ["place failed"])
                if not peers and self.non_leaders_stopped(svcpath, ["place failed"]):
                    return False
                return True
            else:
                return False
        elif global_expect.startswith("placed@"):
            target = global_expect.split("@")[-1].split(",")
            if rcEnv.nodename in target:
                if instance["avail"] in STOPPED_STATES:
                    return True
            else:
                if instance["avail"] not in STOPPED_STATES:
                    return True
        return False

    def instance_provisioned(self, instance):
        if instance is None:
            return False
        return instance.get("provisioned", True)

    def instance_unprovisioned(self, instance):
        if instance is None:
            return True
        for resource in instance.get("resources", {}).values():
            if resource.get("type") in ("disk.scsireserv", "task", "task.docker", "task.podman"):
                # always provisioned
                continue
            if resource.get("provisioned", {}).get("state") is True:
                return False
        return not instance.get("provisioned", False)

    def get_agg(self, svcpath):
        data = Storage()
        data.avail = self.get_agg_avail(svcpath)
        data.frozen = self.get_agg_frozen(svcpath)
        data.overall = self.get_agg_overall(svcpath)
        data.placement = self.get_agg_placement(svcpath)
        data.provisioned = self.get_agg_provisioned(svcpath)
        return data

    def get_agg_services(self, paths=None):
        svcpaths = set()
        with shared.CLUSTER_DATA_LOCK:
            for nodename, data in shared.CLUSTER_DATA.items():
                try:
                    for svcpath in data["services"]["config"]:
                        svcpaths.add(svcpath)
                except KeyError:
                    continue
        data = {}
        for svcpath in svcpaths:
            try:
                if self.get_service(svcpath).topology == "span":
                    data[svcpath] = Storage()
                    continue
            except Exception as exc:
                data[svcpath] = Storage()
                pass
            data[svcpath] = self.get_agg(svcpath)
        with shared.AGG_LOCK:
            shared.AGG = data
        if paths is not None:
            return {path: data[path] for path in svcpaths if path in paths}
        return data

    def update_completions(self):
        self.update_completion("services")
        self.update_completion("nodes")

    def update_completion(self, otype):
        try:
            if otype == "services":
                olist = [path for path in shared.AGG]
            else:
                olist = [path for path in shared.CLUSTER_DATA]
            with open(os.path.join(rcEnv.paths.pathvar, "list."+otype), "w") as filep:
                filep.write("\n".join(olist)+"\n")
        except Exception as exc:
            print(exc)
            pass

    def filter_cluster_data(self, paths=None):
        with shared.CLUSTER_DATA_LOCK:
            data = copy.deepcopy(shared.CLUSTER_DATA)
        if paths is None:
            return data
        nodes = list([n for n in data])
        for node in data:
            cpaths = [p for p in data[node]["services"]["config"] if p in paths]
            data[node]["services"]["config"] = {path: data[node]["services"]["config"][path] for path in cpaths}
            spaths = [p for p in data[node]["services"]["status"] if p in paths]
            data[node]["services"]["status"] = {path: data[node]["services"]["status"][path] for path in spaths}
        return data

    def status(self, **kwargs):
        if kwargs.get("refresh"):
            self.update_hb_data()
        namespaces = kwargs.get("namespaces")
        if namespaces is None:
            paths = None
        else:
            paths = [p for p in shared.SMON_DATA if split_svcpath(p)[1] in namespaces]
        data = shared.OsvcThread.status(self, **kwargs)
        data["nodes"] = self.filter_cluster_data(paths)
        data["compat"] = self.compat
        data["transitions"] = self.transition_count()
        data["frozen"] = self.get_clu_agg_frozen()
        data["services"] = self.get_agg_services(paths)
        return data

    def get_last_shutdown(self):
        try:
            return os.path.getmtime(rcEnv.paths.last_shutdown)
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
            node = shared.CLUSTER_DATA[rcEnv.nodename]
        except:
            return
        frozen = node.get("frozen", 0)
        if frozen:
            return
        if self.freezer.node_frozen():
            return
        nmon = self.get_node_monitor()
        if nmon.global_expect == "thawed":
            return
        for peer in self.cluster_nodes:
            if peer == rcEnv.nodename:
                continue
            try:
                node = shared.CLUSTER_DATA[peer]
            except:
                continue
            frozen = node.get("frozen", 0)
            if not isinstance(frozen, float):
                continue
            if frozen and frozen > last_shutdown:
                self.event("node_freeze", data={
                    "reason": "merge_frozen",
                    "peer": peer,
                })
                self.freezer.node_freeze()

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
            try:
                instance = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.svcpath]
            except:
                continue
            frozen = instance.get("frozen", 0)
            if frozen:
                continue
            if self.instance_frozen(svc.svcpath):
                continue
            smon = self.get_service_monitor(svc.svcpath)
            if smon.global_expect == "thawed":
                continue
            for peer in svc.peers:
                if peer == rcEnv.nodename:
                    continue
                try:
                    instance = shared.CLUSTER_DATA[peer]["services"]["status"][svc.svcpath]
                except:
                    continue
                frozen = instance.get("frozen", 0)
                if not isinstance(frozen, float):
                    continue
                if frozen > last_shutdown:
                    self.event("instance_freeze", data={
                        "reason": "merge_frozen",
                        "peer": peer,
                        "svcpath": svc.svcpath,
                    })
                    svc.freezer.freeze()

    def instance_frozen(self, svcpath, nodename=None):
        if not nodename:
            nodename = rcEnv.nodename
        try:
            return shared.CLUSTER_DATA[nodename]["services"]["status"][svcpath].get("frozen", 0)
        except:
            return 0

    def kern_freeze(self):
        try:
            with open("/proc/cmdline", "r") as ofile:
                buff = ofile.read()
        except Exception:
            return
        if "osvc.freeze" in buff.split():
            self.event("node_freeze", data={"reason": "kern_freeze"})
            self.freezer.node_freeze()
