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

import daemon.shared as shared
import rcExceptions as ex
import json_delta
from rcGlobalEnv import rcEnv
from storage import Storage
from rcUtilities import bdecode, purge_cache, fsum, \
                        svc_pathetc, svc_pathvar, makedirs, split_path, \
                        list_services, svc_pathcf, fmt_path, \
                        resolve_path, factory
from freezer import Freezer

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

ETC_NS_SKIP = len(os.path.join(rcEnv.paths.pathetcns, ""))

class Monitor(shared.OsvcThread):
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

    def init(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(rcEnv.nodename+".osvcd.monitor"), {"node": rcEnv.nodename, "component": self.name})
        self.event("monitor_started")
        self.startup = time.time()
        self.rejoin_grace_period_expired = False
        self.shortloops = 0
        self.unfreeze_when_all_nodes_joined = False
        self.node_frozen = self.freezer.node_frozen()

        shared.CLUSTER_DATA[rcEnv.nodename] = {
            "compat": shared.COMPAT_VERSION,
            "api": shared.API_VERSION,
            "agent": shared.NODE.agent_version,
            "monitor": dict(shared.NMON_DATA),
            "labels": shared.NODE.labels,
            "targets": shared.NODE.targets,
            "services": {},
        }

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
        for path in list(shared.SMON_DATA):
            try:
                data = shared.SMON_DATA[path]
            except KeyError:
                continue
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
        shared.NODE.unset_lazy("labels")
        shared.CLUSTER_DATA[rcEnv.nodename]["labels"] = shared.NODE.labels
        self.on_nodes_info_change()
        for path in shared.SERVICES:
            try:
                name, namespace, kind = split_path(path)
                svc = factory(kind)(name, namespace, node=shared.NODE)
            except Exception as exc:
                continue
            with shared.SERVICES_LOCK:
                shared.SERVICES[path] = svc

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
            new_service = False
            with shared.SERVICES_LOCK:
                if path not in shared.SERVICES:
                    new_service = True
            if self.has_instance_with(path, global_expect=["purged", "deleted"]):
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
                if new_service and rcEnv.nodename not in conf.get("scope", []):
                    # we are not a service node
                    continue
                if conf.csum != ref_conf.csum and \
                   conf.updated > ref_conf.updated:
                    ref_conf = conf
                    ref_nodename = nodename
            if not new_service and ref_conf.scope and rcEnv.nodename not in ref_conf.scope:
                smon = self.get_service_monitor(path)
                if not smon or smon.status == "deleting":
                    continue
                self.log.info("node %s has the most recent %s config, "
                              "which no longer defines %s as a node.",
                              ref_nodename, path, rcEnv.nodename)
                #self.event("instance_stop", {
                #    "reason": "relayout",
                #    "path": svc.path,
                #})
                #self.service_stop(path, force=True)
                self.service_delete(path)
                continue
            if ref_nodename == rcEnv.nodename:
                # we already have the most recent version
                continue
            with shared.SERVICES_LOCK:
                if path in shared.SERVICES and \
                   rcEnv.nodename in shared.SERVICES[path].nodes and \
                   ref_nodename in shared.SERVICES[path].drpnodes:
                    # don't fetch drp config from prd nodes
                    continue
            self.log.info("node %s has the most recent %s config",
                          ref_nodename, path)
            self.fetch_service_config(path, ref_nodename)
            if new_service:
                self.init_new_service(path)
            else:
                self.service_status_fallback(path)

    def init_new_service(self, path):
        name, namespace, kind = split_path(path)
        try:
            shared.SERVICES[path] = factory(kind)(name, namespace, node=shared.NODE)
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
        with tempfile.NamedTemporaryFile(dir=rcEnv.paths.pathtmp, delete=False) as filep:
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
                try:
                    svc.postinstall()
                except Exception as exc:
                    self.log.error("service %s postinstall failed: %s", path, exc)
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
        with shared.SMON_DATA_LOCK:
             for path, smon in shared.SMON_DATA.items():
                 if smon.stonith == node:
                     del shared.SMON_DATA[path]["stonith"]

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
            instance = self.get_service_instance(path, rcEnv.nodename)
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
        if rcEnv.nodename not in shared.SERVICES[path].nodes:
            self.log.info("skip status refresh on %s (foreign)", path)
            return
        smon = self.get_service_monitor(path)
        if smon.status and smon.status.endswith("ing"):
            # no need to run status, the running action will refresh the status earlier
            return
        cmd = ["status", "--refresh", "--waitlock=0"]
        if self.has_proc(cmd):
            # no need to run status twice
            return
        proc = self.service_command(path, cmd, local=False)
        self.push_proc(
            proc=proc,
            cmd=cmd,
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
            candidates = self.placement_candidates(svc, discard_frozen=False,
                                                   discard_na=False,
                                                   discard_overloaded=False,
                                                   discard_unprovisioned=False,
                                                   discard_constraints_violation=False)
            leader = self.placement_leader(svc, candidates)
        else:
            leader = rcEnv.nodename == leader
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
        candidates = self.placement_candidates(svc, discard_frozen=False,
                                               discard_na=False,
                                               discard_overloaded=False,
                                               discard_unprovisioned=False,
                                               discard_constraints_violation=False)
        cmd = ["provision"]
        if self.placement_leader(svc, candidates):
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

    def service_unprovision(self, svc, leader=None):
        self.set_smon(svc.path, "unprovisioning", local_expect="unset")
        if leader is None:
            candidates = self.placement_candidates(svc, discard_frozen=False,
                                                   discard_na=False,
                                                   discard_overloaded=False,
                                                   discard_unprovisioned=False,
                                                   discard_constraints_violation=False)
            leader = self.placement_leader(svc, candidates)
        else:
            leader = rcEnv.nodename == leader
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
            global_expect = shared.SMON_DATA.get(path, {}).get("global_expect")
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
        data = {path: data}
        cmd = ["create", "--config=-"]
        if svc.namespace:
            cmd += ["--namespace=%s" % svc.namespace]
        proc = self.service_command(None, cmd, stdin=json.dumps(data))
        out, err = proc.communicate()
        if proc.returncode != 0:
            self.set_smon(path, "create failed")
        self.service_status_fallback(path)

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

    def services_purge_status(self, paths=None):
        paths = paths or list_services()
        for path in paths:
            fpath = svc_pathvar(path, "status.json")
            try:
                os.unlink(fpath)
            except Exception as exc:
                pass

    def services_init_status(self):
        svcs = list_services()
        if not svcs:
            self.log.info("no objects to get an initial status from")
            self.services_init_status_callback()
            return
        self.services_purge_status(paths=svcs)
        proc = self.service_command(",".join(svcs), ["status", "--parallel", "--refresh"], local=False)
        self.push_proc(
            proc=proc,
            on_success="services_init_status_callback",
            on_error="services_init_status_callback",
        )

    def services_init_boot(self):
        paths = list_services(kinds=["vol", "svc"])
        self.services_purge_status(paths=paths)
        proc = self.service_command(",".join(paths), ["boot", "--parallel"])
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

        if self.missing_beating_peer_data():
            # just after a split+rejoin, we don't have the peers full dataset
            # even if all hb rx are reporting beating. Avoid taking decisions
            # during this transient period.
            return

        # node
        self.node_orchestrator()

        # services (iterate over deleting services too)
        paths = [path for path in shared.SMON_DATA]
        self.get_agg_services()
        for path in paths:
            self.clear_start_failed(path)
            if self.transitions_maxed():
                break
            if self.status_older_than_cf(path):
                #self.log.info("%s status dump is older than its config file",
                #              path)
                self.service_status(path)
                continue
            svc = self.get_service(path)
            self.resources_orchestrator(path, svc)
            self.service_orchestrator(path, svc)
        self.sync_services_conf()

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
        if shared.NMON_DATA.status == "shutting":
            return
        if svc is None:
            return
        if self.node_frozen or self.instance_frozen(path):
            #self.log.info("resource %s orchestrator out (frozen)", svc.path)
            return
        if svc.disabled:
            #self.log.info("resource %s orchestrator out (disabled)", svc.path)
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
                    if candidates != [rcEnv.nodename] and len(candidates) > 0:
                        self.event("resource_toc", {
                            "path": svc.path,
                            "rid": rid,
                            "resource": resource,
                        })
                        if shared.SMON_DATA.get(path, {}).get("status") != "tocing":
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
            nb_restart = svc.get_resource(rid, with_encap=True).nb_restart
            if nb_restart < self.default_stdby_nb_restart:
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

        try:
            with shared.CLUSTER_DATA_LOCK:
                instance = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.path]
                if instance.get("encap") is True:
                    return
                resources = instance.get("resources", {})
        except KeyError:
            return

        mon_rids = []
        stdby_rids = []
        for rid, resource in resources.items():
            if resource["status"] not in ("warn", "down", "stdby down"):
                self.reset_smon_retries(svc.path, rid)
                continue
            if resource.get("provisioned", {}).get("state") is False:
                continue
            if monitored_resource(svc, rid, resource):
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
                if monitored_resource(svc, rid, resource):
                    mon_rids.append(rid)
                elif stdby_resource(svc, rid, resource):
                    stdby_rids.append(rid)
            if len(mon_rids) > 0:
                self.service_start_resources(svc.path, mon_rids, slave=crid)
            if len(stdby_rids) > 0:
                self.service_startstandby_resources(svc.path, stdby_rids, slave=crid)

    def node_orchestrator(self):
        if shared.NMON_DATA.status == "shutting":
            return
        self.orchestrator_auto_grace()
        nmon = self.get_node_monitor()
        if self.unfreeze_when_all_nodes_joined and self.node_frozen and len(self.cluster_nodes) == len(shared.CLUSTER_DATA):
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

    def service_orchestrator(self, path, svc):
        smon = self.get_service_monitor(path)
        if svc is None:
            if smon and path in shared.AGG:
                # deleting service: unset global expect if done cluster-wide
                status = shared.AGG[path].avail
                self.set_smon_g_expect_from_status(path, smon, status)
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
        try:
            status = shared.AGG[svc.path].avail
        except KeyError:
            return
        self.set_smon_g_expect_from_status(svc.path, smon, status)
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
    def scale_path(path, idx):
        name, namespace, kind = split_path(path)
        return fmt_path(str(idx)+"."+name, namespace, kind)

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
            #self.log.info("service %s orchestrator out (disabled)", svc.path)
            return
        if not self.compat:
            return
        if svc.topology == "failover" and smon.local_expect == "started":
            # decide if the service local_expect=started should be reset
            if status == "up" and self.get_service_instance(svc.path, rcEnv.nodename).avail != "up":
                self.log.info("service '%s' is globally up but the local instance is "
                              "not and is in 'started' local expect. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            elif self.service_started_instances_count(svc.path) > 1 and \
                 self.get_service_instance(svc.path, rcEnv.nodename).avail != "up" and \
                 not self.placement_leader(svc):
                self.log.info("service '%s' has multiple instance in 'started' "
                              "local expect and we are not leader. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            elif status != "up" and \
                 self.get_service_instance(svc.path, rcEnv.nodename).avail in ("down", "stdby down", "undef", "n/a") and \
                 not self.resources_orchestrator_will_handle(svc):
                self.log.info("service '%s' is not up and no resource monitor "
                              "action will be attempted, but "
                              "is in 'started' local expect. reset",
                              svc.path)
                self.set_smon(svc.path, local_expect="unset")
            else:
                return
        if self.node_frozen or self.instance_frozen(svc.path):
            #self.log.info("service %s orchestrator out (frozen)", svc.path)
            return
        if not self.rejoin_grace_period_expired:
            return
        if svc.scale_target is not None and smon.global_expect is None:
            self.service_orchestrator_scaler(svc)
            return
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)",
            #              svc.path, status)
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
                    self.set_smon(svc.path, "idle")
                # not natural leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.path, rcEnv.nodename)
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
                if smon.stonith and smon.stonith not in shared.CLUSTER_DATA:
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

    def service_orchestrator_auto_flex(self, svc, smon, status, candidates):
        if svc.orchestrate == "start":
            ranks = self.placement_ranks(svc, candidates=svc.peers)
            if ranks == []:
                return
            try:
                idx = ranks.index(rcEnv.nodename)
            except ValueError:
                return
            if rcEnv.nodename not in ranks[:svc.flex_target]:
                # after loosing the placement leader status, the smon state
                # may need a reset
                if smon.status in ("ready", "wait parents"):
                    self.set_smon(svc.path, "idle")
                # natural not a leader, skip orchestration
                return
            # natural leader, let orchestration unroll
        instance = self.get_service_instance(svc.path, rcEnv.nodename)
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
                self.log.info("service %s will start in %d seconds",
                              svc.path, tmo)
                self.set_next(tmo)
        elif smon.status == "idle":
            if svc.orchestrate == "no" and smon.global_expect not in ("started", "placed"):
                return
            if n_up < svc.flex_target:
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
                self.log.info("flex service %s started, starting or ready to "
                              "start instances: %d/%d. local status %s",
                              svc.path, n_up, svc.flex_target,
                              instance.avail)
                self.set_smon(svc.path, "ready")
            elif n_up > svc.flex_target:
                if instance is None:
                    return
                if instance.avail not in STARTED_STATES:
                    return
                n_to_stop = n_up - svc.flex_target
                overloaded_up_nodes = self.overloaded_up_service_instances(svc.path)
                to_stop = self.placement_ranks(svc, candidates=overloaded_up_nodes)[-n_to_stop:]
                n_to_stop -= len(to_stop)
                if n_to_stop > 0:
                    to_stop += self.placement_ranks(svc, candidates=set(up_nodes)-set(overloaded_up_nodes))[-n_to_stop:]
                self.log.info("%d nodes to stop to honor service %s "
                              "flex_target=%d. choose %s",
                              n_to_stop, svc.path, svc.flex_target,
                              ", ".join(to_stop))
                if rcEnv.nodename not in to_stop:
                    return
                self.event("instance_stop", {
                    "reason": "flex_threshold",
                    "path": svc.path,
                    "up": n_up,
                })
                self.service_stop(svc.path)

    def service_orchestrator_shutting(self, svc, smon, status):
        """
        Take actions to shutdown all local services instances marked with
        local_expect == "shutdown", even if frozen.

        Honor parents/children sequencing.
        """
        instance = self.get_service_instance(svc.path, rcEnv.nodename)
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

    def service_orchestrator_manual(self, svc, smon, status):
        """
        Take actions to meet global expect target, set by user or by
        service_orchestrator_auto()
        """
        instance = self.get_service_instance(svc.path, rcEnv.nodename)
        if smon.global_expect == "frozen":
            if not self.instance_frozen(svc.path):
                self.event("instance_freeze", {
                    "reason": "target",
                    "path": svc.path,
                    "monitor": smon,
                })
                self.service_freeze(svc.path)
        elif smon.global_expect == "thawed":
            if self.instance_frozen(svc.path):
                self.event("instance_thaw", {
                    "reason": "target",
                    "path": svc.path,
                    "monitor": smon,
                })
                self.service_thaw(svc.path)
        elif smon.global_expect == "shutdown":
            if not self.children_down(svc):
                self.set_smon(svc.path, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")

            if not self.instance_frozen(svc.path):
                self.event("instance_freeze", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_freeze(svc.path)
            elif not self.is_instance_shutdown(instance):
                thawed_on = self.service_instances_thawed(svc.path)
                if thawed_on:
                    self.duplog("info", "service %(path)s still has thawed "
                                "instances on nodes %(thawed_on)s, delay "
                                "shutdown",
                                path=svc.path,
                                thawed_on=", ".join(thawed_on))
                else:
                    self.service_shutdown(svc.path)
        elif smon.global_expect == "stopped":
            if not self.children_down(svc):
                self.set_smon(svc.path, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")

            if not self.instance_frozen(svc.path):
                self.log.info("freeze service %s", svc.path)
                self.event("instance_freeze", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_freeze(svc.path)
            elif instance.avail not in STOPPED_STATES:
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
        elif smon.global_expect == "started":
            if not self.parents_available(svc):
                self.set_smon(svc.path, status="wait parents")
                return
            elif smon.status == "wait parents":
                self.set_smon(svc.path, status="idle")
            if self.instance_frozen(svc.path):
                self.event("instance_thaw", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_thaw(svc.path)
            elif status not in STARTED_STATES:
                if shared.AGG[svc.path].frozen != "thawed":
                    return
                self.service_orchestrator_auto(svc, smon, status)
        elif smon.global_expect == "unprovisioned":
            if smon.status in ("unprovisioning", "stopping"):
                return
            if svc.path not in shared.SERVICES or self.instance_unprovisioned(instance):
                if smon.status != "idle":
                    self.set_smon(svc.path, status="idle")
                return
            if not self.children_unprovisioned(svc):
                self.set_smon(svc.path, status="wait children")
                return
            if instance.avail not in STOPPED_STATES:
                self.event("instance_stop", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_stop(svc.path, force=True)
                return
            if shared.AGG[svc.path].avail not in STOPPED_STATES:
                return
            if smon.status == "wait children":
                if not self.children_unprovisioned(svc):
                    return
            elif smon.status == "wait non-leader":
                if not self.leader_last(svc, provisioned=False, silent=True):
                    self.log.info("service %s still waiting non leaders", svc.path)
                    return
            leader = self.leader_last(svc, provisioned=False)
            if not leader:
                self.set_smon(svc.path, status="wait non-leader")
                return
            self.event("instance_unprovision", {
                "reason": "target",
                "path": svc.path,
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
                self.set_smon(svc.path, status="idle")
                return
            if not self.min_instances_reached(svc):
                self.set_smon(svc.path, status="wait sync")
                return
            if not self.parents_available(svc):
                self.set_smon(svc.path, status="wait parents")
                return
            if not self.leader_first(svc, provisioned=True):
                self.set_smon(svc.path, status="wait leader")
                return
            self.event("instance_provision", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_provision(svc)
        elif smon.global_expect == "deleted":
            if not self.children_down(svc):
                self.set_smon(svc.path, status="wait children")
                return
            elif smon.status == "wait children":
                self.set_smon(svc.path, status="idle")
            if svc.path in shared.SERVICES:
                self.event("instance_delete", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_delete(svc.path)
        elif smon.global_expect == "purged":
            if smon.status in ("purging", "deleting", "stopping"):
                return
            if not self.children_unprovisioned(svc):
                self.set_smon(svc.path, status="wait children")
                return
            if instance.avail not in STOPPED_STATES:
                self.event("instance_stop", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_stop(svc.path, force=True)
                return
            if shared.AGG[svc.path].avail not in STOPPED_STATES:
                return
            if smon.status == "wait children":
                if not self.children_unprovisioned(svc):
                    return
            elif smon.status == "wait non-leader":
                if not self.leader_last(svc, provisioned=False, silent=True, deleted=True):
                    return
            leader = self.leader_last(svc, provisioned=False, deleted=True)
            if not leader:
                self.set_smon(svc.path, status="wait non-leader")
                return
            if svc.path in shared.SERVICES and svc.kind not in ("vol", "svc"):
                # base services do not implement the purge action
                self.event("instance_delete", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_delete(svc.path)
                return
            if svc.path not in shared.SERVICES or not instance:
                if smon.status != "idle":
                    self.set_smon(svc.path, status="idle")
                return
            self.event("instance_purge", {
                "reason": "target",
                "path": svc.path,
            })
            self.service_purge(svc, leader)
        elif smon.global_expect == "aborted" and \
             smon.local_expect not in (None, "started"):
            self.event("instance_abort", {
                "reason": "target",
                "path": svc.path,
            })
            self.set_smon(svc.path, local_expect="unset")
        elif smon.global_expect == "placed":
            # refresh smon for placement attr change caused by a clear
            smon = self.get_service_monitor(svc.path)
            if self.instance_frozen(svc.path):
                self.event("instance_thaw", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_thaw(svc.path)
            elif smon.placement != "leader":
                if not self.has_leader(svc):
                    # avoid stopping the instance if no peer node can takeover
                    return
                if instance.avail not in STOPPED_STATES:
                    self.event("instance_stop", {
                        "reason": "target",
                        "path": svc.path,
                    })
                    self.service_stop(svc.path)
            elif self.non_leaders_stopped(svc.path) and \
                 (shared.AGG[svc.path].placement not in ("optimal", "n/a") or shared.AGG[svc.path].avail != "up") and \
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
            if self.instance_frozen(svc.path):
                self.event("instance_thaw", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_thaw(svc.path)
            elif rcEnv.nodename not in target:
                if smon.status == "stop failed":
                    return
                if instance.avail not in STOPPED_STATES and (set(target) & set(candidates)):
                    self.event("instance_stop", {
                        "reason": "target",
                        "path": svc.path,
                    })
                    self.service_stop(svc.path)
            elif self.instances_stopped(svc.path, set(svc.peers) - set(target)) and \
                 rcEnv.nodename in target and \
                 instance.avail in STOPPED_STATES + ["warn"]:
                if smon.status in ("start failed", "place failed"):
                    return
                self.event("instance_start", {
                    "reason": "target",
                    "path": svc.path,
                })
                self.service_start(svc.path, err_status="place failed" if smon.global_expect == "placed" else "start failed")

    def scaler_current_slaves(self, path):
        name, namespace, kind = split_path(path)
        pattern = "[0-9]+\." + name + "$"
        if namespace:
            pattern = "^%s/%s/%s" % (namespace, kind, pattern)
        else:
            pattern = "^%s" % pattern
        return [slave for slave in shared.SERVICES if re.match(pattern, slave)]

    def service_orchestrator_scaler(self, svc):
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
        if ranks[0] != rcEnv.nodename:
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
            self.service_orchestrator_scaler_up(svc, missing, current_slaves)
        else:
            self.event("scale_down", {
               "path": svc.path,
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
        self.log.info("scale service %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.path, exc)

    def service_orchestrator_scaler_down_flex(self, svc, missing, current_slaves):
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
        self.log.info("scale service %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.path, exc)

    @staticmethod
    def sort_scaler_slaves(slaves, reverse=False):
        return sorted(slaves, key=lambda x: int(x.split("/")[-1].split(".")[0]), reverse=reverse)

    def service_orchestrator_scaler_up_failover(self, svc, missing, current_slaves):
        slaves_count = missing
        n_current_slaves = len(current_slaves)
        new_slaves_list = [self.scale_path(svc.path, n_current_slaves+idx) for idx in range(slaves_count)]

        to_add = self.sort_scaler_slaves(new_slaves_list)
        to_add = [[path, None] for path in to_add]
        delta = "add " + ",".join([elem[0] for elem in to_add])
        self.log.info("scale service %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, to_add, []))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.path, exc)

    def service_orchestrator_scaler_down_failover(self, svc, missing, current_slaves):
        slaves_count = -missing
        n_current_slaves = len(current_slaves)
        slaves_list = [self.scale_path(svc.path, n_current_slaves-1-idx) for idx in range(slaves_count)]

        to_remove = self.sort_scaler_slaves(slaves_list)
        to_remove = [path for path in to_remove]
        delta = "delete " + ",".join([elem[0] for elem in to_remove])
        self.log.info("scale service %s: %s", svc.path, delta)
        self.set_smon(svc.path, status="scaling")
        try:
            thr = threading.Thread(target=self.scaling_worker, args=(svc, [], to_remove))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning("failed to start a scaling thread for service "
                             "%s: %s", svc.path, exc)

    def scaling_worker(self, svc, to_add, to_remove):
        threads = []
        for path, instances in to_add:
            if path in shared.SERVICES:
                continue
            data = svc.print_config_data()
            try:
                thr = threading.Thread(
                    target=self.service_create_scaler_slave,
                    args=(path, svc, data, instances)
                )
                thr.start()
                threads.append(thr)
            except RuntimeError as exc:
                self.log.warning("failed to start a scaling thread for "
                                 "service %s: %s", svc.path, exc)
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

    def pass_hard_affinities(self, svc):
        if svc.hard_anti_affinity:
            intersection = set(self.get_local_paths()) & set(svc.hard_anti_affinity)
            if len(intersection) > 0:
                #self.log.info("service %s orchestrator out (hard anti-affinity with %s)",
                #              svc.path, ','.join(intersection))
                return False
        if svc.hard_affinity:
            intersection = set(self.get_local_paths()) & set(svc.hard_affinity)
            if len(intersection) < len(set(svc.hard_affinity)):
                #self.log.info("service %s orchestrator out (hard affinity with %s)",
                #              svc.path, ','.join(intersection))
                return False
        return True

    def pass_soft_affinities(self, svc, candidates):
        if candidates != [rcEnv.nodename]:
            # the local node is not the only candidate, we can apply soft
            # affinity filtering
            if svc.soft_anti_affinity:
                intersection = set(self.get_local_paths()) & set(svc.soft_anti_affinity)
                if len(intersection) > 0:
                    #self.log.info("service %s orchestrator out (soft anti-affinity with %s)",
                    #              svc.path, ','.join(intersection))
                    return False
            if svc.soft_affinity:
                intersection = set(self.get_local_paths()) & set(svc.soft_affinity)
                if len(intersection) < len(set(svc.soft_affinity)):
                    #self.log.info("service %s orchestrator out (soft affinity with %s)",
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
            self.node_frozen = self.freezer.node_frozen()
            return False
        self.duplog("info", "in rejoin grace period", nodename="")
        return True

    def clear_start_failed(self, path):
        try:
            avail = shared.AGG[path].avail
        except KeyError:
            avail = "unknown"
        if avail != "up":
            return
        smon = self.get_service_monitor(path)
        if not smon:
            return
        if smon.global_expect is not None:
            return
        if smon.status not in ("start failed", "place failed"):
            return
        self.log.info("clear %s %s: the service is up", path, smon.status)
        self.set_smon(path, status="idle")

    def local_children_down(self, svc):
        missing = []
        if len(svc.children_and_slaves) == 0:
            return True
        for child in svc.children_and_slaves:
            if child == svc.path:
                continue
            instance = self.get_service_instance(child, rcEnv.nodename)
            if not instance:
                continue
            avail = instance.get("avail", "unknown")
            if avail in STOPPED_STATES + ["unknown"]:
                continue
            missing.append(child)
        if len(missing) == 0:
            self.duplog("info", "service %(path)s local children all avail down",
                        path=svc.path)
            return True
        self.duplog("info", "service %(path)s local children still available:"
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
            self.duplog("info", "service %(path)s children all %(state)s:"
                        " %(children)s", path=svc.path, state=state,
                        children=" ".join(svc.children_and_slaves))
            return True
        state = "available"
        if unprovisioned:
            state += " or provisioned"
        self.duplog("info", "service %(path)s children still %(state)s:"
                    " %(missing)s", path=svc.path, state=state,
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
            parent = resolve_path(parent, svc.namespace)
            if parent == svc.path:
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
            self.duplog("info", "service %(path)s parents all avail up",
                        path=svc.path)
            return True
        self.duplog("info", "service %(path)s parents not available:"
                    " %(missing)s", path=svc.path,
                    missing=" ".join(missing))
        return False

    def min_instances_reached(self, svc):
        instances = self.get_service_instances(svc.path, discard_empty=False)
        live_nodes = [nodename for nodename in shared.CLUSTER_DATA if shared.CLUSTER_DATA[nodename] is not None]
        min_instances = set(svc.peers) & set(live_nodes)
        return len(instances) >= len(min_instances)

    def instances_started_or_start_failed(self, path, nodes):
        for nodename in nodes:
            instance = self.get_service_instance(path, nodename)
            if instance is None:
                continue
            if instance.get("avail") in STOPPED_STATES and instance["monitor"].get("status") != "start failed":
                return False
        self.log.info("service '%s' instances on nodes '%s' are stopped",
            path, ", ".join(nodes))
        return True

    def instances_stopped(self, path, nodes):
        for nodename in nodes:
            instance = self.get_service_instance(path, nodename)
            if instance is None:
                continue
            if instance.get("avail") not in STOPPED_STATES:
                self.log.info("service '%s' instance node '%s' is not stopped yet",
                              path, nodename)
                return False
        return True

    def has_leader(self, svc):
        for nodename, instance in self.get_service_instances(svc.path).items():
            if instance["monitor"].get("placement") == "leader":
                return True
        return False

    def non_leaders_stopped(self, path, exclude_status=None):
        svc = self.get_service(path)
        if svc is None:
            return True
        if exclude_status is None:
            exclude_status = []
        for nodename in svc.peers:
            if nodename == rcEnv.nodename:
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
                self.log.info("service '%s' non leader instance on node '%s' "
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
        if shared.AGG[svc.path].avail is None:
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
                              "service %s", top, svc.path)
        except IndexError:
            if not silent:
                self.log.info("unblock service %s leader last action (placement ranks empty)", svc.path)
            return rcEnv.nodename
        if top != rcEnv.nodename:
            if not silent:
                self.log.info("unblock service %s leader last action (not leader)",
                              svc.path)
            return top
        for node in svc.peers:
            if node == rcEnv.nodename:
                continue
            instance = self.get_service_instance(svc.path, node)
            if instance is None:
                continue
            elif deleted:
                if not silent:
                    self.log.info("delay leader-last action on service %s: "
                                  "node %s is still not deleted", svc.path, node)
                return
            if instance.get("provisioned", False) is not provisioned:
                if not silent:
                    self.log.info("delay leader-last action on service %s: "
                                  "node %s is still %s", svc.path, node,
                                  "unprovisioned" if provisioned else "provisioned")
                return
        self.log.info("unblock service %s leader last action (leader)",
                      svc.path)
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
        instances = self.get_service_instances(svc.path, discard_empty=True)
        candidates = [nodename for (nodename, data) in instances.items() \
                      if data.get("avail") in ("up", "warn")]
        if len(candidates) == 0:
            if not silent:
                self.log.info("service %s has no up instance, relax candidates "
                              "constraints", svc.path)
            candidates = self.placement_candidates(
                svc, discard_frozen=False,
                discard_unprovisioned=False,
            )
        try:
            top = self.placement_ranks(svc, candidates=candidates)[0]
            if not silent:
                self.log.info("elected %s as the first node to take action on "
                              "service %s", top, svc.path)
        except IndexError:
            if not silent:
                self.log.error("service %s placement ranks list is empty", svc.path)
            return True
        if top == rcEnv.nodename:
            return True
        instance = self.get_service_instance(svc.path, top)
        if instance is None and deleted:
            return True
        if instance.get("provisioned", True) is provisioned:
            return True
        if not silent:
            self.log.info("delay leader-first action on service %s", svc.path)
        return False

    def overloaded_up_service_instances(self, path):
        return [nodename for nodename in self.up_service_instances(path) if self.node_overloaded(nodename)]

    def scaler_slots(self, paths):
        count = 0
        for path in paths:
            svc = shared.SERVICES[path]
            if svc.topology == "flex":
                width = len([1 for nodename in svc.peers if nodename in shared.CLUSTER_DATA])
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
                status = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.path]["resources"][res.rid]["status"]
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
        Count the number of service instances in 'started' local expect state.
        """
        count = 0
        try:
            for node, ndata in shared.CLUSTER_DATA.items():
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
            if not with_self and nodename == rcEnv.nodename:
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

    def peer_transitioning(self, path, discard_local=True):
        """
        Return the nodename of the first peer with the service in a transition
        state.
        """
        for nodename, instance in self.get_service_instances(path).items():
            if discard_local and nodename == rcEnv.nodename:
                continue
            if instance["monitor"].get("status", "").endswith("ing"):
                return nodename

    def peer_start_failed(self, path):
        """
        Return the nodename of the first peer with the service in a start failed
        state.
        """
        for nodename, instance in self.get_service_instances(path).items():
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
            if nodename == rcEnv.nodename:
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
        for nodename in self.cluster_nodes:
            try:
                fstatus_l.append(shared.CLUSTER_DATA[nodename].get("frozen"))
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
                try:
                    child_avail = shared.AGG[child]["avail"]
                except KeyError:
                    child_avail = "unknown"
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

    def get_agg_frozen(self, path):
        frozen = 0
        total = 0
        for instance in self.get_service_instances(path).values():
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
        status_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("status", {}).get(path, {}).get("updated", 0)
        config_age = shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("config", {}).get(path, {}).get("updated", 0)
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
        nodenames = []
        if shared.SMON_DATA.get(path, {}).get("global_expect") in global_expect:
            # relayed smon may no longer have an instance
            return True
        for nodename, instance in self.get_service_instances(path).items():
            if global_expect and instance.get("monitor", {}).get("global_expect") in global_expect:
                return True
        return False

    @staticmethod
    def get_local_paths():
        """
        Extract service instance names from the locally maintained hb data.
        """
        paths = []
        try:
            with shared.CLUSTER_DATA_LOCK:
                for path in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                    if shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][path]["avail"] == "up":
                        paths.append(path)
        except KeyError:
            return []
        return paths

    def get_services_configs(self):
        """
        Return a hash indexed by path and nodename, containing the services
        configuration mtime and checksum.
        """
        data = {}
        for nodename in self.cluster_nodes:
            try:
                configs = shared.CLUSTER_DATA[nodename]["services"]["config"]
            except (TypeError, KeyError):
                continue
            for path in [p for p in configs]:
                try:
                    config = configs[path]
                except KeyError:
                    # happens on object delete
                    continue
                if path not in data:
                    data[path] = {}
                data[path][nodename] = Storage(config)
        return data

    def get_any_service_instance(self, path):
        """
        Return the specified service status structure on any node.
        """
        for nodename in self.cluster_nodes:
            try:
                data = shared.CLUSTER_DATA[nodename]["services"]["status"][path]
            except KeyError:
                continue
            if data in (None, ""):
                continue
            return data

    @staticmethod
    def get_last_svc_config(path):
        try:
            return shared.CLUSTER_DATA[rcEnv.nodename]["services"]["config"][path]
        except KeyError:
            return

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
            if peer not in shared.CLUSTER_DATA:
                # discard unreachable nodes from the consensus
                continue
            try:
                csum = shared.CLUSTER_DATA[peer]["services"]["config"][path]["csum"]
            except KeyError:
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

    def get_services_config(self):
        config = {}
        for path in list_services():
            cfg = svc_pathcf(path)
            try:
                config_mtime = os.path.getmtime(cfg)
            except Exception as exc:
                self.log.warning("failed to get %s mtime: %s", cfg, str(exc))
                config_mtime = 0
            last_config = self.get_last_svc_config(path)
            if last_config is None or config_mtime > last_config["updated"]:
                #self.log.debug("compute service %s config checksum", path)
                try:
                    csum = fsum(cfg)
                except (OSError, IOError) as exc:
                    self.log.warning("service %s config checksum error: %s", path, exc)
                    continue
                try:
                    with shared.SERVICES_LOCK:
                        name, namespace, kind = split_path(path)
                        shared.SERVICES[path] = factory(kind)(name, namespace, node=shared.NODE)
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
                    try:
                        del shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][path]
                    except KeyError:
                        pass
        return config

    def get_last_svc_status_mtime(self, path):
        """
        Return the mtime of the specified service configuration file on the
        local node. If unknown, return 0.
        """
        instance = self.get_service_instance(path, rcEnv.nodename)
        if instance is None:
            return 0
        mtime = instance["updated"]
        if mtime is None:
            return 0
        return mtime

    def service_status_fallback(self, path):
        """
        Return the specified service status structure fetched from an execution
        of svcmgr -s <path> json status". As we arrive here when the
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

    def get_services_status(self, paths):
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

        for path in paths:
            idata = None
            last_mtime = self.get_last_svc_status_mtime(path)
            fpath = svc_pathvar(path, "status.json")
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
                idata = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][path]

            if idata:
                data[path] = idata
            else:
                self.service_status(path)
                continue

            # update the frozen instance attribute
            data[path]["frozen"] = shared.SERVICES[path].frozen()

            # embed the updated smon data
            self.set_smon_l_expect_from_status(data, path)
            data[path]["monitor"] = dict(self.get_service_monitor(path))

            # forget the stonith target node if we run the service
            if data[path].get("avail", "n/a") == "up":
                try:
                    del data[path]["monitor"]["stonith"]
                except KeyError:
                    pass

        # deleting services (still in SMON_DATA, no longer has cf).
        # emulate a status
        for path in set(shared.SMON_DATA.keys()) - set(paths):
            data[path] = {
                "monitor": dict(self.get_service_monitor(path)),
                "resources": {},
            }

        return data

    #########################################################################
    #
    # Service-specific monitor data helpers
    #
    #########################################################################
    @staticmethod
    def reset_smon_retries(path, rid):
        with shared.SMON_DATA_LOCK:
            if path not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[path]:
                return
            if rid in shared.SMON_DATA[path].restart:
                del shared.SMON_DATA[path].restart[rid]
            if len(shared.SMON_DATA[path].restart.keys()) == 0:
                del shared.SMON_DATA[path].restart

    @staticmethod
    def get_smon_retries(path, rid):
        with shared.SMON_DATA_LOCK:
            if path not in shared.SMON_DATA:
                return 0
            if "restart" not in shared.SMON_DATA[path]:
                return 0
            if rid not in shared.SMON_DATA[path].restart:
                return 0
            else:
                return shared.SMON_DATA[path].restart[rid]

    @staticmethod
    def inc_smon_retries(path, rid):
        with shared.SMON_DATA_LOCK:
            if path not in shared.SMON_DATA:
                return
            if "restart" not in shared.SMON_DATA[path]:
                shared.SMON_DATA[path].restart = {}
            if rid not in shared.SMON_DATA[path].restart:
                shared.SMON_DATA[path].restart[rid] = 1
            else:
                shared.SMON_DATA[path].restart[rid] += 1

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

    def set_smon_g_expect_from_status(self, path, smon, status):
        """
        Align global_expect with the actual service states.
        """
        if smon.global_expect is None:
            return
        instance = self.get_service_instance(path, rcEnv.nodename)
        if instance is None:
            return
        local_frozen = instance.get("frozen", 0)
        frozen = shared.AGG[path].frozen
        provisioned = shared.AGG[path].provisioned
        deleted = self.get_agg_deleted(path)
        purged = self.get_agg_purged(provisioned, deleted)
        stopped = status in STOPPED_STATES
        if smon.global_expect == "stopped" and stopped and local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", path, smon.global_expect, status)
            self.set_smon(path, global_expect="unset")
        elif smon.global_expect == "shutdown" and self.get_agg_shutdown(path) and \
           local_frozen:
            self.log.info("service %s global expect is %s and its global "
                          "status is %s", path, smon.global_expect, status)
            self.set_smon(path, global_expect="unset")
        elif smon.global_expect == "started":
            if smon.placement == "none":
                self.set_smon(path, global_expect="unset")
            if status in STARTED_STATES and not local_frozen:
                self.log.info("service %s global expect is %s and its global "
                              "status is %s", path, smon.global_expect, status)
                self.set_smon(path, global_expect="unset")
                return
            if frozen != "thawed":
                return
            svc = self.get_service(path)
            if self.peer_warn(path, with_self=True):
                self.set_smon(path, global_expect="unset")
                return
        elif (smon.global_expect == "frozen" and frozen == "frozen") or \
             (smon.global_expect == "thawed" and frozen == "thawed") or \
             (smon.global_expect == "unprovisioned" and provisioned in (False, "n/a") and stopped):
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            self.set_smon(path, global_expect="unset")
        elif smon.global_expect == "provisioned" and provisioned in (True, "n/a"):
            if smon.placement == "none":
                self.set_smon(path, global_expect="unset")
            if shared.AGG[path].avail in ("up", "n/a"):
                # provision success, thaw
                self.set_smon(path, global_expect="thawed")
            else:
                self.set_smon(path, global_expect="started")
        elif (smon.global_expect == "purged" and purged is True) or \
             (smon.global_expect == "deleted" and deleted is True):
            self.log.debug("service %s global expect is %s, already is",
                           path, smon.global_expect)
            with shared.SMON_DATA_LOCK:
                del shared.SMON_DATA[path]
        elif smon.global_expect == "aborted" and \
             self.get_agg_aborted(path):
            self.log.info("service %s action aborted", path)
            self.set_smon(path, global_expect="unset")
            if smon.status and smon.status.startswith("wait "):
                # don't leave lingering "wait" mon state when we no longer
                # have a target state to reach
                self.set_smon(path, status="idle")
        elif smon.global_expect == "placed":
            if frozen != "thawed":
                return
            if shared.AGG[path].placement in ("optimal", "n/a") and \
               shared.AGG[path].avail in ("up", "n/a"):
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
        elif smon.global_expect.startswith("placed@"):
            target = smon.global_expect.split("@")[-1].split(",")
            if self.instances_started_or_start_failed(path, target):
                self.set_smon(path, global_expect="unset")

    def set_smon_l_expect_from_status(self, data, path):
        if path not in data:
            return
        if data.get(path, {}).get("avail") != "up":
            return
        with shared.SMON_DATA_LOCK:
            if path not in shared.SMON_DATA:
                return
            if shared.SMON_DATA[path].global_expect is not None or \
               shared.SMON_DATA[path].status != "idle" or \
               shared.SMON_DATA[path].local_expect in ("started", "shutdown"):
                return
            self.log.info("service %s monitor local_expect "
                          "%s => %s", path,
                          shared.SMON_DATA[path].local_expect, "started")
            shared.SMON_DATA[path].local_expect = "started"

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
        self.update_daemon_status()

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
        data["frozen"] = self.node_frozen
        data["env"] = shared.NODE.env
        data["labels"] = shared.NODE.labels
        data["targets"] = shared.NODE.targets
        data["locks"] = shared.LOCKS
        data["speaker"] = self.speaker() and "collector" in shared.THREADS
        data["min_avail_mem"] = shared.NODE.min_avail_mem
        data["min_avail_swap"] = shared.NODE.min_avail_swap
        data["monitor"] = dict(shared.NMON_DATA)
        data["services"]["config"] = self.get_services_config()
        data["services"]["status"] = self.get_services_status(data["services"]["config"].keys())

        if self.quorum:
            data["arbitrators"] = self.get_arbitrators_data()

        # purge deleted service instances
        for path in set(chain(data["services"]["status"].keys(), shared.SMON_DATA.keys())):
            if path in data["services"]["config"]:
                continue
            try:
                smon = shared.SMON_DATA[path]
                global_expect = smon.get("global_expect")
                global_expect_updated = smon.get("global_expect_updated", 0)
                if global_expect is not None and time.time() < global_expect_updated + 3:
                    # keep the smon around for a while
                    #self.log.info("relay foreign service %s global expect %s",
                    #              path, global_expect)
                    continue
                else:
                    del shared.SMON_DATA[path]
            except KeyError:
                pass
            try:
                del data["services"]["status"][path]
                self.log.debug("purge deleted service %s from status data", path)
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

        # exclude from the diff
        try:
            del data["gen"]
        except KeyError:
            pass
        try:
            updated = data["updated"]
            del data["updated"]
        except KeyError:
            updated = now

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
            data["updated"] = updated
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
        nodenames = list(shared.CLUSTER_DATA)
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
            for path, instance in shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"].items():
                if instance is None:
                    continue
                current_global_expect = instance["monitor"].get("global_expect")
                if current_global_expect == "aborted":
                    # refuse a new global expect if aborting
                    continue
                current_global_expect_updated = instance["monitor"].get("global_expect_updated")
                for nodename in nodenames:
                    rinstance = self.get_service_instance(path, nodename)
                    if rinstance is None:
                        continue
                    if rinstance.get("stonith") is True and \
                       instance["monitor"].get("stonith") != nodename:
                        self.set_smon(path, stonith=nodename)
                    global_expect = rinstance["monitor"].get("global_expect")
                    if global_expect is None:
                        continue
                    global_expect_updated = rinstance["monitor"].get("global_expect_updated")
                    if current_global_expect and global_expect_updated and \
                       current_global_expect_updated and \
                       global_expect_updated < current_global_expect_updated:
                        # we have a more recent update
                        continue
                    if path in shared.SERVICES and shared.SERVICES[path].disabled and \
                       global_expect not in ("frozen", "thawed", "aborted", "deleted", "purged"):
                        continue
                    if global_expect == current_global_expect:
                        self.log.debug("node %s wants service %s %s, already targeting that",
                                       nodename, path, global_expect)
                        continue
                    #else:
                    #    self.log.info("node %s wants service %s %s, already is", nodename, path, global_expect)
                    if self.accept_g_expect(path, instance, global_expect):
                        self.log.info("node %s wants service %s %s", nodename, path, global_expect)
                        self.set_smon(path, global_expect=global_expect)

    def accept_g_expect(self, path, instance, global_expect):
        if path in shared.AGG:
            agg = shared.AGG[path]
        else:
            agg = Storage()
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

    def get_agg(self, path):
        data = self.get_agg_conf(path)
        data.avail = self.get_agg_avail(path)
        data.frozen = self.get_agg_frozen(path)
        data.overall = self.get_agg_overall(path)
        data.placement = self.get_agg_placement(path)
        data.provisioned = self.get_agg_provisioned(path)
        return data

    def get_all_paths(self):
        """
        Caller needs CLUSTER_DATA_LOCK.
        """
        paths = set()
        for nodename, data in shared.CLUSTER_DATA.items():
            try:
                for path in data["services"]["config"]:
                    paths.add(path)
            except KeyError:
                continue
        return paths

    def get_agg_services(self):
        data = {}
        with shared.CLUSTER_DATA_LOCK:
            all_paths = self.get_all_paths()
            for path in all_paths:
                try:
                    if self.get_service(path).topology == "span":
                        data[path] = Storage()
                        continue
                except Exception as exc:
                    data[path] = Storage()
                    pass
                data[path] = self.get_agg(path)
        shared.AGG = data
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

    def status(self):
        data = shared.OsvcThread.status(self)
        data["nodes"] = json.loads(json.dumps(shared.CLUSTER_DATA))
        data["compat"] = self.compat
        data["transitions"] = self.transition_count()
        data["frozen"] = self.get_clu_agg_frozen()
        data["services"] = self.get_agg_services()
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
        if self.node_frozen:
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
            try:
                instance = shared.CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svc.path]
            except:
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
                if peer == rcEnv.nodename:
                    continue
                try:
                    instance = shared.CLUSTER_DATA[peer]["services"]["status"][svc.path]
                except:
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

    def instance_frozen(self, path, nodename=None):
        if not nodename:
            nodename = rcEnv.nodename
        try:
            return shared.CLUSTER_DATA[nodename]["services"]["status"][path].get("frozen", 0)
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
            self.node_frozen = self.freezer.node_frozen()

    def missing_beating_peer_data(self):
        for node in self.cluster_nodes:
            if node == rcEnv.nodename:
                continue
            try:
                shared.CLUSTER_DATA[node]["services"]
                continue
            except KeyError:
                pass
            # node dataset is empty or a brief coming from a ping
            try:
                if any(shared.THREADS[thr_id].is_beating(node) for thr_id in shared.THREADS if thr_id.endswith(".rx")):
                    self.log.info("waiting for node %s dataset", node)
                    return True
            except Exception as exc:
                return True
        return False

