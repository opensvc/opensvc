"""
Collector Thread
"""
import os
import sys
import logging
import time

import osvcd_shared as shared
import rcExceptions as ex
from rcGlobalEnv import rcEnv

MAX_QUEUED = 1000

class Collector(shared.OsvcThread):
    update_interval = 300
    min_update_interval = 10
    min_ping_interval = 60

    def reset(self):
        self.last_comm = None
        self.last_config = {}
        self.last_status = {}
        self.last_status_changed = set()

    def run(self):
        self.set_tid()
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.collector")
        self.log.info("collector started")
        self.reset()

        while True:
            if self.stopped():
                sys.exit(0)
            try:
                self.do()
            except Exception as exc:
                self.log.exception(exc)
                time.sleep(1)

    def get_last_status(self, data):
        """
        Identify changes in data
        """
        last_status = {}
        last_status_changed = set()

        def add_parents(_svcname, done=None):
            """
            Propagate change to the service parents
            """
            if done is None:
                done = []
            if _svcname in done:
                # break recursion loop
                return
            for nodename, ndata in data["nodes"].items():
                for svcname, sdata in ndata.get("services", {}).get("status", {}).items():
                    slaves = sdata.get("slaves", []) + sdata.get("scaler_slaves", [])
                    if _svcname not in slaves:
                        continue
                    if svcname in last_status_changed:
                        continue
                    last_status_changed.add(svcname)
                    add_parents(svcname, done=done+[_svcname])

        for svcname, nodename in self.last_status:
            if svcname is None:
                # node disappeared
                if data["nodes"].get(nodename) is None:
                    last_status_changed.add("@"+nodename)
            else:
                # instance disappeared
                if data["nodes"].get(nodename, {}).get("services", {}).get("status", {}).get(svcname) is None:
                    last_status_changed |= set([svcname, svcname+"@"+nodename])

        for nodename, ndata in data["nodes"].items():
            # detect node frozen changes
            node_frozen = ndata.get("frozen")
            last_node_frozen = self.last_status.get((None, nodename), {}).get("frozen")
            if node_frozen != last_node_frozen:
                last_status_changed.add("@"+nodename)
            last_status[(None, nodename)] = {"frozen": node_frozen}

            # detect instances status changes
            for svcname, sdata in ndata.get("services", {}).get("status", {}).items():
                status_csum = sdata.get("csum", "") + \
                    str(sdata.get("monitor", {}).get("status_updated")) + \
                    str(sdata.get("monitor", {}).get("global_status_updated"))
                prev_status_csum = self.last_status.get((svcname, nodename))
                if status_csum != prev_status_csum:
                    last_status_changed.add(svcname+"@"+nodename)
                    if svcname not in last_status_changed:
                        last_status_changed.add(svcname)
                        add_parents(svcname)
                last_status[(svcname, nodename)] = status_csum

        return last_status, last_status_changed

    def get_last_config(self, data):
        last_config = {}
        last_config_changed = []
        for svcpath, sdata in data["nodes"].get(rcEnv.nodename, {}).get("services", {}).get("config", {}).items():
            config_csum = sdata.get("csum", 0)
            prev_config_csum = self.last_config.get(svcpath, 0)
            if prev_config_csum and config_csum != prev_config_csum:
                # don't send all configs on daemon start
                last_config_changed.append(svcpath)
            last_config[svcpath] = config_csum
        self.last_config = last_config
        return last_config_changed

    def init_collector(self):
        if shared.NODE.collector.reinit():
            self.log.info("the collector is reachable")
            self.reset()

    def do(self):
        self.reload_config()
        self.init_collector()
        if shared.NODE.collector.disabled():
            self.queue_limit()
        else:
            self.run_collector()
            self.unqueue_xmlrpc()
        if not self.stopped():
            with shared.COLLECTOR_TICKER:
                shared.COLLECTOR_TICKER.wait(self.update_interval)

    def queue_limit(self):
        overlimit = len(shared.COLLECTOR_XMLRPC_QUEUE) - MAX_QUEUED
        if overlimit > 0:
            #self.log.warning("drop %d queued messages", overlimit)
            for _ in range(overlimit):
                shared.COLLECTOR_XMLRPC_QUEUE.pop()

    def unqueue_xmlrpc(self):
        while True:
            try:
                args, kwargs = shared.COLLECTOR_XMLRPC_QUEUE.pop()
            except IndexError:
                break
            if len(args) == 0:
                continue
            try:
                #self.log.info("call %s", args[0])
                shared.NODE.collector.call(*args, **kwargs)
            except Exception as exc:
                self.log.error("call %s: %s", args[0], exc)
                time.sleep(0.2)
                shared.NODE.collector.disable()

    def send_containerinfo(self, svcname):
        if svcname not in shared.SERVICES:
            return
        if not shared.SERVICES[svcname].has_encap_resources:
            return
        self.log.info("send service %s container info", svcname)
        with shared.SERVICES_LOCK:
            try:
                shared.NODE.collector.call("push_containerinfo", shared.SERVICES[svcname])
            except Exception as exc:
                self.log.error("call push_containerinfo: %s", exc)
                shared.NODE.collector.disable()

    def send_service_config(self, svcname):
        if svcname not in shared.SERVICES:
            return
        self.log.info("send service %s config", svcname)
        with shared.SERVICES_LOCK:
            try:
                shared.NODE.collector.call("push_config", shared.SERVICES[svcname])
            except Exception as exc:
                self.log.error("call push_config: %s", exc)
                shared.NODE.collector.disable()

    def send_daemon_status(self, data):
        if self.last_status_changed:
            self.log.info("send daemon status, %d changes", len(self.last_status_changed))
        else:
            self.log.info("send daemon status, resync")
        try:
            shared.NODE.collector.call("push_daemon_status", data, list(self.last_status_changed))
        except Exception as exc:
            self.log.error("call push_daemon_status: %s", exc)
            shared.NODE.collector.disable()
        self.last_comm = time.time()

    def ping(self, data):
        self.log.debug("ping the collector")
        try:
            result = shared.NODE.collector.call("daemon_ping")
            if result and result.get("info") == "resync":
                self.log.info("ping rejected, collector ask for resync")
                self.send_daemon_status(data)
        except Exception as exc:
            self.log.error("call daemon_ping: %s", exc)
            shared.NODE.collector.disable()
        self.last_comm = time.time()

    def get_data(self):
        """
        Get a copy of the monitor thread, expunged from encap services,
        to avoid missing changes happening during our work
        """
        if "monitor" not in shared.THREADS:
            # the monitor thread is not started
            return
        data = shared.THREADS["monitor"].status()
        _data = {
            "cluster_id": self.cluster_id,
            "nodes": {},
            "services": {},
        }
        for key in data:
            if key not in _data:
                _data[key] = data[key]

        for nodename in data["nodes"]:
            try:
                instances_status = data["nodes"][nodename]["services"]["status"]
                instances_config = data["nodes"][nodename]["services"]["config"]
                node_frozen = data["nodes"][nodename]["frozen"]
            except (TypeError, KeyError):
                continue
            if instances_status is None:
                continue
            if instances_config is None:
                continue
            for svcname in list(instances_status.keys()):
                if svcname not in instances_config:
                    # deleted service instance
                    continue
                if not instances_status[svcname]:
                    continue
                if instances_status[svcname].get("encap") is True:
                    continue
                if nodename not in _data["nodes"]:
                    _data["nodes"][nodename] = {
                        "services": {
                            "config": {},
                            "status": {},
                        },
                    }
                _data["nodes"][nodename]["frozen"] = node_frozen
                _data["nodes"][nodename]["services"]["status"][svcname] = instances_status[svcname]
                _data["nodes"][nodename]["services"]["config"][svcname] = instances_config[svcname]
                _data["services"][svcname] = data["services"][svcname]
        return _data

    def run_collector(self):
        data = self.get_data()
        if data is None:
            return
        if len(data["services"]) == 0:
            #self.log.debug("no service")
            return

        last_config_changed = self.get_last_config(data)
        for svcname in last_config_changed:
            self.send_service_config(svcname)
            self.send_containerinfo(svcname)

        if self.speaker():
            last_status, last_status_changed = self.get_last_status(data)
            now = time.time()
            self.last_status_changed |= last_status_changed
            if self.last_comm is None:
                self.send_daemon_status(data)
            elif self.last_status_changed:
                if self.last_comm <= now - self.min_update_interval:
                    self.send_daemon_status(data)
                    self.last_status_changed = set()
                else:
                    # avoid storming the collector with daemon status updates
                    #self.log.debug("last daemon status too soon: %d", self.last_comm)
                    pass
            elif self.last_comm <= now - self.min_ping_interval:
                self.ping(data)
            self.last_status = last_status


