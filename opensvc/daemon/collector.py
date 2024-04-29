"""
Collector Thread
"""
import json
import os
import sys
import logging
import time

import daemon.shared as shared
from env import Env
from utilities.naming import svc_pathvar, split_path

MAX_QUEUED = 1000


class Collector(shared.OsvcThread):
    name = "collector"

    def reset(self):
        self.last_comm = None
        self.last_config = {}
        self.last_status = {}
        self.last_status_changed = set()

    def run(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.collector"), {"node": Env.nodename, "component": self.name})
        self.log.info("collector started")
        previous_interval_signature = ""
        self.reset()

        while True:
            if self.stopped():
                self.exit()
            try:
                self.do()
                self.update_status()
                interval_signature = "%d-%d-%d" % (self.db_update_interval, self.db_min_update_interval, self.db_min_ping_interval)
                if interval_signature != previous_interval_signature:
                    self.log.info("collector thread config: update_interval=%d, min_update_interval=%d, min_ping_interval= %d",
                                  self.db_update_interval, self.db_min_update_interval, self.db_min_ping_interval)
                    previous_interval_signature = interval_signature
            except Exception as exc:
                self.log.exception(exc)
                time.sleep(1)

    def get_last_status(self, data):
        """
        Identify changes in data
        """
        last_status = {}
        last_status_changed = set()

        def add_parents(_path, done=None):
            """
            Propagate change to the service parents
            """
            if done is None:
                done = []
            if _path in done:
                # break recursion loop
                return
            for nodename, ndata in data["nodes"].items():
                for path, sdata in ndata.get("services", {}).get("status", {}).items():
                    slaves = sdata.get("slaves", []) + sdata.get("scaler_slaves", [])
                    if _path not in slaves:
                        continue
                    if path in last_status_changed:
                        continue
                    last_status_changed.add(path)
                    add_parents(path, done=done+[_path])

        for path, nodename in self.last_status:
            if path is None:
                # node disappeared
                if data["nodes"].get(nodename) is None:
                    last_status_changed.add("@"+nodename)
            else:
                # instance disappeared
                if data["nodes"].get(nodename, {}).get("services", {}).get("status", {}).get(path) is None:
                    last_status_changed |= set([path, path+"@"+nodename])

        for nodename, ndata in data["nodes"].items():
            # detect node frozen changes
            node_frozen = ndata.get("frozen")
            last_node_frozen = self.last_status.get((None, nodename), {}).get("frozen")
            if node_frozen != last_node_frozen:
                last_status_changed.add("@"+nodename)
            last_status[(None, nodename)] = {"frozen": node_frozen}

            # detect instances status changes
            for path, sdata in ndata.get("services", {}).get("status", {}).items():
                status_csum = sdata.get("csum", "") + \
                    str(sdata.get("monitor", {}).get("status_updated")) + \
                    str(sdata.get("monitor", {}).get("global_status_updated"))
                prev_status_csum = self.last_status.get((path, nodename))
                if status_csum != prev_status_csum:
                    last_status_changed.add(path+"@"+nodename)
                    if path not in last_status_changed:
                        last_status_changed.add(path)
                        add_parents(path)
                last_status[(path, nodename)] = status_csum

        return last_status, last_status_changed

    def config_sent_file(self, path):
        return os.path.join(svc_pathvar(path), "config_sent.json")

    def load_config_sent(self, path):
        p = self.config_sent_file(path)
        with open(p, "r") as f:
            return json.load(f)

    def dump_config_sent(self, path, csum):
        p = self.config_sent_file(path)
        with open(p, "w") as f:
            json.dump({
                "sent": time.time(),
                "csum": csum,
            }, f)

    def get_last_config(self, data):
        last_config = {}
        last_config_changed = {}
        for path, sdata in data["nodes"].get(Env.nodename, {}).get("services", {}).get("config", {}).items():
            _, _, kind = split_path(path)
            if kind in ("sec", "cfg", "ccfg", "usr"):
                # the collector drops updates for these kinds, so save the calls
                continue
            if path not in self.last_config:
                # first time we see this path, try populating our cache from the object's config_sent.json
                try:
                    self.last_config[path] = self.load_config_sent(path)["csum"]
                except Exception as exc:
                    self.last_config[path] = "force_send"
            config_csum = sdata.get("csum", 0)
            prev_config_csum = self.last_config.get(path, 0)
            if prev_config_csum and config_csum != prev_config_csum:
                # don't send all configs on daemon start
                last_config_changed[path] = config_csum
            last_config[path] = config_csum
        self.last_config = last_config
        return last_config_changed

    def init_collector(self):
        if shared.NODE.collector.reinit():
            self.log.info("the collector is reachable")
            self.reset()

    def do(self):
        self.reload_config()
        self.init_collector()
        if shared.NODE.collector_env.uuid == "":
            # don't even queue
            pass
        elif shared.NODE.collector.disabled():
            self.queue_limit()
        else:
            self.run_collector()
            self.unqueue_xmlrpc()
        if not self.stopped():
            with shared.COLLECTOR_TICKER:
                shared.COLLECTOR_TICKER.wait(self.db_update_interval)

    def queue_limit(self):
        overlimit = len(shared.COLLECTOR_XMLRPC_QUEUE) - MAX_QUEUED
        if overlimit > 0:
            #self.log.warning("drop %d queued messages", overlimit)
            for _ in range(overlimit):
                shared.COLLECTOR_XMLRPC_QUEUE.pop()

    def unqueue_xmlrpc(self):
        while True:
            if self.stopped():
                return
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

    def send_containerinfo(self, path):
        if self.stopped():
            return

        with shared.SERVICES_LOCK:
            if path not in shared.SERVICES:
                return
            service = shared.SERVICES[path]
            if not service.has_encap_resources:
                return
            containers = [container.send_containerinfo_arg()
                          for container in service.get_resources('container')]

        if len(containers) == 0:
            return

        self.log.info("send service %s container info", path)
        try:
            shared.NODE.collector.call("push_containerinfo", path, containers)
        except Exception as exc:
            self.log.error("call push_containerinfo %s: %s", path, exc)
            shared.NODE.collector.disable()

    def send_service_config(self, path, csum):
        if self.stopped():
            return

        with shared.SERVICES_LOCK:
            if path not in shared.SERVICES:
                return
            svc = shared.SERVICES[path].send_service_config_args()

        self.log.info("send service %s config", path)
        try:
            shared.NODE.collector.call("push_config", svc)
            sent = True
        except Exception as exc:
            self.log.error("call push_config %s: %s", path, exc)
            shared.NODE.collector.disable()
            sent = False

        if sent:
            try:
                self.dump_config_sent(path, csum)
            except Exception as exc:
                self.log.warning("writing config sent persistent cache file: %s", exc)

    def send_daemon_status(self, data):
        if self.last_status_changed:
            self.log.debug("send daemon status, %d changes", len(self.last_status_changed))
        else:
            self.log.debug("send daemon status, resync")
        try:
            shared.NODE.collector.call("push_daemon_status", data, list(self.last_status_changed))
        except Exception as exc:
            self.log.error("call push_daemon_status: %s", exc)
            shared.NODE.collector.disable()
        self.last_comm = time.time()

    def ping(self, data):
        if self.stopped():
            return
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
        data = self.daemon_status_data.get_copy(["monitor"])
        _data = {
            "cluster_id": self.cluster_id,
            "cluster_name": self.cluster_name,
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
            for path in list(instances_status.keys()):
                if path not in instances_config:
                    # deleted object instance
                    continue
                if path not in data["services"]:
                    # deleted object
                    continue
                if not instances_status[path]:
                    continue
                if instances_status[path].get("encap") is True:
                    continue
                if nodename not in _data["nodes"]:
                    _data["nodes"][nodename] = {
                        "services": {
                            "config": {},
                            "status": {},
                        },
                    }
                _data["nodes"][nodename]["frozen"] = node_frozen
                _data["nodes"][nodename]["services"]["status"][path] = instances_status[path]
                _data["nodes"][nodename]["services"]["config"][path] = instances_config[path]
                _data["services"][path] = data["services"][path]
        return _data

    def run_collector(self):
        data = self.get_data()
        if data is None:
            return
        if len(data["services"]) == 0:
            #self.log.debug("no service")
            return

        last_config_changed = self.get_last_config(data)
        for path, csum in last_config_changed.items():
            self.send_service_config(path, csum)
            self.send_containerinfo(path)

        if self.speaker():
            last_status, last_status_changed = self.get_last_status(data)
            now = time.time()
            self.last_status_changed |= last_status_changed
            if self.last_comm is None:
                self.send_daemon_status(data)
            elif self.last_status_changed:
                if self.last_comm <= now - self.db_min_update_interval:
                    self.send_daemon_status(data)
                    self.last_status_changed = set()
                else:
                    # avoid storming the collector with daemon status updates
                    pass
            elif self.last_comm <= now - self.db_min_ping_interval:
                self.ping(data)
            self.last_status = last_status
