"""
Collector Thread
"""
import os
import sys
import logging
import datetime

import osvcd_shared as shared
import rcExceptions as ex
from comm import Crypt
from rcGlobalEnv import rcEnv

class Collector(shared.OsvcThread, Crypt):
    interval = 300

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.collector")
        self.log.info("collector started")
        self.last_comm = None
        self.last_config = {}
        self.last_status = {}

        while True:
            try:
                self.do()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                sys.exit(0)

    def get_last_status(self, data):
        last_status = {}
        last_status_changed = []
        for nodename, ndata in data["nodes"].items():
            for svcname, sdata in ndata.get("services", {}).get("status", {}).items():
                status_csum = sdata.get("csum")
                prev_status_csum = self.last_status.get((svcname, nodename))
                if status_csum != prev_status_csum:
                    last_status_changed.append(svcname+"@"+nodename)
                    if svcname not in last_status_changed:
                        last_status_changed.append(svcname)
                last_status[(svcname, nodename)] = status_csum
        return last_status, last_status_changed

    def get_last_config(self, data):
        last_config = {}
        last_config_changed = []
        for svcname, sdata in data["nodes"].get(rcEnv.nodename, {}).get("services", {}).get("config", {}).items():
            config_csum = sdata.get("csum")
            prev_config_csum = self.last_config.get(svcname)
            if config_csum != prev_config_csum:
                last_config_changed.append(svcname)
            last_config[svcname] = config_csum
        return last_config, last_config_changed

    def do(self):
        self.reload_config()
        self.run_collector()
        with shared.COLLECTOR_TICKER:
            shared.COLLECTOR_TICKER.wait(self.interval)

    def send_containerinfo(self, svcname):
        if svcname not in shared.SERVICES:
            return
        self.log.info("send service %s container info", svcname)
        with shared.SERVICES_LOCK:
            shared.NODE.collector.call("push_containerinfo", shared.SERVICES[svcname])

    def send_service_config(self, svcname):
        if svcname not in shared.SERVICES:
            return
        self.log.info("send service %s config", svcname)
        with shared.SERVICES_LOCK:
            shared.NODE.collector.call("push_config", shared.SERVICES[svcname])

    def send_daemon_status(self, data, last_status_changed=None):
        if last_status_changed:
            self.log.info("send daemon status, changed: %s", ", ".join(last_status_changed))
        else:
            self.log.info("send daemon status, resync")
        shared.NODE.collector.call("push_daemon_status", data, last_status_changed)
        self.last_comm = datetime.datetime.utcnow()

    def ping(self):
        self.log.info("ping the collector")
        shared.NODE.collector.call("daemon_ping")
        self.last_comm = datetime.datetime.utcnow()

    def speaker(self):
        for nodename in self.sorted_cluster_nodes:
            if nodename in shared.CLUSTER_DATA and shared.CLUSTER_DATA[nodename] != "unknown":
                break
        if nodename == rcEnv.nodename:
            #self.log.debug("we are speaker", nodename)
            return True
        #self.log.debug("the speaker is %s", nodename)
        return False

    def get_data(self):
        """
        Get a copy of the monitor thread, expunged from encap services,
        to avoid missing changes happening during our work
        """
        data = shared.THREADS["monitor"].status()
        _data = {
            "nodes": {},
            "services": {},
        }
        for key in data:
            if key not in _data:
                _data[key] = data[key]

        for nodename in data["nodes"]:
            for svcname in list(data["nodes"][nodename]["services"]["status"].keys()):
                if data["nodes"][nodename]["services"]["status"][svcname].get("encap") is True:
                    continue
                if nodename not in _data["nodes"]:
                    _data["nodes"][nodename] = {
                        "services": {
                            "config": {},
                            "status": {},
                        },
                    }
                _data["nodes"][nodename]["services"]["status"][svcname] = data["nodes"][nodename]["services"]["status"][svcname]
                _data["nodes"][nodename]["services"]["config"][svcname] = data["nodes"][nodename]["services"]["config"][svcname]
                _data["services"][svcname] = data["services"][svcname]
        return _data

    def run_collector(self):
        data = self.get_data()
        if len(data["services"]) == 0:
            #self.log.debug("no service")
            return

        last_config, last_config_changed = self.get_last_config(data)
        for svcname in last_config_changed:
            self.send_service_config(svcname)
            self.send_containerinfo(svcname)
        self.last_config = last_config

        if self.speaker():
            last_status, last_status_changed = self.get_last_status(data)
            if last_status_changed != [] or self.last_comm is None:
                self.send_daemon_status(data, last_status_changed)
            elif self.last_comm <= datetime.datetime.utcnow() - datetime.timedelta(seconds=self.interval):
                self.ping()
            self.last_status = last_status


