from __future__ import print_function

import json
import time

import resources as Res
from rcGlobalEnv import rcEnv
from svcBuilder import conf_get_string_scope, conf_get_int_scope
from rcUtilities import lazy
import rcExceptions as ex

class Disk(Res.Resource):
    """ SAN Disk resource
    """
    def __init__(self, rid=None, disk_id=None, **kwargs):
        Res.Resource.__init__(self, rid, "disk.disk", **kwargs)
        self.disk_id = disk_id if disk_id != "" else None
        self.set_label()

    def set_label(self):
        if self.disk_id is None:
            self.label = "unprovisionned disk"
        else:
            self.label = "disk "+str(self.disk_id)

    def info(self):
        return self.fmt_info([
            ["disk_id", self.disk_id],
        ])

    def __str__(self):
        return "%s disk disk_id=%s" % (
            Res.Resource.__str__(self),
            str(self.disk_id),
        )

    @lazy
    def array_name(self):
        try:
            return conf_get_string_scope(self.svc, self.svc.config, self.rid, "array")
        except:
            raise ex.excError("disk %s: missing the 'array' provisioning parameter" % self.rid)

    @lazy
    def diskgroup(self):
        try:
            return conf_get_string_scope(self.svc, self.svc.config, self.rid, "diskgroup")
        except:
            raise ex.excError("disk %s: missing the 'diskgroup' provisioning parameter" % self.rid)

    @lazy
    def array_id(self):
        data = self.svc.collector_rest_get("/arrays", {
            "filters": "array_name "+self.array_name,
            "props": "id",
        })
        if "error" in data:
            raise ex.excError(data["error"])
        if data["meta"]["total"] != 1:
            raise ex.excError("array %s has %d matching candidates" % (self.array_name, data["meta"]["total"]))
        return data["data"][0]["id"]

    def get_form_id(self, form_name):
        data = self.svc.collector_rest_get("/forms", {
            "filters": "form_name "+form_name,
            "props": "id",
        })
        if "error" in data:
            raise ex.excError(data["error"])
        if data["meta"]["total"] != 1:
            raise ex.excError("form %s has %d matching candidates" % (form_name, data["meta"]["total"]))
        return data["data"][0]["id"]

    def wait_results(self, results):
        """
        Ask the collector for a submitted form-based action results
        until the action is completed.

        The results format is:
        {
            'status': 'QUEUED',
            'request_data': {},
            'returncode': 0,
            'log': {
                'output-0': [
                ],
            },
            'outputs': {
                'output-0': {
                },
            },
            'results_id': 300,
            'outputs_order': ['output-0']
        }
        """
        logs = {}

        while True:
            data = self.get_results(results)
            if "error" in data:
                raise ex.excError(data["error"])

            for output, log in data["log"].items():
                if output not in logs:
                    logs[output] = 0
                if len(log) > logs[output]:
                    for lvl, fmt, d in log[logs[output]:]:
                        if len(fmt) == 0:
                            continue
                        try:
                            msg = fmt % d
                        except:
                            msg = "corrupted collector request log line"
                        if lvl == 0:
                            self.log.info(msg)
                        else:
                            self.log.error(msg)
                    logs[output] = len(log)
            if data["status"] == "COMPLETED":
                if data["returncode"] != 0:
                    raise ex.excError("collector request completed with errors")
                return data
            time.sleep(1)

    def get_results(self, results):
        data = self.svc.collector_rest_get("/form_output_results/%d" % results["results_id"])
        return data

    def provision(self):
        if self.disk_id is not None:
            self.log.info("skip provision: 'disk_id' is already set")
            return
        try:
            size = conf_get_string_scope(self.svc, self.svc.config, self.rid, "size")
        except:
            raise ex.excError("disk %s: missing the 'size' provisioning parameter" % self.rid)
        try:
            slo = conf_get_string_scope(self.svc, self.svc.config, self.rid, "slo")
        except:
            slo = None

        handler = "/services/self/disks"
        data = {
            "action": "provision",
            "size": size,
            "array_name": self.array_name,
            "diskgroup": self.diskgroup,
        }
        if slo is not None:
            data["slo"] = slo
        results = self.svc.collector_rest_put(handler, data)
        if "error" in results:
            raise ex.excError(results["error"])
        self.log.info("disk provision request sent to the collector (id %d). "
                      "waiting for completion." % results["results_id"])

        results = self.wait_results(results)
        self.disk_id = results["outputs"]["add disk"][0]["disk_id"]
        self.set_label()
        self.svc.config.set(self.rid, "disk_id", self.disk_id)
        self.svc.write_config()
        self.log.info("disk %s provisionned" % self.disk_id)

    def unprovision(self):
        handler = "/services/self/disks"
        data = {
            "action": "unprovision",
            "disk_id": self.disk_id,
        }
        results = self.svc.collector_rest_put(handler, data)
        if "error" in results:
            raise ex.excError(results["error"])
        self.log.info("disk unprovision request sent to the collector (id %d). "
                      "waiting for completion." % results["results_id"])
        results = self.wait_results(results)
        self.svc.config.set(self.rid, "disk_id", "")
        self.svc.write_config()
        self.log.info("unprovisionned")

