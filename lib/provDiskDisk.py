import provisioning
import rcExceptions as ex

class Prov(provisioning.Prov):

    def provision(self):
        if self.r.disk_id is not None:
            self.r.log.info("skip provision: 'disk_id' is already set")
            return
        try:
            size = self.r.svc.conf_get(self.r.rid, "size")
        except:
            raise ex.excError("disk %s: missing the 'size' provisioning parameter" % self.r.rid)
        try:
            slo = self.r.svc.conf_get(self.r.rid, "slo")
        except:
            slo = None

        handler = "/services/self.r/disks"
        data = {
            "action": "provision",
            "size": size,
            "array_name": self.r.array_name,
            "diskgroup": self.r.diskgroup,
        }
        if slo is not None:
            data["slo"] = slo
        results = self.r.svc.collector_rest_put(handler, data)
        if "error" in results:
            raise ex.excError(results["error"])
        self.r.log.info("disk provision request sent to the collector (id %d). "
                      "waiting for completion." % results["results_id"])

        results = self.r.wait_results(results)
        self.r.disk_id = results["outputs"]["add disk"][0]["disk_id"]
        self.r.set_label()
        self.r.svc.config.set(self.r.rid, "disk_id", self.r.disk_id)
        self.r.svc.write_config()
        self.r.log.info("disk %s provisioned" % self.r.disk_id)
        self.r.svc.node.unset_lazy("devtree")

    def unprovision(self):
        handler = "/services/self.r/disks"
        data = {
            "action": "unprovision",
            "disk_id": self.r.disk_id,
        }
        results = self.r.svc.collector_rest_put(handler, data)
        if "error" in results:
            raise ex.excError(results["error"])
        self.r.log.info("disk unprovision request sent to the collector (id %d). "
                      "waiting for completion." % results["results_id"])
        results = self.r.wait_results(results)
        self.r.svc.config.set(self.r.rid, "disk_id", "")
        self.r.svc.write_config()
        self.r.log.info("unprovisioned")
        self.r.svc.node.unset_lazy("devtree")

