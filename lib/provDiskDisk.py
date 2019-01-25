import provisioning
import rcExceptions as ex
from rcColor import format_str_flat_json

class Prov(provisioning.Prov):

    def is_provisioned(self):
        if self.r.disk_id:
            return True
        return False

    def provisioner(self):
        if self.r.disk_id:
            self.r.log.info("skip provision: 'disk_id' is already set")
            return
        poolname = self.r.conf_get("pool")
        name = self.r.conf_get("name")
        size = self.r.conf_get("size")
        pool = self.r.svc.node.get_pool(poolname)
        result = pool.create_disk(name, size=size)
        for line in format_str_flat_json(result).splitlines():
            self.r.log.info(line)
        self.r.svc.set_multi(["%s.%s=%s" % (self.r.rid, "disk_id", result["disk_id"])])
        self.r.unset_lazy("disk_id")
        self.r.log.info("disk %s provisioned" % result["disk_id"])

    def unprovisioner(self):
        if not self.r.disk_id:
            self.r.log.info("skip unprovision: 'disk_id' is not set")
            return
        poolname = self.r.conf_get("pool")
        name = self.r.conf_get("name")
        pool = self.r.svc.node.get_pool(poolname)
        result = pool.delete_disk(name)
        for line in format_str_flat_json(result).splitlines():
            self.r.log.info(line)
        self.r.svc.set_multi(["%s.%s=%s" % (self.r.rid, "disk_id", "")])
        self.r.unset_lazy("disk_id")
        self.r.log.info("unprovisioned")

