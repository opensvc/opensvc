"""
SCSI Disk provisioning driver.
Relies on array drivers via pools.
"""
import provisioning
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcColor import format_str_flat_json

class Prov(provisioning.Prov):

    def is_provisioned(self):
        if self.r.disk_id:
            return True
        return False

    def provisioner(self):
        if self.r.disk_id:
            self.r.log.info("skip disk creation: the disk_id keyword is already set")
            self.r.configure()
        else:
            self.create_disk()
            self.r.configure(force=True)

    def create_disk(self):
        poolname = self.r.oget("pool")
        name = self.r.oget("name")
        size = self.r.oget("size")
        pool = self.r.svc.node.get_pool(poolname)
        pool.log = self.r.log
        if self.r.shared:
            disk_id_kw = "disk_id"
            result = pool.create_disk(name, size=size, nodes=self.r.svc.nodes)
        else:
            disk_id_kw = "disk_id@" + rcEnv.nodename
            name += "." + rcEnv.nodename
            result = pool.create_disk(name, size=size, nodes=[rcEnv.nodename])
        if not result:
            raise ex.excError("invalid create disk result: %s" % result)
        for line in format_str_flat_json(result).splitlines():
            self.r.log.info(line)
        changes = []
        if "disk_ids" in result:
            for node, disk_id in result["disk_ids"].items():
                changes.append("%s.disk_id@%s=%s" % (self.r.rid, node, disk_id))
        elif "disk_id" in result:
            disk_id = result["disk_id"]
            changes.append("%s.%s=%s" % (self.r.rid, disk_id_kw, disk_id))
        else:
            raise ex.excError("no disk id found in result")
        self.r.log.info("changes: %s", changes)
        self.r.svc.set_multi(changes, validation=False)
        self.r.log.info("changes applied")
        self.r.unset_lazy("disk_id")
        self.r.log.info("disk %s provisioned" % result["disk_id"])

    def provisioner_shared_non_leader(self):
        self.r.configure()

    def unprovisioner_shared_non_leader(self):
        self.r.unconfigure()

    def unprovisioner(self):
        if not self.r.disk_id:
            self.r.log.info("skip unprovision: 'disk_id' is not set")
            return
        self.r.unconfigure()
        poolname = self.r.oget("pool")
        name = self.r.oget("name")
        pool = self.r.svc.node.get_pool(poolname)
        pool.log = self.r.log
        if self.r.shared:
            disk_id_kw = "disk_id"
        else:
            disk_id_kw = "disk_id@" + rcEnv.nodename
            name += "." + rcEnv.nodename
        result = pool.delete_disk(name=name, disk_id=self.r.disk_id)
        for line in format_str_flat_json(result).splitlines():
            self.r.log.info(line)
        self.r.svc.set_multi(["%s.%s=%s" % (self.r.rid, disk_id_kw, "")], validation=False)
        self.r.unset_lazy("disk_id")
        self.r.log.info("unprovisioned")

