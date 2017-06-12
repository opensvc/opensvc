from provisioning import Provisioning
import rcExceptions as ex
from rcUtilities import convert_size

class ProvisioningDisk(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provisioner(self):
        for name in self.r.names:
            self._provisioner(name)
        self.r.log.info("provisioned")
        self.r.get_disks(refresh=True)
        self.r.start()
        return True

    def _provisioner(self, name):
        disk_names = self.r.get_disk_names()
        if name in disk_names:
            self.r.log.info("gce disk name %s already provisioned" % name)
            return

        try:
            size = self.r.svc.conf_get_string_scope(self.r.rid, "size")
        except:
            raise ex.excError("gce disk name %s in %s: missing the 'size' parameter" % (name, self.r.rid))
        size = str(convert_size(size, _to="MB"))+'MB'

        cmd = ["gcloud", "compute", "disks", "create", "-q",
               name,
               "--size", size,
               "--zone", self.r.gce_zone]

        try:
            description = self.r.svc.conf_get_string_scope(self.r.rid, "description")
            cmd += ["--description", description]
        except:
            pass

        try:
            image = self.r.svc.conf_get_string_scope(self.r.rid, "image")
            cmd += ["--image", image]
        except:
            pass

        try:
            source_snapshot = self.r.svc.conf_get_string_scope(self.r.rid, "source_snapshot")
            cmd += ["--source-snapshot", source_snapshot]
        except:
            pass

        try:
            image_project = self.r.svc.conf_get_string_scope(self.r.rid, "image_project")
            cmd += ["--image-project", image_project]
        except:
            pass

        try:
            disk_type = self.r.svc.conf_get_string_scope(self.r.rid, "disk_type")
            cmd += ["--type", disk_type]
        except:
            pass

        self.r.vcall(cmd)


    def unprovisioner(self):
        self.r.stop()
        for name in self.r.names:
            self._unprovisioner(name)
        self.r.log.info("unprovisioned")
        return True

    def _unprovisioner(self, name):
        disk_names = self.r.get_disk_names()
        if name not in disk_names:
            self.r.log.info("gce disk name %s already unprovisioned" % name)
            return

        cmd = ["gcloud", "compute", "disks", "delete", "-q", name,
               "--zone", self.r.gce_zone]

        self.r.vcall(cmd)


