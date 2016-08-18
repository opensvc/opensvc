from provisioning import Provisioning
import rcExceptions as ex
from rcUtilities import convert_size
from svcBuilder import conf_get_string_scope, conf_get_int_scope

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
            size = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "size")
        except:
            raise ex.excError("gce disk name %s in %s: missing the 'size' parameter" % (name, self.r.rid))
        size = str(convert_size(size, _to="MB"))+'MB'

        cmd = ["gcloud", "compute", "disks", "create", "-q",
               name,
               "--size", size,
               "--zone", self.r.gce_zone]

        try:
            description = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "description")
            cmd += ["--description", description]
        except:
            pass

        try:
            image = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "image")
            cmd += ["--image", image]
        except:
            pass

        try:
            source_snapshot = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "source_snapshot")
            cmd += ["--source-snapshot", source_snapshot]
        except:
            pass

        try:
            image_project = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "image_project")
            cmd += ["--image-project", image_project]
        except:
            pass

        try:
            disk_type = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "disk_type")
            cmd += ["--type", disk_type]
        except:
            pass

        self.r.vcall(cmd)


