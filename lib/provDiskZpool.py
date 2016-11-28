from provisioning import Provisioning
import rcExceptions as ex
from svcBuilder import conf_get_string_scope

class ProvisioningDisk(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def unprovisioner(self):
        self.r.stop()

        if not self.r.has_it():
            self.r.log.info("already unprovisionned")
            return

        cmd = ["zpool", "destroy", "-f", self.r.name]
        self.r.vcall(cmd)

        self.r.log.info("unprovisionned")

    def provisioner(self):
        try:
            self.name = self.r.name
            self.vdev = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vdev").split()
        except Exception as e:
            raise ex.excError(str(e))

        if self.r.has_it():
            self.r.log.info("already provisionned")
            self.r.start()
            return

        cmd = ["zpool", "create", "-m", "legacy", self.name] + self.vdev
        self.r.vcall(cmd)

        self.r.log.info("provisioned")
        self.r.start()
