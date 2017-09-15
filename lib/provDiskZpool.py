from provisioning import Provisioning
import rcExceptions as ex

class Prov(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def unprovisioner(self):
        self.r.stop()

        if not self.r.has_it():
            self.r.log.info("already unprovisioned")
            return

        cmd = ["zpool", "destroy", "-f", self.r.name]
        self.r.vcall(cmd)

        self.r.log.info("unprovisioned")

    def provisioner(self):
        try:
            self.name = self.r.name
            self.vdev = self.r.svc.conf_get(self.r.rid, "vdev")
        except Exception as e:
            raise ex.excError(str(e))

        if self.r.has_it():
            self.r.log.info("already provisioned")
            self.r.start()
            return

        cmd = ["zpool", "create", "-m", "legacy", self.name] + self.vdev
        self.r.vcall(cmd)

        self.r.log.info("provisioned")
        self.r.start()
