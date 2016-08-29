from provisioning import Provisioning

class ProvisioningIp(Provisioning):
    def provisioner(self):
        self.r.start()

    def unprovisioner(self):
        self.r.stop()
