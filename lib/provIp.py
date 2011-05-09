from provisioning import Provisioning

class ProvisioningIp(Provisioning):
    def provisioner(self):
        self.r.start()
