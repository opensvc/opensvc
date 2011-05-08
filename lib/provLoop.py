from provisioning import Provisioning

class ProvisioningLoop(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.path = r.svc.config.get(self.r.rid, 'file')
        self.size = r.svc.config.get(self.r.rid, 'size')

    def validate(self):
        import os
        if os.path.exists(self.path):
            return True
        self.r.log.error("%s does not exist"%self.path)
        return False

    def provisioner(self):
        try:
            with open(self.path, 'w') as f:
                f.seek(int(self.size)*1024*1024)
                f.write('\0')
        except:
            self.r.log.error("Failed to create %s"%self.path)
            return False

        self.r.log.info("provisioned")
        self.r.start()
        return True
