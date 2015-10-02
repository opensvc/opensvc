from provisioning import Provisioning
import os
import rcExceptions as ex

class ProvisioningLoop(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.path = r.svc.config.get(self.r.rid, 'file')
        self.size = r.svc.config.get(self.r.rid, 'size')

    def provisioner(self):
        d = os.path.dirname(self.path)
        try:
            if not os.path.exists(d):
                self.r.log.info("create directory %s"%d)
                os.makedirs(d)
            with open(self.path, 'w') as f:
                self.r.log.info("create file %s, size %sMB"%(self.path, self.size))
                f.seek(int(self.size)*1024*1024)
                f.write('\0')
        except:
            self.r.log.error("Failed to create %s"%self.path)
            raise ex.excError

        self.remove_keywords(["size"])
        self.r.log.info("provisioned")
        self.r.start()
