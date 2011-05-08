from provisioning import Provisioning
from rcUtilities import justcall
import os

class ProvisioningFs(Provisioning):
    # required from children:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']
    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.section = dict(r.svc.config.items(r.rid))
        self.dev = self.section['dev']
        self.mnt = self.section['mnt']

    def check_fs(self):
        cmd = self.info + [self.dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.r.log.info("%s is not formatted"%self.dev)
        return False

    def provisioner(self):
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev):
            self.r.log.error("%s device does not exist"%self.dev)
            return False

        if not self.check_fs():
            cmd = self.mkfs + [self.dev]
            (err, out) = self.r.vcall(cmd)
            if err != 0:
                self.r.log.error('Failed to format %s'%self.dev)
                return False

        self.r.log.info("provisioned")
        self.r.start()
        return True
