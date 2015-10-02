from provisioning import Provisioning
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex

class ProvisioningFs(Provisioning):
    # required from child classes:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']

    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.section = dict(r.svc.config.items(r.rid))

    def check_fs(self):
        cmd = self.info + [self.mkfs_dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.r.log.info("%s is not formatted"%self.mkfs_dev)
        return False

    def provision_dev(self):
        if rcEnv.sysname == 'Linux':
            p = __import__("provLvLinux")
        elif rcEnv.sysname == 'HP-UX':
            p = __import__("provLvHP-UX")
        else:
            return
        p.ProvisioningLv(self.r).provision_lv()
           
    def provisioner_fs(self):
        for i in ('dev', 'mnt'):
            if i not in self.section:
                raise ex.excError("%s keyword is not set in section %s"%(i, r.rid))
            setattr(self, i, self.section[i])
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev):
            self.provision_dev()

        self.mkfs_dev = self.dev
        if rcEnv.sysname == 'HP-UX':
            l = self.dev.split('/')
            l[-1] = 'r'+l[-1]
            self.mkfs_dev = '/'.join(l)
            if not os.path.exists(self.mkfs_dev):
               self.r.log.error("%s raw device does not exists"%self.mkfs_dev)
               return

        if not self.check_fs():
            cmd = self.mkfs + [self.mkfs_dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.excError

    def provisioner(self):
        self.provisioner_fs()
        self.r.log.info("provisioned")
        self.r.start()
        return True
