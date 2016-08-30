from provisioning import Provisioning
from rcUtilities import justcall, which, protected_dirs
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex
import shutil

class ProvisioningFs(Provisioning):
    # required from child classes:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']

    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.section = dict(r.svc.config.items(r.rid))

    def check_fs(self):
        if not hasattr(self, "info"):
            return True
        cmd = self.info + [self.mkfs_dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.r.log.info("%s is not formatted"%self.mkfs_dev)
        return False

    def provision_dev(self):
        if rcEnv.sysname == 'Linux':
            p = __import__("provDiskLvLinux")
        elif rcEnv.sysname == 'HP-UX':
            p = __import__("provDiskLvHP-UX")
        else:
            return
        p.ProvisioningDisk(self.r).provisioner()

    def provisioner_fs(self):
        for i in ('dev', 'mnt'):
            if i not in self.section:
                raise ex.excError("%s keyword is not set in section %s"%(i, r.rid))
            setattr(self, i, self.section[i])
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev) and self.r.fsType not in self.r.netfs:
            self.r.log.info("dev %s does not exist. create a logical volume"%self.dev)
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
            if not hasattr(self, "mkfs"):
                raise ex.excError("no mkfs method implemented")
            cmd = self.mkfs + [self.mkfs_dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.excError
            self.r.log.info("provisioned")
        else:
            self.r.log.info("already provisioned")

        self.remove_keywords(["size", "vg"])

    def provisioner(self):
        self.provisioner_fs()
        self.r.start()

    def unprovisioner_fs(self):
        pass

    def unprovisioner(self):
        self.r.stop()
        self.unprovisioner_fs()
        if os.path.exists(self.r.mountPoint) and not self.r.mountPoint in protected_dirs:
            self.r.log.info("rm -rf %s" % self.r.mountPoint)
            try:
                shutil.rmtree(self.r.mountPoint)
            except Exception as e:
                raise ex.excError(str(e))
