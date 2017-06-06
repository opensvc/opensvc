from provisioning import Provisioning
from rcUtilities import justcall, which, protected_dir
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex
import shutil
from svcBuilder import conf_get_string_scope

class ProvisioningFs(Provisioning):
    # required from child classes:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']

    def __init__(self, r):
        Provisioning.__init__(self, r)

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

    def unprovision_dev(self):
        if rcEnv.sysname == 'Linux':
            p = __import__("provDiskLvLinux")
        else:
            return
        p.ProvisioningDisk(self.r).unprovisioner()

    def provisioner_fs(self):
        self.dev = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "dev")
        self.mnt = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "mnt")

        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev) and self.r.fs_type not in self.r.netfs:
            self.provision_dev()

        self.mkfs_dev = self.dev
        if rcEnv.sysname == 'HP-UX':
            l = self.dev.split('/')
            l[-1] = 'r'+l[-1]
            self.mkfs_dev = '/'.join(l)
            if not os.path.exists(self.mkfs_dev):
                self.r.log.error("%s raw device does not exists"%self.mkfs_dev)
                return
        elif rcEnv.sysname == 'Darwin':
            if os.path.isfile(self.mkfs_dev):
                from rcLoopDarwin import file_to_loop
                devs = file_to_loop(self.mkfs_dev)
                if len(devs) == 1:
                    self.mkfs_dev = devs[0]
                else:
                    raise ex.excError("unable to find a device associated to %s" % self.mkfs_dev)

        if not os.path.exists(self.mkfs_dev):
            raise ex.excError("abort fs provisioning: %s does not exist" % self.mkfs_dev)

        if self.check_fs():
            self.r.log.info("already provisioned")
            return

        if hasattr(self, "do_mkfs"):
            self.do_mkfs()
        elif hasattr(self, "mkfs"):
            try:
                opts = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "mkfs_opt").split()
            except:
                opts = []
            cmd = self.mkfs + opts + [self.mkfs_dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.excError
        else:
            raise ex.excError("no mkfs method implemented")

        self.r.log.info("provisioned")


    def provisioner(self):
        if "bind" in self.r.mount_options:
            return
        self.provisioner_fs()
        self.r.start()

    def purge_mountpoint(self):
        if os.path.exists(self.r.mount_point) and not protected_dir(self.r.mount_point):
            self.r.log.info("rm -rf %s" % self.r.mount_point)
            try:
                shutil.rmtree(self.r.mount_point)
            except Exception as e:
                raise ex.excError(str(e))

    def unprovisioner_fs(self):
        pass

    def unprovisioner(self):
        self.r.stop()
        self.unprovisioner_fs()
        self.purge_mountpoint()
        self.unprovision_dev()


