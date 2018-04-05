import provisioning
from rcUtilities import justcall, which, protected_dir
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex
import shutil

class Prov(provisioning.Prov):
    # required from child classes:
    #   mkfs = ['mkfs.ext4', '-F']
    #   info = ['tune2fs', '-l']

    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def check_fs(self):
        if not hasattr(self, "info"):
            return True
        if self.mkfs_dev is None:
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
        p.Prov(self.r).provisioner()

    def unprovision_dev(self):
        if rcEnv.sysname == 'Linux':
            p = __import__("provDiskLvLinux")
        else:
            return
        p.Prov(self.r).unprovisioner()

    def is_provisioned(self):
        if "bind" in self.r.mount_options:
            return True
        try:
            self.dev = self.r.conf_get("dev")
            self.mnt = self.r.conf_get("mnt")
        except ex.OptNotFound:
            return
        if self.dev is None:
            return
        if self.mnt is None:
            return
        if not os.path.exists(self.mnt):
            return False
        if self.r.fs_type in self.r.netfs:
            return True
        try:
            self.get_mkfs_dev()
        except ex.excError:
            self.mkfs_dev = None
        if not os.path.exists(self.dev) and (self.mkfs_dev is None or not os.path.exists(self.mkfs_dev)):
            return False
        return self.check_fs()

    def get_mkfs_dev(self):
        self.mkfs_dev = self.dev
        if rcEnv.sysname == 'HP-UX':
            l = self.dev.split('/')
            l[-1] = 'r'+l[-1]
            self.mkfs_dev = '/'.join(l)
            if not os.path.exists(self.mkfs_dev):
                raise ex.excError("%s raw device does not exists"%self.mkfs_dev)
        elif rcEnv.sysname == 'Darwin':
            if os.path.isfile(self.mkfs_dev):
                from rcLoopDarwin import file_to_loop
                devs = file_to_loop(self.mkfs_dev)
                if len(devs) == 1:
                    self.mkfs_dev = devs[0]
                else:
                    raise ex.excError("unable to find a device associated to %s" % self.mkfs_dev)
        elif rcEnv.sysname == 'Linux':
            if os.path.isfile(self.mkfs_dev):
                from rcLoopLinux import file_to_loop
                devs = file_to_loop(self.mkfs_dev)
                if len(devs) == 1:
                    self.mkfs_dev = devs[0]
                else:
                    raise ex.excError("unable to find a device associated to %s" % self.mkfs_dev)

    def provisioner_fs(self):
        if self.r.fs_type in self.r.netfs:
            return

        self.dev = self.r.conf_get("dev")
        self.mnt = self.r.conf_get("mnt")

        if self.dev is None:
            raise ex.excError("device not found. parent resource is down ?")
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.r.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev) and self.r.fs_type not in self.r.netfs:
            try:
                self.r.conf_get("vg", verbose=False)
                self.provision_dev()
            except ValueError:
                # keyword not supported (ex. bind mounts)
                pass
            except ex.OptNotFound:
                pass

        self.get_mkfs_dev()

        if not os.path.exists(self.mkfs_dev):
            raise ex.excError("abort fs provisioning: %s does not exist" % self.mkfs_dev)

        if self.check_fs():
            self.r.log.info("already provisioned")
            return

        if hasattr(self, "do_mkfs"):
            self.do_mkfs()
        elif hasattr(self, "mkfs"):
            try:
                opts = self.r.svc.conf_get(self.r.rid, "mkfs_opt")
            except:
                opts = []
            cmd = self.mkfs + opts + [self.mkfs_dev]
            (ret, out, err) = self.r.vcall(cmd)
            if ret != 0:
                self.r.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.excError
        else:
            raise ex.excError("no mkfs method implemented")

    def provisioner(self):
        self.r.unset_lazy("device")
        self.r.unset_lazy("label")
        if "bind" in self.r.mount_options:
            return
        self.provisioner_fs()

    def purge_mountpoint(self):
        if self.r.mount_point is None:
            return
        if os.path.exists(self.r.mount_point) and not protected_dir(self.r.mount_point):
            self.r.log.info("rm -rf %s" % self.r.mount_point)
            try:
                shutil.rmtree(self.r.mount_point)
            except Exception as e:
                raise ex.excError(str(e))

    def unprovisioner_fs(self):
        pass

    def unprovisioner(self):
        if self.r.fs_type in self.r.netfs:
            return
        self.unprovisioner_fs()
        self.purge_mountpoint()
        if self.r.fs_type not in self.r.netfs:
            try:
                self.r.conf_get("vg", verbose=False)
                self.unprovision_dev()
            except ValueError:
                # keyword not supported (ex. bind mounts)
                return
            except ex.OptNotFound:
                pass


