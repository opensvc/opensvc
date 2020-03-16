import os
import time
import re

import rcMountsSunOS as rcMounts
import resFs as Res
import rcExceptions as ex
from rcZfs import zfs_getprop, zfs_setprop
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

def adder(svc, s):
    Res.adder(svc, s, drv=Mount)

class Mount(Res.Mount):
    """
    SunOS fs resource driver.
    """
    def __init__(self,
                 rid,
                 mount_point,
                 device,
                 fs_type,
                 mount_options,
                 snap_size=None,
                 **kwargs):
        self.rdevice = device.replace('/dsk/', '/rdsk/', 1)
        Res.Mount.__init__(self,
                           rid=rid,
                           mount_point=mount_point,
                           device=device,
                           fs_type=fs_type,
                           mount_options=mount_options,
                           snap_size=snap_size,
                           **kwargs)

    def set_fsck_h(self):
        self.fsck_h = {
            'ufs': {
                'bin': 'fsck',
                'cmd': ['fsck', '-F', 'ufs', '-y', self.rdevice],
                'reportcmd': ['fsck', '-F', 'ufs', '-m', self.rdevice],
                'reportclean': [32],
            },
            'vxfs': {
                'bin': 'fsck',
                'cmd': ['fsck', '-F', 'vxfs', '-y', self.rdevice],
                'reportcmd': ['fsck', '-F', 'vxfs', '-m', self.rdevice],
                'reportclean': [32],
            },
        }

    def is_up(self):
        mounts = rcMounts.Mounts()
        return mounts.has_mount(self.device, self.mount_point)

    def start(self):
        Res.Mount.start(self)
        m = re.match("<(\w+)>", self.mount_point)
        if m:
            # the zone was not created when the service was built. now it should,
            # so try the redetect the zonepath
            zone = m.group(1)
            for r in self.svc.get_resources("container.zone"):
                if r.name == zone:
                    zonepath = r.get_zonepath()
                    self.mount_point = re.sub("<\w+>", zonepath, self.mount_point)

        if self.fs_type == 'zfs':
            if 'noaction' not in self.tags and zfs_getprop(self.device, 'canmount') != 'noauto':
                self.log.info("%s should be set to canmount=noauto (zfs set canmount=noauto %s)"%(self.label, self.device))

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return

        if self.fs_type == 'zfs':
            return self.mount_zfs()
        return self.mount_generic()

    def mount_zfs(self):
        zone = self.svc.oget(self.rid, "zone")
        if not self.encap and not zone and zfs_getprop(self.device, 'zoned') != 'off':
            if zfs_setprop(self.device, 'zoned', 'off', log=self.log):
                raise ex.excError
        if zfs_getprop(self.device, 'mountpoint') == "legacy":
            self.mount_generic()
        else:
            self.mount_zfs_native()

    def mount_zfs_native(self):
        if zfs_getprop(self.device, 'mountpoint') != self.mount_point:
            if not zfs_setprop(self.device, 'mountpoint', self.mount_point, log=self.log):
                raise ex.excError

        if self.is_up() is True:
            return

        try:
            os.unlink(self.mount_point+"/.opensvc")
        except:
            pass
        ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'mount', self.device])
        if ret != 0:
            ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'mount', '-O', self.device])
            if ret != 0:
                raise ex.excError
        self.can_rollback = True

    def mount_generic(self):
        if self.fs_type != "":
            fstype = ['-F', self.fs_type]
            self.fsck()
        else:
            fstype = []

        if self.mount_options != "":
            mntopt = ['-o', self.mount_options]
        else:
            mntopt = []

        if not os.path.exists(self.mount_point):
            os.makedirs(self.mount_point, 0o755)

        for i in range(3):
            ret = self.try_mount(fstype, mntopt)
            if ret == 0:
                break
            time.sleep(1)


        if ret != 0:
            raise ex.excError

        self.can_rollback = True

    def can_check_writable(self):
        if self.fs_type != 'zfs':
            return True
        pool = self.device.split("/")[0]
        cmd = [rcEnv.syspaths.zpool, "status", pool]
        out, err, ret = justcall(cmd)
        if "state: SUSPENDED" in out:
            self.status_log("pool %s is suspended")
            return False
        return True

    def try_mount(self, fstype, mntopt):
        cmd = ['mount'] + fstype + mntopt + [self.device, self.mount_point]
        ret, out, err = self.vcall(cmd)
        return ret

    def try_umount(self):
        if self.fs_type == 'zfs' and zfs_getprop(self.device, 'mountpoint') != "legacy":
            ret, out, err = self.vcall(['zfs', 'umount', self.device], err_to_info=True)
            if ret != 0:
                ret, out, err = self.vcall(['zfs', 'umount', '-f', self.device], err_to_info=True)
                if ret != 0:
                    raise ex.excError
            return
        (ret, out, err) = self.vcall(['umount', self.mount_point], err_to_info=True)
        if ret == 0:
            return
        for i in range(4):
            ret, out, err = self.vcall(['fuser', '-ck', self.mount_point],
                                       err_to_info=True)
            ret, out, err = self.vcall(['umount', self.mount_point],
                                       err_to_info=True)
            if ret == 0:
                return
            if self.fs_type != 'lofs':
                ret, out, err = self.vcall(['umount', '-f', self.mount_point],
                                           err_to_info=True)
                if ret == 0:
                    return
        raise ex.excError

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return

        try:
            self.try_umount()
        except:
            self.log.error("failed")
            raise ex.excError

if __name__ == "__main__":
    for c in (Mount,):
        help(c)
