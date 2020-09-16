import os
import re
import time

import core.exceptions as ex
from env import Env
from utilities.subsystems.zfs import zfs_getprop, zfs_setprop
from utilities.mounts.sunos import Mounts
from . import BaseFs
from utilities.proc import justcall
from utilities.lazy import lazy

DRIVER_GROUP = "fs"
DRIVER_BASENAME = ""

class Fs(BaseFs):
    """
    SunOS fs resource driver.
    """
    def __init__(self, **kwargs):
        super(Fs, self).__init__(**kwargs)

    @lazy
    def rdevice(self):
        return self.device.replace('/dsk/', '/rdsk/', 1)

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
        mounts = Mounts()
        return mounts.has_mount(self.device, self.mount_point)

    def start_mount(self):
        self.prepare_mount()
        m = re.match(r"<(\w+)>", self.raw_mount_point)
        if m:
            # the zone was not created when the service was built. now it should,
            # so try the redetect the zonepath
            zone = m.group(1)
            for r in self.svc.get_resources("container.zone"):
                if r.name == zone:
                    zonepath = r.get_zonepath()
                    self.raw_mount_point = re.sub(r"<\w+>", zonepath, self.raw_mount_point)
                    self.unset_lazy("mount_point")

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
                raise ex.Error
        if zfs_getprop(self.device, 'mountpoint') == "legacy":
            self.mount_generic()
        else:
            self.mount_zfs_native()

    def mount_zfs_native(self):
        if zfs_getprop(self.device, 'mountpoint') != self.mount_point:
            if not zfs_setprop(self.device, 'mountpoint', self.mount_point, log=self.log):
                raise ex.Error

        if self.is_up() is True:
            return

        try:
            os.unlink(self.mount_point+"/.opensvc")
        except:
            pass
        ret, out, err = self.vcall([Env.syspaths.zfs, 'mount', self.device])
        if ret != 0:
            ret, out, err = self.vcall([Env.syspaths.zfs, 'mount', '-O', self.device])
            if ret != 0:
                raise ex.Error
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
            raise ex.Error

        self.can_rollback = True

    def can_check_writable(self):
        if self.fs_type != 'zfs':
            return True
        pool = self.device.split("/")[0]
        cmd = [Env.syspaths.zpool, "status", pool]
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
                    raise ex.Error
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
        raise ex.Error

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return

        try:
            self.try_umount()
        except:
            self.log.error("failed")
            raise ex.Error
