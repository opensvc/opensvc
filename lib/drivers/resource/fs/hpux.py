import os

import rcExceptions as ex
import rcStatus

from . import BaseFs, adder as base_adder
from rcGlobalEnv import rcEnv
from rcUtilities import qcall, protected_mount

rcMounts = __import__('rcMounts'+rcEnv.sysname)

def adder(svc, s):
    base_adder(svc, s, drv=Fs)

def try_umount(self):
    cmd = ['umount', self.mount_point]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

    """ don't try to kill process using the source of a
        protected bind mount
    """
    if protected_mount(self.mount_point):
        return 1

    """ best effort kill of all processes that might block
        the umount operation. The priority is given to mass
        action reliability, ie don't contest oprator's will
    """
    cmd = ['sync']
    (ret, out, err) = self.vcall(cmd, err_to_info=True)

    for i in range(4):
        cmd = ['fuser', '-kc', self.mount_point]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mount_point)
        cmd = ['umount', self.mount_point]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Fs(BaseFs):
    def set_fsck_h(self):
        self.fsck_h = {
            'vxfs': {
                'bin': 'fsck',
                'cmd': ['fsck', '-F', 'vxfs', '-y', self.device]
            },
        }

    def is_up(self):
        return rcMounts.Mounts().has_mount(self.device, self.mount_point)

    def start(self):
        super(Fs, self).start()
        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0
        self.fsck()
        if not os.path.exists(self.mount_point):
            os.makedirs(self.mount_point, 0o755)
        if self.fs_type != "":
            fstype = ['-F', self.fs_type]
        else:
            fstype = []
        if self.mount_options != "":
            mntopt = ['-o', self.mount_options]
        else:
            mntopt = []
        cmd = ['mount']+fstype+mntopt+[self.device, self.mount_point]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.can_rollback = True

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return 0
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mount_point)
            raise ex.excError

    def lv_name(self):
        return os.path.basename(self.device)
