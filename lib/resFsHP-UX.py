import os

import rcStatus
from rcGlobalEnv import rcEnv
rcMounts = __import__('rcMounts'+rcEnv.sysname)
import resFs as Res
from rcUtilities import qcall, protected_mount
import rcExceptions as ex

def adder(svc, s):
    Res.adder(svc, s, drv=Mount)

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


class Mount(Res.Mount):
    """ define HP-UX mount/umount doAction """
    def __init__(self,
                 rid,
                 mount_point,
                 device,
                 fs_type,
                 mount_options,
                 snap_size=None,
                 **kwargs):
        Res.Mount.__init__(self,
                           rid,
                           mount_point=mount_point,
                           device=device,
                           fs_type=fs_type,
                           mount_options=mount_options,
                           snap_size=snap_size,
                           **kwargs)

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
        Res.Mount.start(self)
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

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

