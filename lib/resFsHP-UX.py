import os

import rcStatus
from rcGlobalEnv import rcEnv
rcMounts = __import__('rcMounts'+rcEnv.sysname)
import resFs as Res
from rcUtilities import qcall, protected_mount
import rcExceptions as ex

def try_umount(self):
    cmd = ['umount', self.mountPoint]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

    """ don't try to kill process using the source of a
        protected bind mount
    """
    if protected_mount(self.mountPoint):
        return 1

    """ best effort kill of all processes that might block
        the umount operation. The priority is given to mass
        action reliability, ie don't contest oprator's will
    """
    cmd = ['sync']
    (ret, out, err) = self.vcall(cmd, err_to_info=True)

    for i in range(4):
        cmd = ['fuser', '-kc', self.mountPoint]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Mount(Res.Mount):
    """ define HP-UX mount/umount doAction """
    def __init__(self,
                 rid,
                 mountPoint,
                 device,
                 fsType,
                 mntOpt,
                 snap_size=None,
                 always_on=set([]),
                 disabled=False,
                 tags=set([]),
                 optional=False,
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Mount.__init__(self,
                           rid,
                           mountPoint,
                           device,
                           fsType,
                           mntOpt,
                           snap_size,
                           always_on=always_on,
                           disabled=disabled,
                           tags=tags,
                           optional=optional,
                           monitor=monitor,
                           restart=restart,
                           subset=subset)
        self.fsck_h = {
            'vxfs': {'bin': 'fsck', 'cmd': ['fsck', '-F', 'vxfs', '-y', self.device]},
        }

    def is_up(self):
        return rcMounts.Mounts().has_mount(self.device, self.mountPoint)

    def start(self):
        Res.Mount.start(self)
        if self.is_up() is True:
            self.log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return 0
        self.fsck()
        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0o755)
        if self.fsType != "":
            fstype = ['-F', self.fsType]
        else:
            fstype = []
        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []
        cmd = ['mount']+fstype+mntopt+[self.device, self.mountPoint]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.can_rollback = True

    def stop(self):
        if self.is_up() is False:
            self.log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return 0
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mountPoint)
            raise ex.excError

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

