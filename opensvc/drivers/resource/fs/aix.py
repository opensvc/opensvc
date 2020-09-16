import os
from stat import *

import core.exceptions as ex
from utilities.files import protected_mount, getmount
from utilities.mounts.aix import Mounts
from utilities.proc import qcall
from . import BaseFs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = ""


def try_umount(self):
    cmd = ['umount', self.mount_point]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

    # don't try to kill process using the source of a
    # protected bind mount
    if protected_mount(self.mount_point):
        return 1

    # best effort kill of all processes that might block
    # the umount operation. The priority is given to mass
    # action reliability, ie don't contest oprator's will
    cmd = ['sync']
    (ret, out, err) = self.vcall(cmd, err_to_info=True)

    for i in range(4):
        cmd = ['fuser', '-k', '-x', '-c', self.mount_point]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mount_point)
        cmd = ['umount', self.mount_point]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Fs(BaseFs):
    """
    AIX fs resource driver.
    """
    def __init__(self, **kwargs):
        self.mounts = None
        super(Fs, self).__init__(**kwargs)

    def set_fsck_h(self):
        self.fsck_h = {
            'jfs': {
                'bin': 'fsck',
                'cmd': ['fsck', '-p', '-V', 'jfs', self.device]
            },
            'jfs2': {
                'bin': 'fsck',
                'cmd': ['fsck', '-p', '-V', 'jfs2', self.device]
            },
        }

    def is_up(self):
        self.mounts = Mounts()
        return self.mounts.has_mount(self.device, self.mount_point)

    def realdev(self):
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return None
        if S_ISBLK(mode):
            dev = self.device
        else:
            mnt = getmount(self.device)
            if self.mounts is None:
                self.mounts = Mounts()
            m = self.mounts.has_param("mnt", mnt)
            if m is None:
                self.log.debug("can't find dev %(dev)s mounted in %(mnt)s in mnttab"%dict(mnt=mnt, dev=self.device))
                return None
            dev = m.dev

        return dev

    def mplist(self):
        dev = self.realdev()
        if dev is None:
            return set()

        return self._mplist([dev])

    def _mplist(self, devs):
        mps = set()
        return mps

    def sub_devs(self):
        dev = self.realdev()
        if dev is None:
            return set()
        return set()

    def can_check_writable(self):
        return True

    def start_mount(self):
        if self.mounts is None:
            self.mounts = Mounts()
        self.prepare_mount()
        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0
        self.fsck()
        if not os.path.exists(self.mount_point):
            os.makedirs(self.mount_point, 0o755)
        if self.fs_type != "":
            fstype = ['-v', self.fs_type]
        else:
            fstype = []
        if self.mount_options != "":
            mntopt = ['-o', self.mount_options]
        else:
            mntopt = []
        cmd = ['mount']+fstype+mntopt+[self.device, self.mount_point]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.mounts = None
        self.can_rollback = True

    def stop(self):
        if self.mounts is None:
            self.mounts = Mounts()
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mount_point)
            raise ex.Error
        self.mounts = None
