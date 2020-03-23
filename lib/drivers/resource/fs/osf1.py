import os

from stat import *

import rcExceptions as ex

from . import BaseFs, adder as base_adder
from rcGlobalEnv import rcEnv
from rcMountsOSF1 import Mounts
from rcUtilities import qcall, protected_mount, getmount

def adder(svc, s):
    base_adder(svc, s, drv=Fs)

def try_umount(self):
    cmd = ['umount', self.mount_point]
    (ret, out, err) = self.vcall(cmd, err_to_warn=True)
    if ret == 0:
        return 0

    if "not currently mounted" in err:
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
    (ret, out, err) = self.vcall(cmd)

    for i in range(4):
        cmd = ['fuser', '-kcv', self.mount_point]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mount_point)
        cmd = ['umount', self.mount_point]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Fs(BaseFs):
    def __init__(self, **kwargs):
        self.Mounts = None
        super().__init__(**kwargs)

    def set_fsck_h(self):
        self.fsck_h = {
            'ufs': {
                'bin': 'fsck',
                'cmd': ['fsck', '-p', self.device], 'allowed_ret': []
            },
        }

    def is_up(self):
        self.Mounts = Mounts()
        ret = self.Mounts.has_mount(self.device, self.mount_point)
        if ret:
            return True

        if self.fs_type not in ["advfs"] + self.netfs:
            # might be a loopback mount
            try:
                mode = os.stat(self.device)[ST_MODE]
            except:
                self.log.debug("can not stat %s" % self.device)
                return False

        return False

    def sub_devs(self):
        if '#' in self.device:
            dom, fset = self.device.split('#')
            for r in self.svc.get_resources('disk.vg'):
                if r.name == dom:
                    # no need to compute device list: the vg resource will do the job
                    return set()
            import rcAdvfs
            try:
                o = rcAdvfs.Fdmns()
                d = o.get_fdmn(dom)
            except rcAdvfs.ExInit as e:
                return set()
            if d is None:
                return set()
            return set(d.list_volnames())
        else:
            return set([self.device])

    def can_check_writable(self):
        return True

    def start(self):
        if self.Mounts is None:
            self.Mounts = Mounts()
        super().start()

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0

        self.fsck()
        if not os.path.exists(self.mount_point):
            os.makedirs(self.mount_point, 0o755)
        if self.fs_type != "":
            fstype = ['-t', self.fs_type]
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
        self.Mounts = None
        self.can_rollback = True

    def stop(self):
        if self.Mounts is None:
            self.Mounts = Mounts()
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mount_point)
            raise ex.excError
        self.Mounts = None
