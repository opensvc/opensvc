import os
from stat import *

import rcExceptions as ex

from . import BaseFs, adder as base_adder
from rcGlobalEnv import rcEnv
from rcLoopDarwin import file_to_loop
from rcMountsDarwin import Mounts
from rcUtilities import qcall, protected_mount, getmount

def adder(svc, s):
    base_adder(svc, s, drv=Fs)

def try_umount(self):
    cmd = ['diskutil', 'umount', self.mount_point]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

    cmd = ['diskutil', 'umount', 'force', self.mount_point]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

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
        nb_killed = self.killfuser(self.mount_point)
        self.log.info('umount %s'%self.mount_point)
        cmd = ['umount', self.mount_point]
        ret = qcall(cmd)
        if ret == 0 or nb_killed == 0:
            break

    if ret != 0:
        self.log.info("no more process using %s, yet umount fails. try forced umount."%self.mount_point)
        cmd = ['umount', '-f', self.mount_point]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)

    return ret


class Fs(BaseFs):
    """ define FreeBSD mount/umount doAction """
    def __init__(self,
                 rid,
                 mount_point,
                 device,
                 fs_type,
                 mount_options,
                 snap_size=None,
                 **kwargs):
        self.Mounts = None
        self.loopdevice = None
        self.isloop = False
        super().__init__(rid,
                         mount_point=mount_point,
                         device=device,
                         fs_type=fs_type,
                         mount_options=mount_options,
                         snap_size=snap_size,
                         **kwargs)

    def set_fsck_h(self):
        self.fsck_h = {
            'hfs': {
                'bin': 'fsck',
                'cmd': ['diskutil', 'repairVolume', self.device]
            },
        }

    def killfuser(self, dir):
        cmd = ['fuser', '-kmc', dir]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)

        """ return the number of process we sent signal to
        """
        l = out.split(':')
        if len(l) < 2:
            return 0
        return len(l[1].split())

    def is_up(self):
        self.Mounts = Mounts()
        ret = self.Mounts.has_mount(self.device, self.mount_point)
        if ret:
            return True

        if self.fs_type not in self.netfs:
            try:
                st = os.stat(self.device)
                mode = st[ST_MODE]
            except:
                self.log.debug("can not stat %s" % self.device)
                return False

            if S_ISREG(mode):
                # might be a loopback mount
                devs = file_to_loop(self.device)
                for dev in devs:
                    ret = self.Mounts.has_mount(dev, self.mount_point)
                    if ret:
                        return True

        return False

    def realdev(self):
        dev = None
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return None
        if S_ISCHR(mode):
            dev = self.device
        else:
            mnt = getmount(self.device)
            if self.Mounts is None:
                self.Mounts = Mounts()
            m = self.Mounts.has_param("mnt", mnt)
            if m is None:
                self.log.debug("can't find dev %(dev)s mounted in %(mnt)s in mnttab"%dict(mnt=mnt, dev=self.device))
                return None
            dev = m.dev

        return dev

    def sub_devs(self):
        dev = self.realdev()
        if dev is None:
            return set()

        try:
            statinfo = os.stat(dev)
        except:
            self.log.error("can not stat %s" % dev)
            raise ex.excError

        return set([dev])

    def can_check_writable(self):
        return True

    def start(self):
        if self.Mounts is None:
            self.Mounts = Mounts()
        super().start()

        if self.fs_type in self.netfs or self.device == "none":
            # TODO showmount -e
            pass
        else:
            try:
                mode = os.stat(self.device)[ST_MODE]
                if S_ISREG(mode):
                    devs = file_to_loop(self.device)
                    if len(devs) > 0:
                        self.loopdevice = devs[0]
                    self.loopdev = devs[0]
                    self.isloop = True
            except:
                self.log.debug("can not stat %s" % self.device)
                return False

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0

        if not os.path.exists(self.mount_point):
            os.makedirs(self.mount_point, 0o755)

        if self.isloop is True:
            #cmd = ['hdiutil', 'attach', '-mountpoint', self.mount_point , self.device]
            #(ret, out, err) = self.vcall(cmd)
            device = self.loopdev
        else:
            device = self.device
            self.fsck()

        try:
            cmd = ['diskutil', 'mount', '-mountPoint', self.mount_point , device]
            (ret, out, err) = self.vcall(cmd)
        except:
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
