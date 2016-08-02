import os

import rcMountsDarwin as rcMounts
import resFs as Res
from rcUtilities import qcall, protected_mount, getmount
from rcGlobalEnv import rcEnv
from rcLoopDarwin import file_to_loop
import rcExceptions as ex
from stat import *

def try_umount(self):
    cmd = ['diskutil', 'umount', self.mountPoint]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

    cmd = ['diskutil', 'umount', 'force', self.mountPoint]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
    if ret == 0:
        return 0

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
        nb_killed = self.killfuser(self.mountPoint)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0 or nb_killed == 0:
            break

    if ret != 0:
        self.log.info("no more process using %s, yet umount fails. try forced umount."%self.mountPoint)
        cmd = ['umount', '-f', self.mountPoint]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)

    return ret


class Mount(Res.Mount):
    """ define FreeBSD mount/umount doAction """
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
        self.Mounts = None
        self.loopdevice = None
        self.isloop = False
        Res.Mount.__init__(self,
                           rid,
                           mountPoint,
                           device,
                           fsType,
                           mntOpt,
                           snap_size,
                           always_on,
                           disabled=disabled,
                           tags=tags,
                           optional=optional,
                           monitor=monitor,
                           restart=restart,
                           subset=subset)
        self.fsck_h = {
            'hfs': {'bin': 'fsck', 'cmd': ['diskutil', 'repairVolume', self.device]},
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
        self.Mounts = rcMounts.Mounts()
        ret = self.Mounts.has_mount(self.device, self.mountPoint)
        if ret:
            return True

        if self.fsType not in self.netfs:
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
                    ret = self.Mounts.has_mount(dev, self.mountPoint)
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
                self.Mounts = rcMounts.Mounts()
            m = self.Mounts.has_param("mnt", mnt)
            if m is None:
                self.log.debug("can't find dev %(dev)s mounted in %(mnt)s in mnttab"%dict(mnt=mnt, dev=self.device))
                return None
            dev = m.dev

        return dev

    def disklist(self):
        dev = self.realdev()
        if dev is None:
            return set([])

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
            self.Mounts = rcMounts.Mounts()
        Res.Mount.start(self)

        if self.fsType in self.netfs or self.device == "none":
            # TODO showmount -e
            pass
        else:
            try:
                mode = os.stat(self.device)[ST_MODE]
                if S_ISREG(mode):
                    devs = file_to_loop(self.device)
                    if len(devs) > 0:
                        self.loopdevice = devs[0]
                    self.isloop = True
            except:
                self.log.debug("can not stat %s" % self.device)
                return False

        if self.is_up() is True:
            self.log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return 0

        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0o755)

        if self.isloop is True:
            cmd = ['hdiutil', 'attach', '-mountpoint', self.mountPoint , self.device]
            (ret, out, err) = self.vcall(cmd)
        else:
            self.fsck()
            try:
                cmd = ['diskutil', 'mount', '-mountPoint', self.mountPoint , self.device]
                (ret, out, err) = self.vcall(cmd)
            except:
                if self.fsType != "":
                    fstype = ['-t', self.fsType]
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
        self.Mounts = None
        self.can_rollback = True

    def stop(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        if self.is_up() is False:
            self.log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mountPoint)
            raise ex.excError
        self.Mounts = None

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

