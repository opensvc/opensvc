import os
import rcMountsOSF1 as rcMounts
import resFs as Res
from rcUtilities import qcall, protected_mount, getmount
from rcGlobalEnv import rcEnv
import rcExceptions as ex
from stat import *

def try_umount(self):
    cmd = ['umount', self.mountPoint]
    (ret, out, err) = self.vcall(cmd, err_to_warn=True)
    if ret == 0:
        return 0

    if "not currently mounted" in err:
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
    (ret, out, err) = self.vcall(cmd)

    for i in range(4):
        cmd = ['fuser', '-kcv', self.mountPoint]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Mount(Res.Mount):
    def __init__(self,
                 rid,
                 mountPoint,
                 device,
                 fsType,
                 mntOpt,
                 always_on=set([]),
                 snap_size=None,
                 disabled=False,
                 tags=set([]),
                 optional=False,
                 monitor=False,
                 restart=0,
                 subset=None):
        self.Mounts = None
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
            'ufs': {'bin': 'fsck', 'cmd': ['fsck', '-p', self.device], 'allowed_ret': []},
        }

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        ret = self.Mounts.has_mount(self.device, self.mountPoint)
        if ret:
            return True

        if self.fsType not in ["advfs"] + self.netfs:
            # might be a loopback mount
            try:
                mode = os.stat(self.device)[ST_MODE]
            except:
                self.log.debug("can not stat %s" % self.device)
                return False

        return False

    def devlist(self):
        return self.disklist()

    def disklist(self):
        if '#' in self.device:
            dom, fset = self.device.split('#')
            for r in self.svc.get_resources('disk.vg'):
                if r.name == dom:
                    # no need to compute device list: the vg resource will do the job
                    return set([])
            import rcAdvfs
            try:
                o = rcAdvfs.Fdmns()
                d = o.get_fdmn(dom)
            except rcAdvfs.ExInit as e:
                return set([])
            if d is None:
                return set([])
            return set(d.list_volnames())
        else:
            return set([self.device])

    def can_check_writable(self):
        return True

    def start(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        Res.Mount.start(self)

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0

        self.fsck()
        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0o755)
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
            self.log.info("%s is already umounted" % self.label)
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

