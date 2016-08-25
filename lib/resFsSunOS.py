import os
import time
import re

import rcStatus
import rcMountsSunOS as rcMounts
import resFs as Res
import rcExceptions as ex
from rcZfs import zfs_getprop, zfs_setprop
from rcUtilities import justcall

class Mount(Res.Mount):
    """ define SunOS mount/umount doAction """
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
        self.rdevice = device.replace('/dsk/','/rdsk/',1)
        self.Mounts = rcMounts.Mounts()
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
            'ufs': {'bin': 'fsck',
                    'cmd':       ['fsck', '-F', 'ufs', '-y', self.rdevice],
                    'reportcmd': ['fsck', '-F', 'ufs', '-m', self.rdevice],
                    'reportclean': [ 32 ],
            },
            'vxfs': {'bin': 'fsck',
                    'cmd':       ['fsck', '-F', 'vxfs', '-y', self.rdevice],
                    'reportcmd': ['fsck', '-F', 'vxfs', '-m', self.rdevice],
                    'reportclean': [ 32 ],
            },
        }

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def start(self):
        self.Mounts = None
        Res.Mount.start(self)
        m = re.match("<(\w+)>", self.mountPoint)
        if m:
            # the zone was not created when the service was built. now it should,
            # so try the redetect the zonepath
            zone = m.group(1)
            for r in self.svc.get_resources("container.zone"):
                if r.name == zone:
                    zonepath = r.get_zonepath()
                    self.mountPoint = re.sub("<\w+>", zonepath, self.mountPoint)

        if self.fsType == 'zfs' :
            if 'noaction' not in self.tags and zfs_getprop(self.device, 'canmount' ) != 'noauto' :
                self.log.info("%s should be set to canmount=noauto (zfs set canmount=noauto %s)"%(self.label, self.device))

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return

        if self.fsType == 'zfs' :
            if 'encap' not in self.tags and not self.svc.config.has_option(self.rid, 'zone') and zfs_getprop(self.device, 'zoned') != 'off':
                if zfs_setprop(self.device, 'zoned', 'off'):
                    raise ex.excError
            if zfs_getprop(self.device, 'mountpoint') != self.mountPoint:
                if not zfs_setprop(self.device, 'mountpoint', self.mountPoint):
                    raise ex.excError

            self.Mounts = None
            if self.is_up() is True:
                return

            (stdout,stderr,returncode)= justcall(['rm', self.mountPoint+"/.opensvc" ])
            ret, out, err = self.vcall(['zfs', 'mount', self.device ])
            if ret != 0:
                ret, out, err = self.vcall(['zfs', 'mount', '-O', self.device ])
                if ret != 0:
                    raise ex.excError
            return
        elif self.fsType != "":
            fstype = ['-F', self.fsType]
            self.fsck()
        else:
            fstype = []

        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []

        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0o755)

        for i in range(3):
            ret = self.try_mount(fstype, mntopt)
            if ret == 0: break
            time.sleep(1)

        self.Mounts = None

        if ret != 0:
            raise ex.excError

        self.can_rollback = True

    def can_check_writable(self):
        if self.fsType != 'zfs':
            return True
        pool = self.device.split("/")[0]
        cmd = ["zpool", "status", pool]
        out, err, ret = justcall(cmd)
        if "state: SUSPENDED" in out:
            self.status_log("pool %s is suspended")
            return False
        return True

    def try_mount(self, fstype, mntopt):
        cmd = ['mount'] + fstype + mntopt + [self.device, self.mountPoint]
        ret, out, err = self.vcall(cmd)
        return ret

    def try_umount(self):
        if self.fsType == 'zfs' :
            ret, out, err = self.vcall(['zfs', 'umount', self.device ], err_to_info=True)
            if ret != 0 :
                ret, out, err = self.vcall(['zfs', 'umount', '-f', self.device ], err_to_info=True)
                if ret != 0 :
                    raise ex.excError
            return
        (ret, out, err) = self.vcall(['umount', self.mountPoint], err_to_info=True)
        if ret == 0 :
            return
        for i in range(4):
            (ret, out, err) = self.vcall(['fuser', '-ck', self.mountPoint],
                                    err_to_info=True)
            (ret, out, err) = self.vcall(['umount', self.mountPoint],
                                    err_to_info=True)
            if ret == 0 :
                return
            if self.fsType != 'lofs' :
                (ret, out, err) = self.vcall(['umount', '-f', self.mountPoint],
                                        err_to_info=True)
                if ret == 0 :
                    return
        raise ex.excError

    def stop(self):
        self.Mounts = None
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return

        try:
            self.try_umount()
        except:
            self.Mounts = None
            self.log.error("failed")
            raise ex.excError
        self.Mounts = None

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

