import os
import rcMountsLinux as rcMounts
import resFs as Res
from rcUtilities import qcall, protected_mount, getmount, which, justcall
from rcUtilitiesLinux import major, get_blockdev_sd_slaves, lv_exists, devs_to_disks, label_to_dev
from rcGlobalEnv import rcEnv
from rcLoopLinux import file_to_loop
import rcExceptions as ex
from stat import *
from rcZfs import zfs_getprop, zfs_setprop

class Mount(Res.Mount):
    """ define Linux mount/umount doAction """
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
                           always_on=always_on,
                           disabled=disabled,
                           tags=tags,
                           optional=optional,
                           monitor=monitor,
                           restart=restart,
                           subset=subset)
        """
            0    - No errors
            1    - File system errors corrected
            32   - E2fsck canceled by user request
        """
        dev_realpath = os.path.realpath(self.device)
        if self.device.startswith("/dev/disk/by-") or dev_realpath.startswith("/dev/rbd"):
            self.device = dev_realpath


        self.fsck_h = {
            'ext2': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device], 'allowed_ret': [0, 1, 32, 33]},
            'ext3': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device], 'allowed_ret': [0, 1, 32, 33]},
            'ext4': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device], 'allowed_ret': [0, 1, 32, 33]},
        }
        self.loopdevice = None

    def try_umount(self, mnt=None):
        if self.fsType == "zfs":
            self.umount_zfs()
            return 0

        if mnt is None:
            mnt = self.mountPoint
        cmd = ['umount', mnt]
        (ret, out, err) = self.vcall(cmd, err_to_warn=True)
        if ret == 0:
            return 0
    
        if "not mounted" in err:
            return 0
    
        """ don't try to kill process using the source of a
            protected bind mount
        """
        if protected_mount(mnt):
            return 1
    
        """ best effort kill of all processes that might block
            the umount operation. The priority is given to mass
            action reliability, ie don't contest oprator's will
        """
        cmd = ['sync']
        (ret, out, err) = self.vcall(cmd)
    
        for i in range(4):
            cmd = ['fuser', '-kmv', mnt]
            (ret, out, err) = self.vcall(cmd, err_to_info=True)
            self.log.info('umount %s'%mnt)
            cmd = ['umount', mnt]
            ret = qcall(cmd)
            if ret == 0:
                break
    
        return ret

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        ret = self.Mounts.has_mount(self.device, self.mountPoint)
        if ret:
            return True

        # might be defined as a symlink. Linux display realpaths in /proc/mounts
        ret = self.Mounts.has_mount(self.device, os.path.realpath(self.mountPoint))
        if ret:
            return True

        # might be defined as a symlink. Linux display realpaths in /proc/mounts
        ret = self.Mounts.has_mount(os.path.realpath(self.device), os.path.realpath(self.mountPoint))
        if ret:
            return True

        # might be a mount by label or uuid
        for dev in self.devlist():
            ret = self.Mounts.has_mount(dev, self.mountPoint)
            if ret:
                return True
            ret = self.Mounts.has_mount(dev, os.path.realpath(self.mountPoint))
            if ret:
                return True


        # might be mount using a /dev/mapper/ name too
        l = self.device.split('/')
        if len(l) == 4 and l[2] != "mapper":
            dev = "/dev/mapper/%s-%s"%(l[2].replace('-','--'),l[3].replace('-','--'))
            ret = self.Mounts.has_mount(dev, self.mountPoint)
            if ret:
                return True
            ret = self.Mounts.has_mount(dev, os.path.realpath(self.mountPoint))
            if ret:
                return True

        if os.path.exists(self.device):
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
                    ret = self.Mounts.has_mount(dev, os.path.realpath(self.mountPoint))
                    if ret:
                        return True
            elif S_ISBLK(mode):
                # might be a mount using a /dev/dm-<minor> name too
                from rcUtilitiesLinux import major
                dm_major = major('device-mapper')
                if os.major(st.st_rdev) == dm_major:
                    dev = '/dev/dm-' + str(os.minor(st.st_rdev))
                    ret = self.Mounts.has_mount(dev, self.mountPoint)
                    if ret:
                        return True
                    ret = self.Mounts.has_mount(dev, os.path.realpath(self.mountPoint))
                    if ret:
                        return True

        return False

    def realdev(self):
        if self.device.startswith("LABEL=") or self.device.startswith("UUID="):
            _dev = label_to_dev(self.device)
            if _dev:
                return _dev
            return self.device
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return

        if os.path.exists(self.device) and S_ISBLK(mode):
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

    def mplist(self):
        dev = self.realdev()
        if dev is None:
            return set([])

        try:
            self.dm_major = major('device-mapper')
        except:
            return set([])

        return self._mplist([dev])

    def devname_to_dev(self, x):
        if 'cciss!' in x:
            return '/dev/cciss/'+x.replace('cciss!', '')
        return '/dev/'+x

    def _mplist(self, devs):
        mps = set([])
        for dev in devs:
            devmap = False
            if 'dm-' in dev:
                minor = int(dev.replace('/dev/dm-', ''))
                dm = dev.replace('/dev/', '')
                devmap = True
            else:
                try:
                    statinfo = os.stat(dev)
                except:
                    self.log.warning("can not stat %s" % dev)
                    continue
                minor = os.minor(statinfo.st_rdev)
                dm = 'dm-%i'%minor
                devmap = self.is_devmap(statinfo)

            if self.is_multipath(minor):
                mps |= set([dev])
            elif devmap:
                syspath = '/sys/block/' + dm + '/slaves'
                if not os.path.exists(syspath):
                    continue
                slaves = os.listdir(syspath)
                mps |= self._mplist(map(self.devname_to_dev, slaves))
        return mps

    def is_multipath(self, minor):
        cmd = ['dmsetup', '-j', str(self.dm_major),
                          '-m', str(minor),
                          'table'
              ]
        (ret, buff, err) = self.call(cmd, errlog=False, cache=True)
        if ret != 0:
            return False
        l = buff.split()
        if len(l) < 3:
            return False
        if l[2] != 'multipath':
            return False
        if 'queue_if_no_path' not in l:
            return False
        cmd = ['dmsetup', '-j', str(self.dm_major),
                          '-m', str(minor),
                          'status'
              ]
        (ret, buff, err) = self.call(cmd, errlog=False, cache=True)
        if ret != 0:
            return False
        l = buff.split()
        if l.count('A') > 1:
            return False
        return True

    def is_devmap(self, statinfo):
        if os.major(statinfo.st_rdev) == self.dm_major:
            return True
        return False

    def _disklist(self):
        dev = self.realdev()
        if dev is None:
            return set([])

        if dev.startswith("/dev/rbd"):
            return set([])

        try:
            self.dm_major = major('device-mapper')
        except:
            return set([dev])

        try:
            statinfo = os.stat(dev)
        except:
            self.log.error("can not stat %s" % dev)
            raise ex.excError

        if not self.is_devmap(statinfo):
            return set([dev])

        if lv_exists(self, dev):
            """ if the fs is built on a lv of a private vg, its
                disks will be given by the vg resource.
                if the fs is built on a lv of a shared vg, we
                don't want to account its disks : don't reserve
                them, don't account their size multiple times.
            """
            return set([])

        dm = 'dm-' + str(os.minor(statinfo.st_rdev))
        syspath = '/sys/block/' + dm + '/slaves'
        devs = get_blockdev_sd_slaves(syspath)
        return devs

    def disklist(self):
        return devs_to_disks(self, self._disklist())

    def devlist(self):
        dev = self.realdev()
        if dev is None:
            return set([])
        return set([dev])

    def can_check_writable(self):
        if self.fsType == "zfs":
            return self.can_check_zfs_writable()
        if len(self.mplist()) > 0:
            self.log.debug("a multipath under fs has queueing enabled and no active path")
            return False
        return True

    def start(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        Res.Mount.start(self)

        """ loopback mount
            if the file has already been binded to a loop re-use
            the loopdev to avoid allocating another one
        """
        if os.path.exists(self.device):
            try:
                mode = os.stat(self.device)[ST_MODE]
                if S_ISREG(mode):
                    devs = file_to_loop(self.device)
                    if len(devs) > 0:
                        self.loopdevice = devs[0]
                        mntopt_l = self.mntOpt.split(',')
                        mntopt_l.remove("loop")
                        self.mntOpt = ','.join(mntopt_l)
            except:
                self.log.debug("can not stat %s" % self.device)
                return False
 
        if self.fsType == "zfs":
            self.check_zfs_canmount()

        if self.is_up() is True:
            self.log.info("%s is already mounted" % self.label)
            return 0

        if self.fsType == "btrfs":
            cmd = ['btrfs', 'device', 'scan']
            ret, out, err = self.vcall(cmd)

        self.fsck()
        if not os.path.exists(self.mountPoint):
            try:
                os.makedirs(self.mountPoint, 0o755)
            except Exception as e:
                raise ex.excError(str(e))

        if self.fsType == "zfs":
            self.mount_zfs()
        else:
            self.mount_generic()

        self.Mounts = None
        self.can_rollback = True

    def can_check_zfs_writable(self):
        pool = self.device.split("/")[0]
        cmd = ["zpool", "status", pool]
        out, err, ret = justcall(cmd)
        if "state: SUSPENDED" in out:
            self.status_log("pool %s is suspended")
            return False
        return True

    def check_zfs_canmount(self):
        if 'noaction' not in self.tags and zfs_getprop(self.device, 'canmount' ) != 'noauto' :
            self.log.info("%s should be set to canmount=noauto (zfs set canmount=noauto %s)" % (self.label, self.device))

    def umount_zfs(self):
        ret, out, err = self.vcall(['zfs', 'umount', self.device ], err_to_info=True)
        if ret != 0 :
            ret, out, err = self.vcall(['zfs', 'umount', '-f', self.device ], err_to_info=True)
            if ret != 0 :
                raise ex.excError

    def mount_zfs(self):
        if 'encap' not in self.tags and not self.svc.config.has_option(self.rid, 'zone') and zfs_getprop(self.device, 'zoned') != 'off':
            if zfs_setprop(self.device, 'zoned', 'off'):
                raise ex.excError
        if zfs_getprop(self.device, 'mountpoint') != self.mountPoint:
            if not zfs_setprop(self.device, 'mountpoint', self.mountPoint):
                raise ex.excError

        try:
            os.unlink(self.mountPoint+"/.opensvc")
        except:
            pass
        ret, out, err = self.vcall(['zfs', 'mount', self.device ])
        if ret != 0:
            ret, out, err = self.vcall(['zfs', 'mount', '-O', self.device ])
            if ret != 0:
                raise ex.excError

    def mount_generic(self):
        if self.fsType != "":
            fstype = ['-t', self.fsType]
        else:
            fstype = []

        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []

        if self.loopdevice is None:
            device = self.device
        else:
            device = self.loopdevice

        cmd = ['mount']+fstype+mntopt+[device, self.mountPoint]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def kill_users(self):
        import glob
        for p in glob.glob("/proc/*/fd/*") + glob.glob("/proc/*/cwd") + glob.glob("/proc/*/exe"):
            try:
                dest = os.path.realpath(p)
            except:
                continue
            if dest.startswith(self.mountPoint):
                l = p.split("/")
                try:
                    pid = int(l[2])
                except:
                    continue
                try:
                    with open("/proc/%d/cmdline"%pid, "r") as f:
                        cmdline = f.read()
                except Exception as e:
                    self.log.warning(str(e))
                    cmdline = ""
                self.log.info("kill -9 %d (cmdline: %s)" % (pid, cmdline))
                os.kill(pid, 9)

    def stop(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        if self.is_up() is False:
            self.log.info("%s is already umounted" % self.label)
            return
        try:
            os.stat(self.mountPoint)
            if not os.path.exists(self.mountPoint):
                raise ex.excError('mount point %s does not exist' % self.mountPoint)
        except OSError as e:
            if e.errno == 5:
                self.log.warning("I/O error on mount point. try to umount anyway")
                self.kill_users()
            else:
                raise
        self.remove_holders()
        self.remove_deeper_mounts()
        for i in range(3):
            ret = self.try_umount()
            if ret == 0: break
        if ret != 0:
            raise ex.excError('failed to umount %s'%self.mountPoint)
        self.Mounts = None

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        if dev is None:
            return
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.devlist(filtered=False) & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.excError("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        import glob
        import rcDevTreeLinux
        tree = rcDevTreeLinux.DevTree()
        tree.load()
        dev_realpath = os.path.realpath(self.device)
        self.remove_dev_holders(dev_realpath, tree)

    def remove_deeper_mounts(self):
        import rcMountsLinux
        mounts = rcMountsLinux.Mounts()
        mnt_realpath = os.path.realpath(self.mountPoint)
        for m in mounts:
            _mnt_realpath = os.path.realpath(m.mnt)
            if _mnt_realpath != mnt_realpath and \
               _mnt_realpath.startswith(mnt_realpath+"/"):
                ret = self.try_umount(_mnt_realpath)
                if ret != 0:
                    break

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

