"""
Linux Fs resource driver module
"""
import os
from stat import ST_MODE, S_ISREG, S_ISBLK

from rcGlobalEnv import rcEnv
import rcMountsLinux as rcMounts
import resFs as Res
from rcUtilities import qcall, protected_mount, getmount, justcall
from rcUtilitiesLinux import major, get_blockdev_sd_slaves, lv_exists, devs_to_disks, label_to_dev
from rcLoopLinux import file_to_loop
import rcExceptions as ex
from rcZfs import zfs_getprop, zfs_setprop

class Mount(Res.Mount):
    """
    Linux Fs resource driver
    """
    def __init__(self, **kwargs):
        self.mounts = None
        Res.Mount.__init__(self, **kwargs)

        dev_realpath = os.path.realpath(self.device)
        if self.device.startswith("/dev/disk/by-") or dev_realpath.startswith("/dev/rbd"):
            self.device = dev_realpath

        # 0    - No errors
        # 1    - File system errors corrected
        # 32   - E2fsck canceled by user request
        self.fsck_h = {
            'ext2': {
                'bin': 'e2fsck',
                'cmd': ['e2fsck', '-p', self.device],
                'allowed_ret': [0, 1, 32, 33]
            },
            'ext3': {
                'bin': 'e2fsck',
                'cmd': ['e2fsck', '-p', self.device],
                'allowed_ret': [0, 1, 32, 33]
            },
            'ext4': {
                'bin': 'e2fsck',
                'cmd': ['e2fsck', '-p', self.device],
                'allowed_ret': [0, 1, 32, 33]
            },
        }
        self.loopdevice = None
        self.dm_major = None

    def umount_generic(self, mnt=None):
        if mnt is None:
            mnt = self.mount_point
        cmd = ['umount', mnt]
        return self.vcall(cmd, err_to_warn=True)

    def try_umount(self, mnt=None):
        if mnt is None:
            mnt = self.mount_point

        if self.fs_type == "zfs":
            ret, out, err = self.umount_zfs()
        else:
            ret, out, err = self.umount_generic(mnt)

        if ret == 0:
            return 0

        if "not mounted" in err:
            return 0

        # don't try to kill process using the source of a
        # protected bind mount
        if protected_mount(mnt):
            return 1

        # best effort kill of all processes that might block
        # the umount operation. The priority is given to mass
        # action reliability, ie don't contest oprator's will
        cmd = ['sync']
        ret, out, err = self.vcall(cmd)

        if os.path.isdir(self.device):
            fuser_opts = '-kv'
        else:
            fuser_opts = '-kmv'
        for _ in range(4):
            cmd = ['fuser', fuser_opts, mnt]
            (ret, out, err) = self.vcall(cmd, err_to_info=True)
            self.log.info('umount %s', mnt)
            cmd = ['umount', mnt]
            ret = qcall(cmd)
            if ret == 0:
                break

        return ret

    def loop_dev_to_file(self, dev):
        cmd = [rcEnv.syspaths.losetup, dev, "-O", "BACK-FILE", "-n"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        return out.replace(" (deleted)", "").strip()

    def is_up(self):
        self.mounts = rcMounts.Mounts()
        ret = self.mounts.has_mount(self.device, self.mount_point)
        if ret:
            return True

        # might be defined as a symlink. Linux display realpaths in /proc/mounts
        ret = self.mounts.has_mount(self.device,
                                    os.path.realpath(self.mount_point))
        if ret:
            return True

        # might be defined as a symlink. Linux display realpaths in /proc/mounts
        ret = self.mounts.has_mount(os.path.realpath(self.device),
                                    os.path.realpath(self.mount_point))
        if ret:
            return True

        # might be a loop device seen in mounts as its backing file
        if self.device.startswith("/dev/loop"):
            backfile = self.loop_dev_to_file(self.device)
            if backfile and self.mounts.has_mount(backfile, os.path.realpath(self.mount_point)):
                return True

        # might be a mount by label or uuid
        for dev in self.devlist():
            ret = self.mounts.has_mount(dev, self.mount_point)
            if ret:
                return True
            ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
            if ret:
                return True
            if dev.startswith("/dev/loop"):
                backfile = self.loop_dev_to_file(dev)
                if backfile and self.mounts.has_mount(backfile, os.path.realpath(self.mount_point)):
                    return True

        # might be mount using a /dev/mapper/ name too
        elements = self.device.split('/')
        if len(elements) == 4 and elements[2] != "mapper":
            dev = "/dev/mapper/%s-%s" % (elements[2].replace('-', '--'), elements[3].replace('-', '--'))
            ret = self.mounts.has_mount(dev, self.mount_point)
            if ret:
                return True
            ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
            if ret:
                return True

        if os.path.exists(self.device):
            try:
                fstat = os.stat(self.device)
                mode = fstat[ST_MODE]
            except:
                self.log.debug("can not stat %s", self.device)
                return False

            if S_ISREG(mode):
                # might be a loopback mount
                devs = file_to_loop(self.device)
                for dev in devs:
                    ret = self.mounts.has_mount(dev, self.mount_point)
                    if ret:
                        return True
                    ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
                    if ret:
                        return True
            elif S_ISBLK(mode):
                # might be a mount using a /dev/dm-<minor> name too
                dm_major = major('device-mapper')
                if os.major(fstat.st_rdev) == dm_major:
                    dev = '/dev/dm-' + str(os.minor(fstat.st_rdev))
                    ret = self.mounts.has_mount(dev, self.mount_point)
                    if ret:
                        return True
                    ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
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
            self.log.debug("can not stat %s", self.device)
            return

        if os.path.exists(self.device) and S_ISBLK(mode):
            dev = self.device
        else:
            mnt = getmount(self.device)
            if self.mounts is None:
                self.mounts = rcMounts.Mounts()
            mount = self.mounts.has_param("mnt", mnt)
            if mount is None:
                self.log.debug("can't find dev %s mounted in %s in mnttab",
                               mnt, self.device)
                return None
            dev = mount.dev

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

    @staticmethod
    def devname_to_dev(devname):
        if 'cciss!' in devname:
            return '/dev/cciss/'+devname.replace('cciss!', '')
        return '/dev/'+devname

    def _mplist(self, devs):
        mps = set([])
        for dev in devs:
            devmap = False
            if 'dm-' in dev:
                minor = int(dev.replace('/dev/dm-', ''))
                devname = dev.replace('/dev/', '')
                devmap = True
            else:
                try:
                    statinfo = os.stat(dev)
                except:
                    self.log.warning("can not stat %s", dev)
                    continue
                minor = os.minor(statinfo.st_rdev)
                devname = 'dm-%i'%minor
                devmap = self.is_devmap(statinfo)

            if self.is_multipath(minor):
                mps |= set([dev])
            elif devmap:
                syspath = '/sys/block/' + devname + '/slaves'
                if not os.path.exists(syspath):
                    continue
                slaves = os.listdir(syspath)
                mps |= self._mplist([self.devname_to_dev(slave) for slave in slaves])
        return mps

    def is_multipath(self, minor):
        cmd = [
            rcEnv.syspaths.dmsetup, '-j', str(self.dm_major),
            '-m', str(minor),
            'table'
        ]
        ret, buff, err = self.call(cmd, errlog=False, cache=True)
        if ret != 0:
            return False
        elements = buff.split()
        if len(elements) < 3:
            return False
        if elements[2] != 'multipath':
            return False
        if 'queue_if_no_path' not in elements:
            return False
        cmd = [
            rcEnv.syspaths.dmsetup, '-j', str(self.dm_major),
            '-m', str(minor),
            'status'
        ]
        ret, buff, err = self.call(cmd, errlog=False, cache=True)
        if ret != 0:
            return False
        elements = buff.split()
        if elements.count('A') > 1:
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
            self.log.error("can not stat %s", dev)
            raise ex.excError

        if not self.is_devmap(statinfo):
            return set([dev])

        if lv_exists(self, dev):
            # If the fs is built on a lv of a private vg, its
            # disks will be given by the vg resource.
            # if the fs is built on a lv of a shared vg, we
            # don't want to account its disks : don't reserve
            # them, don't account their size multiple times.
            return set([])

        devname = 'dm-' + str(os.minor(statinfo.st_rdev))
        syspath = '/sys/block/' + devname + '/slaves'
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
        if self.fs_type == "zfs":
            return self._can_check_zfs_writable()
        if len(self.mplist()) > 0:
            self.log.debug("a multipath under fs has queueing enabled and no active path")
            return False
        return True

    def start(self):
        if self.mounts is None:
            self.mounts = rcMounts.Mounts()
        Res.Mount.start(self)

        # loopback mount
        # if the file has already been binded to a loop re-use
        # the loopdev to avoid allocating another one
        if os.path.exists(self.device):
            try:
                mode = os.stat(self.device)[ST_MODE]
                if S_ISREG(mode):
                    devs = file_to_loop(self.device)
                    if len(devs) > 0:
                        self.loopdevice = devs[0]
                        mntopt_l = self.mount_options.split(',')
                        if "loop" in mntopt_l:
                            mntopt_l.remove("loop")
                            self.mount_options = ','.join(mntopt_l)
            except Exception as exc:
                raise ex.excError(str(exc))

        if self.fs_type == "zfs":
            self._check_zfs_canmount()

        if self.is_up() is True:
            self.log.info("%s is already mounted", self.label)
            return 0

        if self.fs_type == "btrfs":
            cmd = ['btrfs', 'device', 'scan']
            self.vcall(cmd)

        self.fsck()
        if not os.path.exists(self.mount_point):
            try:
                os.makedirs(self.mount_point, 0o755)
            except Exception as exc:
                raise ex.excError(str(exc))

        if self.fs_type == "zfs":
            self.mount_zfs()
        else:
            self.mount_generic()

        self.mounts = None
        self.can_rollback = True

    def _can_check_zfs_writable(self):
        pool = self.device.split("/")[0]
        cmd = [rcEnv.syspaths.zpool, "status", pool]
        out, err, ret = justcall(cmd)
        if "state: SUSPENDED" in out:
            self.status_log("pool %s is suspended" % pool)
            return False
        return True

    def _check_zfs_canmount(self):
        if 'noaction' not in self.tags and \
           zfs_getprop(self.device, 'canmount') != 'noauto':
            self.log.info("%s should be set to canmount=noauto (zfs set "
                          "canmount=noauto %s)", self.label, self.device)

    def umount_zfs(self):
        ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'umount', self.device], err_to_info=True)
        if ret != 0:
            ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'umount', '-f', self.device], err_to_info=True)
        return ret, out, err

    def mount_zfs(self):
        if 'encap' not in self.tags and \
           not self.svc.config.has_option(self.rid, 'zone') and \
           zfs_getprop(self.device, 'zoned') != 'off':
            if zfs_setprop(self.device, 'zoned', 'off'):
                raise ex.excError
        if zfs_getprop(self.device, 'mountpoint') != self.mount_point:
            if not zfs_setprop(self.device, 'mountpoint', self.mount_point):
                raise ex.excError

        try:
            os.unlink(self.mount_point+"/.opensvc")
        except:
            pass
        ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'mount', self.device])
        if ret != 0:
            ret, out, err = self.vcall([rcEnv.syspaths.zfs, 'mount', '-O', self.device])
            if ret != 0:
                raise ex.excError

    def mount_generic(self):
        if self.fs_type != "":
            fstype = ['-t', self.fs_type]
        else:
            fstype = []

        if self.mount_options != "":
            mntopt = ['-o', self.mount_options]
        else:
            mntopt = []

        if self.loopdevice is None:
            device = self.device
        else:
            device = self.loopdevice

        cmd = ['mount'] + fstype + mntopt + [device, self.mount_point]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def kill_users(self):
        import glob
        for path in glob.glob("/proc/*/fd/*") + glob.glob("/proc/*/cwd") + glob.glob("/proc/*/exe"):
            try:
                dest = os.path.realpath(path)
            except:
                continue
            if dest.startswith(self.mount_point):
                elements = path.split("/")
                try:
                    pid = int(elements[2])
                except:
                    continue
                try:
                    with open("/proc/%d/cmdline" % pid, "r") as ofile:
                        cmdline = ofile.read()
                except Exception as exc:
                    self.log.warning(str(exc))
                    cmdline = ""
                self.log.info("kill -9 %d (cmdline: %s)", pid, cmdline)
                os.kill(pid, 9)

    def stop(self):
        if self.mounts is None:
            self.mounts = rcMounts.Mounts()
        if self.is_up() is False:
            self.log.info("%s is already umounted", self.label)
            return
        if not os.path.exists(self.mount_point):
            raise ex.excError('mount point %s does not exist' % self.mount_point)
        try:
            os.stat(self.mount_point)
        except OSError as exc:
            if exc.errno == (5, 13):
                self.log.warning("I/O error on mount point. try to umount anyway")
                self.kill_users()
            else:
                raise ex.excError(str(exc))
        self.remove_holders()
        self.remove_deeper_mounts()
        for _ in range(3):
            ret = self.try_umount()
            if ret == 0:
                break
        if ret != 0:
            raise ex.excError('failed to umount %s'%self.mount_point)
        self.mounts = None

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
            raise ex.excError("resource %s has holders handled by other "
                              "resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
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
        mounts = rcMounts.Mounts()
        mnt_realpath = os.path.realpath(self.mount_point)
        for mount in mounts:
            _mnt_realpath = os.path.realpath(mount.mnt)
            if _mnt_realpath != mnt_realpath and \
               _mnt_realpath.startswith(mnt_realpath+"/"):
                ret = self.try_umount(_mnt_realpath)
                if ret != 0:
                    break

