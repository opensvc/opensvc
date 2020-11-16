"""
Linux Fs resource driver module
"""
import os
from stat import ST_MODE, ST_INO, S_ISREG, S_ISBLK, S_ISDIR

import core.exceptions as ex
import utilities.devices.linux
from env import Env
from utilities.files import protected_mount, getmount
from utilities.cache import cache
from utilities.lazy import lazy
from utilities.subsystems.zfs import zfs_getprop, zfs_setprop, zpool_devs
from utilities.mounts.linux import Mounts
from . import BaseFs
from utilities.proc import justcall, qcall

DRIVER_GROUP = "fs"
DRIVER_BASENAME = ""

class Fs(BaseFs):
    """
    Linux Fs resource driver
    """
    def __init__(self, **kwargs):
        super(Fs, self).__init__(**kwargs)
        self.mounts = None
        self.loopdevice = None

    def set_fsck_h(self):
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

    @lazy
    def device(self):
        if self._device is not None:
            device = self._device
            if self.fs_type == "bind" or "bind" in self.mount_options:
                if not self._device.startswith(os.sep):
                    l = self._device.split("/")
                    vol = self.svc.get_volume(l[0])
                    if vol.mount_point is not None:
                        l[0] = vol.mount_point
                        return "/".join(l)
            device = self._device
        else:
            # lazy reference support
            device = self.conf_get("dev")
        if device is None:
            return device
        dev_realpath = os.path.realpath(device)
        if device.startswith("/dev/disk/by-") or dev_realpath.startswith("/dev/rbd"):
            device = dev_realpath
        return device

    def umount_generic(self, mnt):
        cmd = ['umount', mnt]
        return self.vcall(cmd, err_to_warn=True)

    def try_umount(self, dev=None, mnt=None, fs_type=None):
        if dev is None:
            dev = self.device
        if mnt is None:
            mnt = self.mount_point
        if fs_type is None:
            fs_type = self.fs_type

        if fs_type == "zfs":
            ret, out, err = self.umount_zfs(dev, mnt)
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

        if os.path.isdir(dev):
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

    def is_up(self):
        if self.device is None:
            self.status_log("dev is not defined", "info")
            return False
        if self.mount_point is None:
            self.status_log("mnt is not defined", "info")
            return False
        self.mounts = Mounts()
        for dev in [self.device] + utilities.devices.linux.udevadm_query_symlink(self.device):
            ret = self.mounts.has_mount(dev, self.mount_point)
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
            backfile = utilities.devices.linux.loop_to_file(self.device)
            if backfile and self.mounts.has_mount(backfile, os.path.realpath(self.mount_point)):
                return True

        # might be a mount by label or uuid
        for dev in self.sub_devs():
            ret = self.mounts.has_mount(dev, self.mount_point)
            if ret:
                return True
            ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
            if ret:
                return True
            if dev.startswith("/dev/loop"):
                backfile = utilities.devices.linux.loop_to_file(dev)
                if backfile and self.mounts.has_mount(backfile, os.path.realpath(self.mount_point)):
                    return True

        # might be mount using a /dev/mapper/ name too
        elements = self.device.split('/')
        if len(elements) == 4 and elements[2] != "mapper":
            dev = "/dev/mapper/%s-%s" % (elements[2].replace('-', '--'),
                                         elements[3].replace('-', '--'))
            ret = self.mounts.has_mount(dev, self.mount_point)
            if ret:
                return True
            ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
            if ret:
                return True

        if self.device.startswith(os.sep) and os.path.exists(self.device):
            try:
                fstat = os.stat(self.device)
                mode = fstat[ST_MODE]
            except:
                self.log.debug("can not stat %s", self.device)
                return False

            if S_ISREG(mode):
                # might be a loopback mount
                devs = utilities.devices.linux.file_to_loop(self.device)
                for dev in devs:
                    ret = self.mounts.has_mount(dev, self.mount_point)
                    if ret:
                        return True
                    ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
                    if ret:
                        return True
            elif S_ISBLK(mode):
                # might be a mount using a /dev/dm-<minor> name too
                if os.major(fstat.st_rdev) == self.dm_major:
                    dev = '/dev/dm-' + str(os.minor(fstat.st_rdev))
                    ret = self.mounts.has_mount(dev, self.mount_point)
                    if ret:
                        return True
                    ret = self.mounts.has_mount(dev, os.path.realpath(self.mount_point))
                    if ret:
                        return True
            elif S_ISDIR(mode):
                try:
                    mnt_fstat = os.stat(self.mount_point)
                    mnt_ino = mnt_fstat[ST_INO]
                except:
                    self.log.debug("can not stat %s", self.mount_point)
                    return False
                dev_ino = fstat[ST_INO]
                if dev_ino == mnt_ino:
                    return True

        return False

    def realdev(self):
        if self.fs_type in ("none", "tmpfs", "bind"):
            return
        if self.device is None:
            return
        if self.device.startswith("LABEL=") or self.device.startswith("UUID="):
            try:
                _dev = utilities.devices.linux.label_to_dev(self.device, self.svc.node.devtree)
            except ex.Error as exc:
                self.status_log(str(exc))
                _dev = None
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
                self.mounts = Mounts()
            mount = self.mounts.has_param("mnt", mnt)
            if mount is None:
                self.log.debug("can't find dev %s mounted in %s in mnttab",
                               mnt, self.device)
                return None
            dev = mount.dev
        if dev in ("tmpfs", "shm", "shmfs", "none"):
            # bind mounts for ex.
            return
        return dev

    @cache("dmsetup.ls.multipath")
    def dmsetup_ls_multipath(self):
        cmd = [Env.syspaths.dmsetup, "ls", "--target", "multipath"]
        out, _, _ = justcall(cmd)
        data = {}
        for line in out.splitlines():
            try:
                name, devno = line.replace(", ", ":").split()
            except Exception:
                continue
            data[name] = devno.strip("()")
        return data

    @lazy
    def dmsetup_ls_multipath_rev(self):
        data = {}
        for k, v in self.dmsetup_ls_multipath().items():
            data[v] = k
        return data

    @cache("dmsetup.status")
    def dmsetup_status(self):
        cmd = [Env.syspaths.dmsetup, "status"]
        out, _, _ = justcall(cmd)
        data = {}
        for line in out.splitlines():
            v = line.split()
            data[v[0]] = line[len(v[0]):].strip()
        return data

    @cache("dmsetup.table")
    def dmsetup_table(self):
        cmd = [Env.syspaths.dmsetup, "table"]
        out, _, _ = justcall(cmd)
        data = {}
        for line in out.splitlines():
            v = line.split()
            data[v[0]] = line[len(v[0]):].strip()
        return data

    @lazy
    def dm_major(self):
        try:
            return utilities.devices.linux.major('device-mapper')
        except:
            return

    def mplist(self):
        dev = self.realdev()
        if dev is None:
            return set()
        if self.dm_major is None:
            return set()
        return self._mplist([dev])

    @staticmethod
    def devname_to_dev(devname):
        if 'cciss!' in devname:
            return '/dev/cciss/'+devname.replace('cciss!', '')
        return '/dev/'+devname

    def _mplist(self, devs):
        mps = set()
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
                    self.log.debug("can not stat %s", dev)
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
        devno = "%d:%d" % (self.dm_major, minor)
        if devno not in self.dmsetup_ls_multipath_rev:
            return False
        name = self.dmsetup_ls_multipath_rev[devno]
        dmsetup_table = self.dmsetup_table()
        if name not in dmsetup_table:
            return False
        elements = dmsetup_table[name].split()
        if 'queue_if_no_path' not in elements:
            return False
        dmsetup_status = self.dmsetup_status()
        if name not in dmsetup_status:
            return False
        elements = dmsetup_status[name].split()
        if elements.count('A') > 1:
            return False
        return True

    def is_devmap(self, statinfo):
        if os.major(statinfo.st_rdev) == self.dm_major:
            return True
        return False

    def sub_devs(self):
        if self.fs_type == "btrfs":
            from utilities.subsystems.btrfs import btrfs_devs
            return set(btrfs_devs(self.mount_point))
        if self.fs_type == "zfs":
            if not self.device:
                return set()
            return set(zpool_devs(self.device.split("/")[0], self.svc.node))

        dev = self.realdev()
        if dev is None or dev.startswith("LABEL=") or dev.startswith("UUID="):
            # realdev() may fail to resolve UUID and LABEL if the hosting dev
            # is not visible
            return set()

        if dev.startswith("/dev/rbd") or dev.startswith("/dev/loop"):
            return set([dev])

        if self.dm_major is None:
            return set([dev])

        try:
            statinfo = os.stat(dev)
        except:
            self.log.error("can not stat %s", dev)
            raise ex.Error

        if not self.is_devmap(statinfo):
            return set([dev])

        if utilities.devices.linux.lv_exists(self, dev):
            # If the fs is built on a lv of a private vg, its
            # disks will be given by the vg resource.
            # if the fs is built on a lv of a shared vg, we
            # don't want to account its disks : don't reserve
            # them, don't account their size multiple times.
            return set()

        devname = 'dm-' + str(os.minor(statinfo.st_rdev))
        syspath = '/sys/block/' + devname + '/slaves'
        devs = utilities.devices.linux.get_blockdev_sd_slaves(syspath)
        return devs

    def sub_disks(self):
        return utilities.devices.linux.devs_to_disks(self, self.sub_devs())

    def can_check_writable(self):
        if self.fs_type == "zfs":
            return self._can_check_zfs_writable()
        if len(self.mplist()) > 0:
            self.log.debug("a multipath under fs has queueing enabled and no active path")
            return False
        return True

    def set_loopdevice(self):
        # loopback mount
        # if the file has already been binded to a loop re-use
        # the loopdev to avoid allocating another one
        if not os.path.exists(self.device):
            return
        if not os.path.isfile(self.device):
            return
        try:
            devs = utilities.devices.linux.file_to_loop(self.device)
            if len(devs) > 0:
                self.loopdevice = devs[0]
                mntopt_l = self.mount_options.split(',')
                if "loop" in mntopt_l:
                    mntopt_l.remove("loop")
                    self.mount_options = ','.join(mntopt_l)
        except Exception as exc:
            raise ex.Error(str(exc))

    def start_mount(self):
        if self.mounts is None:
            self.mounts = Mounts()
        self.prepare_mount()

        self.set_loopdevice()

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
                raise ex.Error(str(exc))

        if self.fs_type == "zfs":
            self.mount_zfs()
        else:
            self.mount_generic()

        self.mounts = None
        self.can_rollback = True

    def _can_check_zfs_writable(self):
        pool = self.device.split("/")[0]
        cmd = [Env.syspaths.zpool, "status", pool]
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

    def umount_zfs(self, dev, mnt):
        mntprop = zfs_getprop(dev, 'mountpoint')
        if mntprop == "legacy":
            return self.umount_generic(mnt)
        elif mntprop != mnt:
            # docker data dir case, ex: dev=data/svc1 mnt=/srv/svc1/docker/zfs
            # and mntprop=/srv/svc1
            return self.umount_generic(mnt)
        else:
            return self.umount_zfs_native(mnt)

    def umount_zfs_native(self, mnt):
        ret, out, err = self.vcall([Env.syspaths.zfs, 'umount', mnt], err_to_info=True)
        if ret != 0:
            ret, out, err = self.vcall([Env.syspaths.zfs, 'umount', '-f', mnt], err_to_info=True)
        return ret, out, err

    def mount_zfs(self):
        zone = self.zone or self.oget("zone")
        if not self.encap and not zone and \
           zfs_getprop(self.device, 'zoned') != 'off':
            if zfs_setprop(self.device, 'zoned', 'off', log=self.log):
                raise ex.Error
        try:
            os.unlink(self.mount_point+"/.opensvc")
        except:
            pass
        if zfs_getprop(self.device, 'mountpoint') == "legacy":
            return self.mount_generic()
        else:
            return self.mount_zfs_native()

    def mount_zfs_native(self):
        if zfs_getprop(self.device, 'mountpoint') != self.mount_point:
            if not zfs_setprop(self.device, 'mountpoint', self.mount_point, log=self.log):
                raise ex.Error
            # the prop change has mounted the dataset
            return
        ret, out, err = self.vcall([Env.syspaths.zfs, 'mount', self.device])
        if ret != 0:
            ret, out, err = self.vcall([Env.syspaths.zfs, 'mount', '-O', self.device])
            if ret != 0:
                raise ex.Error
        return ret, out, err

    def mount_generic(self):
        if self.fs_type and self.fs_type != "bind":
            fstype = ['-t', self.fs_type]
        else:
            fstype = []

        if self.mount_options:
            opt = self.mount_options.strip().split(",")
        else:
            opt = []
        if self.fs_type == "bind" and not "bind" in opt:
            opt.append("bind")
        if opt:
            mntopt = ['-o', ",".join(opt)]
        else:
            mntopt = []

        if self.loopdevice is None:
            device = self.device
        else:
            device = self.loopdevice

        cmd = ['mount'] + fstype + mntopt + [device, self.mount_point]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

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
            self.mounts = Mounts()
        if self.is_up() is False:
            self.log.info("%s is already umounted", self.label)
            return
        try:
            os.stat(self.mount_point)
        except OSError as exc:
            if exc.errno in (5, 13):
                self.log.warning("I/O error on mount point. try to umount anyway")
                self.kill_users()
            else:
                raise ex.Error(str(exc))
        self.remove_holders()
        self.remove_deeper_mounts()
        for _ in range(3):
            ret = self.try_umount()
            if ret == 0:
                break
        if ret != 0:
            raise ex.Error('failed to umount %s'%self.mount_point)
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
        holders_handled_by_resources = self.svc.exposed_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.Error("resource %s has holders handled by other "
                              "resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        if not self.svc:
            return
        tree = self.svc.node.devtree
        dev_realpath = os.path.realpath(self.device)
        self.remove_dev_holders(dev_realpath, tree)

    def remove_deeper_mounts(self):
        mounts = Mounts()
        mnt_realpath = os.path.realpath(self.mount_point)
        for mount in sorted(mounts, key=lambda x: x.mnt, reverse=True):
            _mnt_realpath = os.path.realpath(mount.mnt)
            if _mnt_realpath != mnt_realpath and \
               _mnt_realpath.startswith(mnt_realpath+"/"):
                ret = self.try_umount(dev=mount.dev, mnt=_mnt_realpath, fs_type=mount.type)
                if ret != 0:
                    break

    def lv_name(self):
        dev = self.oget("dev")

        if dev.startswith("LABEL=") or dev.startswith("UUID="):
            try:
                _dev = utilities.devices.linux.label_to_dev(dev, tree=self.svc.node.devtree)
            except ex.Error as exc:
                _dev = None
            if _dev is None:
                self.log.info("unable to find device identified by %s", dev)
                return
            dev = _dev

        vg = self.oget("vg")

        if dev.startswith('/dev/mapper/'):
            dev = dev.replace(vg.replace('-', '--')+'-', '')
            dev = dev.replace('--', '-')
            return "/dev/"+vg+"/"+os.path.basename(dev)
        if "/"+vg+"/" in dev:
            return dev

