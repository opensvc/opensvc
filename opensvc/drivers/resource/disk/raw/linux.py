import os

import core.exceptions as ex
import utilities.lock
import utilities.devices

from . import BaseDiskRaw, BASE_RAW_KEYWORDS
from env import Env
from utilities.cache import cache
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall
from utilities.devices.linux import loop_to_file

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "raw"
KEYWORDS = BASE_RAW_KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class DiskRaw(BaseDiskRaw):
    def __init__(self, **kwargs):
        super(DiskRaw, self).__init__(**kwargs)
        self.min_raw = 1
        self.sys_devs = {}

    @lazy
    def devs_t(self):
        devs_t = {}
        for dev in self.devs:
            d = os.stat(dev)
            devs_t[dev] = (os.major(d.st_rdev), os.minor(d.st_rdev))
        return devs_t

    @cache("raw.rdevs_t")
    def _rdevs_t(self):
        rdevs_t = {}
        for dirpath, dirnames, filenames in os.walk('/dev/raw'):
            for filename in filenames:
                f = os.path.join(dirpath, filename)
                d = os.stat(f)
                key = "%d:%d" % (os.major(d.st_rdev), os.minor(d.st_rdev))
                rdevs_t[key] = filename
        return rdevs_t

    @lazy
    def rdevs_t(self):
        return self._rdevs_t()

    @cache("raw.modprobe")
    def modprobe(self):
        """
        Load the raw driver if necessary.
        Cached to execute only once per run. The result is of no interest.
        """
        cmd = ["raw", "-qa"]
        err, ret, out = justcall(cmd)
        if ret == 0:
            # no need to load (already loaded or compiled-in)
            return
        cmd = [Env.syspaths.lsmod]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error
        if "raw" in out.split():
            return
        cmd = ["modprobe", "raw"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to load raw device driver")
            raise ex.Error

    @cache("raw.list")
    def get_raws(self):
        self.modprobe()
        raws = {}
        sysfs_raw_path = os.path.join(os.sep, 'sys', 'class', 'raw')
        for dirpath, dirnames, filenames in os.walk(sysfs_raw_path):
            for dirname in dirnames:
                if dirname == 'rawctl':
                    continue
                dev = os.path.join(dirpath, dirname, 'dev')
                if not os.path.exists(dev):
                    continue
                with open(dev, 'r') as f:
                    buff = f.read()
                try:
                    major, minor = buff.strip().split(':')
                except:
                    continue
                raws[dirname] = {'rdev': (int(major), int(minor))}

        cmd = ['raw', '-qa']
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.error('failed to fetch raw device bindings')
            raise ex.Error
        for line in out.split('\n'):
            l = line.split()
            if len(l) != 7:
                continue
            raw = l[0].strip(':').replace('/dev/raw/','')
            major = int(l[4].strip(','))
            minor = int(l[6])
            if raw in raws:
                raws[raw]['bdev'] = (major, minor)
        return raws

    @lazy
    def raws(self):
        raws = self.get_raws()
        for dev, dev_t in self.devs_t.items():
            for raw, d in raws.items():
                if list(dev_t) == list(d['bdev']):
                    raws[raw]['devname'] = dev

        for raw, d in raws.items():
            key = "%d:%d" % (d['rdev'][0], d['rdev'][1])
            if key in self.rdevs_t:
                raws[raw]['rdevname'] = self.rdevs_t[key]
        return raws

    def find_next_raw(self):
        allocated = set(map(lambda x: x['rdev'][1], self.raws.values()))
        candidates = set(range(1, 255))
        candidates -= allocated
        if len(candidates) == 0:
            self.log.error("no more raw device can be allocated")
            raise ex.Error
        return 'raw%d'%sorted(list(candidates))[0]

    def find_raw(self, dev):
        for raw, d in self.raws.items():
            if 'devname' in d and dev == d['devname']:
                return raw
        return None

    def lock(self, timeout=30, delay=1):
        lockfile = os.path.join(Env.paths.pathlock, 'startvgraw')
        lockfd = None
        try:
            lockfd = utilities.lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except utilities.lock.LockTimeout:
            self.log.error("timed out waiting for lock")
            raise ex.Error
        except utilities.lock.LockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.Error
        except utilities.lock.LockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.Error
        except utilities.lock.LockAcquire as e:
            self.log.warning("another action is currently running (pid=%s)"%e.pid)
            raise ex.Error
        except ex.Signal:
            self.log.error("interrupted by signal")
            raise ex.Error
        except:
            self.save_exc()
            raise ex.Error("unexpected locking error")
        self.lockfd = lockfd

    def unlock(self):
        utilities.lock.unlock(self.lockfd)

    def has_it_char_devices(self):
        r = True
        l = []
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is None:
                l.append(dev)
                r &= False
        if len(l) > 0 and len(l) < len(self.devs):
            self.status_log("%s not mapped to a raw device"% ", ".join(l))
        return r

    def mangle_devs_map(self):
        if not self.create_char_devices:
            return
        for dev in self.devs:
            raw = self.find_raw(dev)
            if dev in self.devs_map:
                if raw is not None:
                    self.devs_map["/dev/raw/"+raw] = self.devs_map[dev]
                del(self.devs_map[dev])

    def do_start_char_devices(self):
        if not self.create_char_devices:
            return
        self.lock()
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is not None:
                self.log.info("%s is already mapped to a raw device"%dev)
            else:
                raw = self.find_next_raw()
                cmd = ['raw', "/dev/raw/"+raw, dev]
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    self.unlock()
                    raise ex.Error
                s = os.stat("/dev/raw/"+raw)
                self.raws[raw] = {
                  'rdev': (os.major(s.st_rdev), os.minor(s.st_rdev)),
                  'devname': dev,
                  'bdev': self.devs_t[dev]
                }

        self.clear_caches()
        self.unlock()

    def clear_caches(self):
        self.clear_cache("raw.list")
        self.clear_cache("raw.rdevs_t")
        self.unset_lazy("devs_t")
        self.unset_lazy("rdevs_t")
        self.unset_lazy("raws")

    def do_stop_char_devices(self):
        if not self.create_char_devices:
            return
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is not None:
                cmd = ['raw', '/dev/raw/raw%d'%self.raws[raw]['rdev'][1], '0', '0']
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    raise ex.Error
                del(self.raws[raw])
        self.clear_caches()

    def load_sys_devs(self):
        import glob
        if self.sys_devs != {}:
            return
        for e in glob.glob('/sys/block/*/dev'):
            with open(e, 'r') as f:
                dev = e.replace('/sys/block/', '').replace('/dev', '')
                dev_t = f.read().strip()
                self.sys_devs[dev_t] = '/dev/'+dev

    def sub_disks(self):
        sys_devs = self.sub_devs()
        return utilities.devices.devs_to_disks(self, sys_devs)

    def sub_devs(self):
        """ Admins can set arbitrary named devices, for example /dev/oracle/DGREDO_MYSID.
            Resolve those names into well known systems device names, so that they can be
            found in the DevTree
        """
        self.validate_devs()
        if not self.create_char_devices:
            return self.devs
        sys_devs = set()
        self.load_sys_devs()
        for dev in self.devs:
            if dev not in self.devs_t:
                continue
            dev_t = ':'.join(map(str, self.devs_t[dev]))
            if dev_t not in self.sys_devs:
                continue
            sys_dev = self.sys_devs[dev_t]
            sys_devs.add(sys_dev)
        return sys_devs

    def verify_dev(self, dev):
        if dev.startswith("/dev/loop"):
            fpath = loop_to_file(dev)
            return fpath is not None
        return True
