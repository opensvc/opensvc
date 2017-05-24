import resDiskRaw
import os
import rcStatus
import re
import rcExceptions as ex
from rcGlobalEnv import *
from rcUtilities import justcall

class Disk(resDiskRaw.Disk):
    def __init__(self,
                 rid=None,
                 devs=set([]),
                 create_char_devices=True,
                 user=None,
                 group=None,
                 perm=None,
                 **kwargs):

        resDiskRaw.Disk.__init__(self,
                             rid=rid,
                             devs=devs,
                             user=user,
                             group=group,
                             perm=perm,
                             create_char_devices=create_char_devices,
                             **kwargs)
        self.min_raw = 1
        self.raws = {}
        self.sys_devs = {}

    def get_devs_t(self):
        self.devs_t = {}
        for dev in self.devs:
            d = os.stat(dev)
            self.devs_t[dev] = (os.major(d.st_rdev), os.minor(d.st_rdev))

    def get_rdevnames(self):
        self.rdevs_t = {}
        for dirpath, dirnames, filenames in os.walk('/dev/raw'):
            for filename in filenames:
                f = os.path.join(dirpath, filename)
                d = os.stat(f)
                self.rdevs_t[(os.major(d.st_rdev), os.minor(d.st_rdev))] = filename

    def modprobe(self):
        cmd = ["raw", "-qa"]
        err, ret, out = justcall(cmd)
        if ret == 0:
            # no need to load (already loaded or compiled-in)
            return
        cmd = ["lsmod"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError
        if "raw" in out.split():
            return
        cmd = ["modprobe", "raw"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to load raw device driver")
            raise ex.excError

    def get_raws(self):
        self.modprobe()
        self.sysfs_raw_path = os.path.join(os.sep, 'sys', 'class', 'raw')
        for dirpath, dirnames, filenames in os.walk(self.sysfs_raw_path):
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
                self.raws[dirname] = {'rdev': (int(major), int(minor))}

        cmd = ['raw', '-qa']
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.error('failed to fetch raw device bindings')
            raise ex.excError
        for line in out.split('\n'):
            l = line.split()
            if len(l) != 7:
                continue
            raw = l[0].strip(':').replace('/dev/raw/','')
            major = int(l[4].strip(','))
            minor = int(l[6])
            if raw in self.raws:
                self.raws[raw]['bdev'] = (major, minor)

        self.get_devs_t()
        for dev, dev_t in self.devs_t.items():
            for raw, d in self.raws.items():
                if dev_t == d['bdev']:
                    self.raws[raw]['devname'] = dev

        self.get_rdevnames()
        for raw, d in self.raws.items():
            if d['rdev'] in self.rdevs_t:
                self.raws[raw]['rdevname'] = self.rdevs_t[d['rdev']]


    def find_next_raw(self):
        allocated = set(map(lambda x: x['rdev'][1], self.raws.values()))
        candidates = set(range(1, 255))
        candidates -= allocated
        if len(candidates) == 0:
            self.log.error("no more raw device can be allocated")
            raise ex.excError
        return 'raw%d'%sorted(list(candidates))[0]

    def find_raw(self, dev):
        for raw, d in self.raws.items():
            if 'devname' in d and dev == d['devname']:
                return raw
        return None

    def lock(self, timeout=30, delay=1):
        import lock
        lockfile = os.path.join(rcEnv.paths.pathlock, 'startvgraw')
        lockfd = None
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            self.log.error("timed out waiting for lock")
            raise ex.excError
        except lock.lockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.excError
        except lock.lockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.excError
        except lock.lockAcquire as e:
            self.log.warn("another action is currently running (pid=%s)"%e.pid)
            raise ex.excError
        except ex.excSignal:
            self.log.error("interrupted by signal")
            raise ex.excError
        except:
            self.save_exc()
            raise ex.excError("unexpected locking error")
        self.lockfd = lockfd

    def unlock(self):
        import lock
        lock.unlock(self.lockfd)

    def has_it_char_devices(self):
        r = False
        self.get_raws()
        l = []
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is None:
                l.append(dev)
                r |= True
        if len(l) > 0 and len(l) < len(self.devs):
            self.status_log("%s not mapped to a raw device"% ", ".join(l))
        return not r

    def mangle_devs_map(self):
        self.get_raws()
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
        self.get_raws()
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
                    raise ex.excError
                s = os.stat("/dev/raw/"+raw)
                self.raws[raw] = {
                  'rdev': (os.major(s.st_rdev), os.minor(s.st_rdev)),
                  'devname': dev,
                  'bdev': self.devs_t[dev]
                }

        self.unlock()

    def do_stop_char_devices(self):
        if not self.create_char_devices:
            return
        self.get_raws()
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is not None:
                cmd = ['raw', '/dev/raw/raw%d'%self.raws[raw]['rdev'][1], '0', '0']
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    raise ex.excError
                del(self.raws[raw])

    def load_sys_devs(self):
        import glob
        if self.sys_devs != {}:
            return
        for e in glob.glob('/sys/block/*/dev'):
            with open(e, 'r') as f:
                dev = e.replace('/sys/block/', '').replace('/dev', '')
                dev_t = f.read().strip()
                self.sys_devs[dev_t] = '/dev/'+dev

    def disklist(self):
        sys_devs = self.devlist()
        from rcUtilitiesLinux import devs_to_disks
        return devs_to_disks(self, sys_devs)

    def devlist(self):
        """ Admins can set arbitrary named devices, for example /dev/oracle/DGREDO_MYSID.
            Resolve those names into well known systems device names, so that they can be
            found in the DevTree
        """
        self.validate_devs()
        if not self.create_char_devices:
            return self.devs
        sys_devs = set([])
        self.load_sys_devs()
        self.get_devs_t()
        for dev in self.devs:
            if dev not in self.devs_t:
                continue
            dev_t = ':'.join(map(str, self.devs_t[dev]))
            if dev_t not in self.sys_devs:
                continue
            sys_dev = self.sys_devs[dev_t]
            sys_devs.add(sys_dev)
        return sys_devs

