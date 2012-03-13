#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import resVgRaw
import os
import rcStatus
import re
import rcExceptions as ex
from rcGlobalEnv import *

class Vg(resVgRaw.Vg):
    def __init__(self, rid=None, devs=set([]), user="root",
                 group="root", perm="660", type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False):
        
        resVgRaw.Vg.__init__(self, rid=rid,
                             devs=devs,
                             user=user,
                             group=group,
                             perm=perm,
                             type=type,
                             optional=optional,
                             disabled=disabled,
                             tags=tags,
                             always_on=always_on,
                             monitor=monitor)
        self.min_raw = 1
        self.raws = {}

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
        ret, out, err = self.call(cmd)
        if ret == 0:
            # no need to load (already loaded or compiled-in)
            return
        cmd = ["lsmod"]
        ret, out, err = self.call(cmd)
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
        ret, out, err = self.call(cmd)
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
        return '/dev/raw/raw%d'%sorted(list(candidates))[0]
        
    def devname_to_rdevname(self, devname):
        b = os.path.basename(devname)
        return '/dev/raw/'+self.svc.svcname+"."+b

    def find_raw(self, dev):
        for raw, d in self.raws.items():
            if 'devname' in d and dev == d['devname']:
                return raw
        return None

    def lock(self, timeout=30, delay=1):
        import lock
        lockfile = os.path.join(rcEnv.pathlock, 'startvgraw')
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
            self.log.error("unexpected locking error")
            import traceback
            traceback.print_exc()
            raise ex.excError
        self.lockfd = lockfd

    def unlock(self):
        import lock
        lock.unlock(self.lockfd)

    def has_it(self):
        """Returns True if all raw devices are present and correctly
           named
        """
        r = False
        if len(self.devs_not_found) > 0:
            self.status_log("%s not found"%', '.join(self.devs_not_found))
            r |= True
        self.get_raws()
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is None:
                self.status_log("%s is not mapped to a raw device"%dev)
                r |= True
            elif '/dev/raw/'+self.raws[raw]['rdevname'] != self.devname_to_rdevname(dev):
                self.status_log("%s raw device is named %s, expected %s"%(dev, '/dev/raw/'+self.raws[raw]['rdevname'], self.devname_to_rdevname(dev)))
                r |= True
            elif not self.check_uid('/dev/raw/'+self.raws[raw]['rdevname'], verbose=True):
                r |= True
            elif not self.check_gid('/dev/raw/'+self.raws[raw]['rdevname'], verbose=True):
                r |= True
            elif not self.check_perm('/dev/raw/'+self.raws[raw]['rdevname'], verbose=True):
                r |= True
        return not r

    def _status(self, verbose=False):
        if rcEnv.nodename in self.always_on:
            if self.is_up(): return rcStatus.STDBY_UP
            else: return rcStatus.STDBY_DOWN
        else:
            if self.is_up(): return rcStatus.UP
            else: return rcStatus.DOWN

    def do_start(self):
        self.lock()
        self.get_raws()
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is not None:
                self.log.info("%s is already mapped to a raw device"%dev)
                rdevname = '/dev/raw/'+self.raws[raw]['rdevname']
                if rdevname != self.devname_to_rdevname(dev):
                    cmd = ['mv', rdevname, self.devname_to_rdevname(dev)]
                    ret, out, err = self.vcall(cmd)
                    if ret != 0:
                        self.unlock()
                        raise ex.excError
                else:
                    self.log.info("%s is correctly named"%rdevname)
            else:
                raw = self.find_next_raw()
                cmd = ['raw', raw, dev]
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    self.unlock()
                    raise ex.excError
                s = os.stat(raw)
                self.raws[raw] = {'rdevname': self.devname_to_rdevname(dev),
                                  'rdev': (os.major(s.st_rdev), os.minor(s.st_rdev)),
                                  'devname': dev,
                                  'bdev': self.devs_t[dev]}
                cmd = ['mv', raw, self.devname_to_rdevname(dev)]
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    self.unlock()
                    raise ex.excError

            rdevname = self.raws[raw]['rdevname']
            if not self.check_uid(rdevname) or not self.check_gid(rdevname):
                self.vcall(['chown', ':'.join((str(self.uid),str(self.gid))), rdevname])
            else:
                self.log.info("%s has correct ownership"%rdevname)
            if not self.check_perm(rdevname):
                self.vcall(['chmod', self.perm, rdevname])
            else:
                self.log.info("%s has correct permissions"%rdevname)

        self.unlock()

    def do_stop(self):
        self.get_raws()
        for dev in self.devs:
            raw = self.find_raw(dev)
            if raw is not None:
                cmd = ['raw', '/dev/raw/raw%d'%self.raws[raw]['rdev'][1], '0', '0']
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    raise ex.excError
            rdevname = self.devname_to_rdevname(dev)
            if os.path.exists(rdevname):
                if '*' in rdevname:
                    self.log.error("no wildcard allow in raw device names")
                    raise ex.excError
                cmd = ['rm', '-f', rdevname]
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    raise ex.excError
