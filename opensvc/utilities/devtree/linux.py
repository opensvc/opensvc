from __future__ import division

import glob
import os
import re
from subprocess import *

import math

from .devtree import DevTree as BaseDevTree, Dev as BaseDev
from .veritas import DevTreeVeritas
import core.exceptions as ex
from core.capabilities import capabilities
from env import Env
from utilities.mounts import Mounts


class Dev(BaseDev):
    def remove_loop(self, r):
        cmd = [Env.syspaths.losetup, "-d", self.devpath[0]]
        ret, out, err = r.vcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        self.removed = True

    def remove_dm(self, r):
        cmd = [Env.syspaths.dmsetup, "remove", self.alias]
        ret, out, err = r.vcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        self.removed = True

    def remove(self, r):
        if self.removed:
            return
        if self.devname.startswith("loop"):
            return self.remove_loop(r)
        if self.devname.startswith("dm-"):
            return self.remove_dm(r)

class DevTree(DevTreeVeritas, BaseDevTree):
    di = None
    dev_h = {}
    dev_class = Dev

    def get_size(self, devpath):
        size = 0
        try:
            with open(devpath+'/size', 'r') as f:
                size = int(f.read().strip()) // 2048
        except:
            pass
        return size

    def get_dm(self):
        try:
            return getattr(self, "dm_h")
        except AttributeError:
            pass
        self.dm_h = {}
        self._dm_h = {}
        if not os.path.exists("/dev/mapper"):
            return self.dm_h
        try:
            cmd = [Env.syspaths.dmsetup, 'mknodes']
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            p.communicate()
        except:
            # best effort
            pass
        devpaths = glob.glob("/dev/mapper/*")
        if '/dev/mapper/control' in devpaths:
            devpaths.remove('/dev/mapper/control')
        for devpath in devpaths:
            try:
                s = os.stat(devpath)
            except OSError:
                continue
            minor = os.minor(s.st_rdev)
            self.dm_h[devpath.replace("/dev/mapper/", "")] = "dm-%d"%minor

        # reverse hash
        for mapname, devname in self.dm_h.items():
            self._dm_h[devname] = mapname

        return self.dm_h

    def get_map_wwid(self, map):
        if "node.x.multipath" not in capabilities:
            return None
        if not hasattr(self, 'multipath_l'):
            self.multipath_l = []
            cmd = [Env.syspaths.multipath, '-l']
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
            if p.returncode != 0:
                return None
            self.multipath_l = out.decode().splitlines()
        for line in self.multipath_l:
            if not line.startswith(map):
                continue
            try:
                wwid = line[line.index('(')+2:line.index(')')]
            except ValueError:
                wwid = line.split()[0]
            return wwid
        return None

    def get_wwid(self):
        try:
            return getattr(self, 'wwid_h')
        except AttributeError:
            pass
        self.wwid_h = {}
        self.wwid_h.update(self.get_wwid_native())
        self.wwid_h.update(self.get_mp_powerpath())
        return self.wwid_h

    def get_wwid_native(self):
        if "node.x.multipath" not in capabilities:
            return self.wwid_h
        cmd = [Env.syspaths.multipath, '-l']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return self.wwid_h
        for line in out.decode().splitlines():
            if 'dm-' not in line:
                continue
            devname = line[line.index('dm-'):].split()[0]
            try:
                wwid = line[line.index('(')+2:line.index(')')]
            except ValueError:
                wwid = line.split()[0][1:]
            self.wwid_h[devname] = wwid
        return self.wwid_h

    def get_mp(self):
        try:
            return getattr(self, 'mp_h')
        except AttributeError:
            pass
        self.mp_h = {}
        self.mp_h.update(self.get_mp_native())
        self.mp_h.update(self.get_mp_powerpath())
        return self.mp_h

    def get_mp_powerpath(self):
        self.powerpath = {}
        if "node.x.powermt" not in capabilities:
            return {}
        cmd = ['powermt', 'display', 'dev=all']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return {}
        lines = out.decode().splitlines()
        if len(lines) < 1:
            return {}
        dev = None
        name = None
        paths = []
        mp_h = {}
        for line in lines:
            if len(line) == 0:
                # new mpath
                # - store previous
                # - reset path counter
                if dev is not None:
                    if len(paths) > 0:
                        did = self.di.disk_id(paths[0])
                    mp_h[name] = did
                    self.powerpath[name] = paths
                    dev = None
                    paths = []
            if 'Pseudo name' in line:
                l = line.split('=')
                if len(l) != 2:
                    continue
                name = l[1]
                dev = "/dev/"+name
            else:
                l = line.split()
                if len(l) < 3:
                    continue
                if l[2].startswith("sd"):
                    paths.append("/dev/"+l[2])
        return mp_h

    def get_mp_native(self):
        if "node.x.dmsetup" not in capabilities:
            return {}
        cmd = [Env.syspaths.dmsetup, 'ls', '--target', 'multipath']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return {}
        mp_h = {}
        for line in out.decode().splitlines():
            l = line.split()
            if len(l) == 0:
                continue
            mapname = l[0]
            major = l[1].strip('(,')
            minor = l[2].strip(' )')
            mp_h['dm-'+minor] = mapname
        return mp_h

    def get_md(self):
        try:
            return getattr(self, "md_h")
        except AttributeError:
            pass
        fpath = "/proc/mdstat"
        self.md_h = {}
        try:
            with open(fpath, 'r') as f:
                buff = f.read()
        except:
            return self.md_h
        for line in buff.split('\n'):
            if line.startswith("Personalities"):
                continue
            if len(line) == 0 or line[0] == " ":
                continue
            l = line.split()
            if len(l) < 4:
                continue
            self.md_h[l[0]] = l[3]
        return self.md_h

    def load_dm_dev_t(self):
        table = {}
        if "node.x.dmsetup" not in capabilities:
            return
        cmd = [Env.syspaths.dmsetup, 'ls']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        for line in out.decode().splitlines():
            l = line.split()
            if len(l) == 0:
                continue
            mapname = l[0]
            major = l[1].strip('(,')
            minor = l[2].strip(' )')
            dev_t = ':'.join((major, minor))
            self.dev_h[dev_t] = mapname

    def load_dm(self):
        table = {}
        self.load_dm_dev_t()
        if "node.x.dmsetup" not in capabilities:
            return
        cmd = [Env.syspaths.dmsetup, 'table']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        for line in out.decode().splitlines():
            l = line.split()
            if len(l) < 5:
                continue
            mapname = l[0].strip(':')
            size = int(math.ceil(1.*int(l[2])*512/1024/1024))
            maptype = l[3]

            if maptype == "multipath" and size in [0, 2, 3, 30, 45]:
                continue

            for w in l[4:]:
                if ':' not in w:
                    continue
                if mapname not in table:
                    table[mapname] = {"devs": [], "size": 0, "type": "linear"}
                table[mapname]["devs"].append(w)
                table[mapname]["size"] += size
                table[mapname]["type"] = maptype
        for mapname in table:
            d = self.add_dev(mapname, table[mapname]["size"])
            d.set_devtype(table[mapname]["type"])
            d.set_devpath('/dev/mapper/'+mapname)

            s = mapname.replace('--', ':').replace('-', '/').replace(':','-')
            if "/" in s:
                d.dg = s.split("/", 1)[0]
                d.set_devpath('/dev/'+s)
            wwid = self.get_map_wwid(mapname)
            if wwid is not None:
                d.set_alias(wwid)
            for dev in table[mapname]["devs"]:
                if dev not in self.dev_h:
                    continue
                d.add_parent(self.dev_h[dev])
                parentdev = self.get_dev(self.dev_h[dev])
                parentdev.add_child(mapname)

    def set_udev_symlink(self, d, name):
        if "node.x.udevadm" not in capabilities:
            return
        cmd = ["udevadm", "info", "-q", "symlink", "--name", name]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        for s in out.decode().split():
            d.set_devpath("/dev/"+s)

    def get_lv_linear(self):
        try:
            return getattr(self, "lv_linear")
        except AttributeError:
            pass
        self.lv_linear = {}
        if "node.x.dmsetup" not in capabilities:
            return self.lv_linear
        cmd = [Env.syspaths.dmsetup, 'table', '--target', 'linear']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return self.lv_linear
        for line in out.decode().splitlines():
            l = line.split(':')
            if len(l) < 2:
                continue
            mapname = l[0]
            line = line[line.index(':')+1:]
            l = line.split()
            if len(l) < 3:
                continue
            length = int(l[1])*512/1024/1024
            devt = l[3]
            if mapname in self.lv_linear:
                self.lv_linear[mapname].append((devt, length))
            else:
                self.lv_linear[mapname] = [(devt, length)]
        return self.lv_linear

    def is_cdrom(self, devname):
        p = '/sys/block/%s/device/media'%devname
        if not os.path.exists(p):
            return False
        with open(p, 'r') as f:
            buff = f.read()
        if buff.strip() == "cdrom":
            return True
        return False

    def get_loop(self):
        self.loop = {}
        cmd = [Env.syspaths.losetup]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        for line in out.decode().splitlines():
            if not line.startswith("/"):
                continue
            l = line.split()
            if len(l) < 2:
                continue
            loop = l[0].replace("/dev/", "")
            for idx in range(1, len(l)):
                fpath = l[-idx]
                if fpath.startswith("/"):
                    break
            self.loop[loop] = fpath

    def dev_type(self, devname):
        t = "linear"
        md_h = self.get_md()
        mp_h = self.get_mp()
        if devname in md_h:
            return md_h[devname]
        if devname in mp_h:
            return "multipath"
        return t

    def add_loop_relations(self):
        self.get_loop()
        m = Mounts()
        for devname, fpath in self.loop.items():
            if fpath == "(deleted)":
                continue
            parentpath = m.get_fpath_dev(fpath)
            if parentpath is None:
                continue
            d = self.get_dev_by_devpath(parentpath)
            if d is None:
                continue
            d.add_child(devname)
            c = self.get_dev(devname)
            r = c.add_parent(d.devname, size=c.size)

    def add_drbd_relations(self):
        if "node.x.drbdadm" not in capabilities or not os.path.exists("/proc/drbd"):
            return
        cmd = ["drbdadm", "dump-xml"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        from xml.etree import ElementTree as etree
        tree = etree.fromstring(out.decode())
        for res in tree.getiterator('resource'):
            for host in res.findall('host'):
                if host.attrib['name'] != Env.nodename:
                    continue
                edisk = host.find('disk')
                edev = host.find('device')
                if edisk is None or edev is None:
                    edisk = host.find('volume/disk')
                    edev = host.find('volume/device')
                if edisk is None or edev is None:
                    continue
                devname = 'drbd'+edev.attrib['minor']
                parentpath = edisk.text
                d = self.get_dev_by_devpath(parentpath)
                if d is None:
                    continue
                d.add_child(devname)
                c = self.get_dev(devname)
                c.add_parent(d.devname)

    def load_dev(self, devname, devpath):
        if self.is_cdrom(devname):
            return

        mp_h = self.get_mp()
        wwid_h = self.get_wwid()
        size = self.get_size(devpath)

        # exclude 0-sized md, Symmetrix gatekeeper and vcmdb
        if devname in self.mp_h and size in (0, 2, 30, 45):
            return

        devtype = self.dev_type(devname)
        d = self.add_dev(devname, size, devtype)

        if d is None:
            return

        self.set_udev_symlink(d, devname)
        self.get_dm()

        if 'cciss' in devname:
            d.set_devpath('/dev/'+devname.replace('!', '/'))
        elif devname in self.mp_h:
            if devname in self._dm_h:
                d.set_devpath('/dev/mpath/'+self._dm_h[devname])
                d.set_devpath('/dev/'+devname)
        else:
            d.set_devpath('/dev/'+devname)

        # store devt
        try:
            with open("%s/dev"%devpath, 'r') as f:
                devt = f.read().strip()
                self.dev_h[devt] = devname
        except IOError:
            pass

        # add holders
        for holderpath in glob.glob("%s/holders/*"%devpath):
            holdername = os.path.basename(holderpath)
            if not os.path.exists(holderpath):
                # broken symlink
                continue
            size = self.get_size(holderpath)
            devtype = self.dev_type(holdername)
            if d.dg == "" and holdername in self._dm_h:
                alias = self._dm_h[holdername]
                s = alias.replace('--', ':').replace('-', '/').replace(':','-')
                d.dg = s.split("/", 1)[0]
            d.add_child(holdername, size, devtype)

        # add lv aliases
        if devname in self._dm_h:
            alias = self._dm_h[devname]
            d.set_alias(alias)
            d.set_devpath('/dev/mapper/'+alias)
            s = alias.replace('--', ':').replace('-', '/').replace(':','-')
            d.dg = s.split("/", 1)[0]
            d.set_devpath('/dev/'+s)

        # add slaves
        for slavepath in glob.glob("%s/slaves/*"%devpath):
            slavename = os.path.basename(slavepath)
            if not os.path.exists(slavepath):
                # broken symlink
                continue
            size = self.get_size(slavepath)
            devtype = self.dev_type(slavename)
            d.add_parent(slavename, size, devtype)

        if devname in wwid_h:
            wwid = wwid_h[devname]
            d.set_alias(wwid)
            try:
                p = glob.glob('/dev/mpath/?'+wwid)[0]
                d.set_devpath(p)
            except:
                pass

        return d

    def get_dev_t(self, dev):
        major, minor = self._get_dev_t(dev)
        return ":".join((str(major), str(minor)))

    def _get_dev_t(self, dev):
        try:
            s = os.stat(dev)
            minor = os.minor(s.st_rdev)
            major = os.major(s.st_rdev)
        except:
            return 0, 0
        return major, minor

    def load_fdisk(self):
        self.get_wwid()
        p = Popen(["fdisk", "-l"], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        for line in out.decode().splitlines():
            if line.startswith('/dev/dm-'):
                continue
            elif line.startswith("Disk "):
                # disk
                devpath = line.split()[1].strip(':')
                if devpath.startswith('/dev/dm-'):
                    continue
                size = int(line.split()[-2]) / 1024 / 1024
                if size in [2, 3, 30, 45]:
                    continue
                devname = devpath.replace('/dev/','').replace("/","!")
                devtype = self.dev_type(devname)
                dev_t = self.get_dev_t(devpath)
                self.dev_h[dev_t] = devname
                d = self.add_dev(devname, size, devtype)
                if d is None:
                    continue
                d.set_devpath(devpath)
                if devname.startswith('emc') and devname in self.wwid_h:
                    d.set_alias(self.wwid_h[devname])
                    for path in self.powerpath[devname]:
                        p = self.add_dev(path.replace('/dev/',''), size, "linear")
                        p.set_devpath(path)
                        p.add_child(devname)
                        d.add_parent(path.replace('/dev/',''))
            elif line.startswith('Unit'):
                unit = int(line.split()[-2])
            elif line.startswith('/dev/'):
                # partition
                line = line.replace('*', '')
                _l = line.split()
                partpath = _l[0]
                partend = int(_l[2])
                partstart = int(_l[1])
                partsize = (partend - partstart) * unit / 1024/1024
                partname = partpath.replace('/dev/','').replace("/","!")
                dev_t = self.get_dev_t(partpath)
                self.dev_h[dev_t] = partname
                p = self.add_dev(partname, partsize, "linear")
                if p is None:
                    continue
                p.set_devpath(partpath)
                d.add_child(partname)
                p.add_parent(devname)

    def load_sysfs(self):
        for devpath in glob.glob("/sys/block/*"):
            devname = os.path.basename(devpath)
            if devname.startswith("Vx"):
                continue
            d = self.load_dev(devname, devpath)

            if d is None:
                continue

            # add parts
            for partpath in glob.glob("%s/%s*"%(devpath, devname)):
                partname = os.path.basename(partpath)
                p = self.load_dev(partname, partpath)
                if p is None:
                    continue
                d.add_child(partname)
                p.add_parent(devname)

    def tune_lv_relations(self):
        dm_h = self.get_dm()
        for lv, segments in self.get_lv_linear().items():
            for devt, length in segments:
                if devt not in self.dev_h:
                    continue
                if lv not in dm_h:
                    continue
                child = dm_h[lv]
                parent = self.dev_h[devt]
                r = self.get_relation(parent, child)
                if r is not None:
                    r.set_used(length)

    def load(self, di=None):
        if di is not None:
            self.di = di
        if self.di is None:
            from utilities.diskinfo import DiskInfo
            self.di = DiskInfo()

        if len(glob.glob("/sys/block/*/slaves")) == 0:
            self.load_fdisk()
            self.load_dm()
        else:
            self.load_sysfs()
            self.tune_lv_relations()

        self.load_vx_dmp()
        self.load_vx_vm()
        self.add_drbd_relations()
        self.add_loop_relations()

    def blacklist(self, devname):
        bl = [r'^ram[0-9]*.*', r'^scd[0-9]*', r'^sr[0-9]*']
        for b in bl:
            if re.match(b, devname):
                return True
        return False

if __name__ == "__main__":
    tree = DevTree()
    tree.load()
    #print(tree)
    tree.print_tree_bottom_up()
    #print(map(lambda x: x.alias, tree.get_top_devs()))
