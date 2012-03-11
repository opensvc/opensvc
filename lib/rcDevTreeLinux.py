import rcDevTree
import glob
import os
import re
from subprocess import *
from rcUtilities import which
from rcGlobalEnv import rcEnv

class DevTree(rcDevTree.DevTree):
    dev_h = {}

    def get_size(self, devpath):
        size = 0
        with open(devpath+'/size', 'r') as f:
            try:
                size = int(f.read().strip()) * 512 / 1024 / 1024
            except:
                pass
        return size

    def get_dm(self):
        if hasattr(self, 'dm_h'):
            return self.dm_h
        self.dm_h = {}
        devpaths = glob.glob("/dev/mapper/*")
        devpaths.remove('/dev/mapper/control')
        for devpath in devpaths:
            s = os.stat(devpath)
            minor = os.minor(s.st_rdev)
            self.dm_h[devpath.replace("/dev/mapper/", "")] = "dm-%d"%minor

        # reverse hash
        self._dm_h = {}
        for mapname, devname in self.dm_h.items():
            self._dm_h[devname] = mapname

        return self.dm_h

    def get_wwid(self):
        if hasattr(self, 'wwid_h'):
            return self.wwid_h
        self.wwid_h = {}
        if not which("multipath"):
            return self.wwid_h
        cmd = ['multipath', '-l']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return self.mp_h
        for line in out.split('\n'):
            if 'dm-' not in line:
                continue
            devname = line[line.index('dm-'):].split()[0]
            try:
                wwid = line[line.index('(')+2:line.index(')')]
            except ValueError:
                wwid = line.split()[0]
            self.wwid_h[devname] = wwid
        return self.wwid_h

    def get_mp(self):
        if hasattr(self, 'mp_h'):
            return self.mp_h
        self.mp_h = {}
        cmd = ['dmsetup', 'ls', '--target', 'multipath']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return self.mp_h
        for line in out.split('\n'):
            l = line.split()
            if len(l) == 0:
                continue
            mapname = l[0]
            major = l[1].strip('(,')
            minor = l[2].strip(' )')
            self.mp_h['dm-'+minor] = mapname
        return self.mp_h

    def get_md(self):
        if hasattr(self, 'md_h'):
            return self.md_h
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

    def get_lv_linear(self):
        if hasattr(self, 'lv_linear'):
            return self.lv_linear
        self.lv_linear = {}
        cmd = ['dmsetup', 'table', '--target', 'linear']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return self.lv_linear
        for line in out.split('\n'):
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

    def dev_type(self, devname):
        t = "linear"
        md_h = self.get_md()
        mp_h = self.get_mp()
        if devname in md_h:
            return md_h[devname]
        if devname in mp_h:
            return "multipath"
        return t

    def add_drbd_relations(self):
        if not which("drbdadm") or not os.path.exists('/proc/drbd'):
            return
        cmd = ["drbdadm", "dump-xml"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        from xml.etree import ElementTree as etree
        tree = etree.fromstring(out)
        for res in tree.getiterator('resource'):
            for host in res.findall('host'):
                if host.attrib['name'] != rcEnv.nodename:
                    continue
                edisk = host.find('disk')
                edev = host.find('device')
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
        mp_h = self.get_mp()
        wwid_h = self.get_wwid()
        size = self.get_size(devpath)

        # exclude 0-sized md, Symmetrix gatekeeper and vcmdb
        if size in [0, 2, 30]:
            return

        devtype = self.dev_type(devname)
        d = self.add_dev(devname, size, devtype)

        if d is None:
            return

        if 'cciss' in devname:
            d.set_devpath('/dev/'+devname.replace('!', '/'))
        else:
            d.set_devpath('/dev/'+devname)

        # store devt
        with open("%s/dev"%devpath, 'r') as f:
            devt = f.read().strip()
            self.dev_h[devt] = devname

        # add holders
        holderpaths = glob.glob("%s/holders/*"%devpath)
        holdernames = map(lambda x: os.path.basename(x), holderpaths)
        for holdername, holderpath in zip(holdernames, holderpaths):
            size = self.get_size(holderpath)
            devtype = self.dev_type(holdername)
            d.add_child(holdername, size, devtype)

        # add lv aliases
        self.get_dm()
        if devname in self._dm_h:
            d.set_alias(self._dm_h[devname])
            d.set_devpath('/dev/mapper/'+self._dm_h[devname])
            s = self._dm_h[devname].replace('--', ':').replace('-', '/').replace(':','-')
            d.set_devpath('/dev/'+s)

        # add slaves
        slavepaths = glob.glob("%s/slaves/*"%devpath)
        slavenames = map(lambda x: os.path.basename(x), slavepaths)
        for slavename, slavepath in zip(slavenames, slavepaths):
            size = self.get_size(slavepath)
            devtype = self.dev_type(slavename)
            d.add_parent(slavename, size, devtype)

        if devname in mp_h:
            d.set_alias(wwid_h[devname])

        return d

    def load(self):
        devpaths = glob.glob("/sys/block/*")
        devnames = map(lambda x: os.path.basename(x), devpaths)
        for devname, devpath in zip(devnames, devpaths):
            d = self.load_dev(devname, devpath)

            if d is None:
                continue

            # add parts
            partpaths = glob.glob("%s/%s*"%(devpath, devname))
            partnames = map(lambda x: os.path.basename(x), partpaths)
            for partname, partpath in zip(partnames, partpaths):
                p = self.load_dev(partname, partpath)
                if p is None:
                    continue
                d.add_child(partname)
                p.add_parent(devname)

        # tune relations
        dm_h = self.get_dm()
        for lv, segments in self.get_lv_linear().items():
            for devt, length in segments:
                child = dm_h[lv]
                parent = self.dev_h[devt]
                r = self.get_relation(parent, child)
                if r is None:
                    print "no %s-%s relation found"%(parent, child)
                    continue
                else:
                    r.set_used(length)

        self.add_drbd_relations()

    def blacklist(self, devname):
        bl = [r'^loop[0-9]*.*', r'^ram[0-9]*.*', r'^scd[0-9]*', r'^sr[0-9]*']
        for b in bl:
            if re.match(b, devname):
                return True
        return False

if __name__ == "__main__":
    tree = DevTree()
    tree.load()
    #print tree
    tree.print_tree_bottom_up()
    #print map(lambda x: x.alias, tree.get_top_devs())
