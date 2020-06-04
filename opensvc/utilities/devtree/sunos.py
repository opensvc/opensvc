import glob
import os
import re

from subprocess import PIPE

from .devtree import DevTree as BaseDevTree
from .veritas import DevTreeVeritas
from utilities.devices.sunos import prtvtoc
from env import Env
from utilities.cache import cache
from utilities.proc import justcall

class DevTree(DevTreeVeritas, BaseDevTree):
    di = None
    zpool_members = {}
    zpool_used = {}
    zpool_used_zfs = {}
    zpool_size = {}
    zpool_datasets = {}
    zpool_datasets_used = {}

    def load_partitions(self, d):
        """
        *                          First     Sector    Last
        * Partition  Tag  Flags    Sector     Count    Sector  Mount Directory
               0      2    00   16779312  54281421  71060732
               1      3    01          0  16779312  16779311
               2      5    00          0  71127180  71127179
               7      0    00   71060733     66447  71127179
        """
        out = prtvtoc(d.devpath[0])
        if out is None:
            return
        for line in out.splitlines():
            line = line.strip()
            if line.startswith('*'):
                continue
            if line.startswith('2'):
                continue
            l = line.split()
            if len(l) < 6:
                continue
            partname = d.devname + 's' + l[0]
            partpath = d.devpath[0][:-1] + l[0]
            partsize = self.di.get_part_size(partpath)
            p = self.add_dev(partname, partsize, "linear")
            p.set_devpath(partpath)
            self.add_device_devpath(p, partpath)
            p.set_devpath(partpath.replace("/dev/rdsk/", "/dev/dsk/"))
            self.add_device_devpath(p, partpath)
            d.add_child(partname)
            p.add_parent(d.devname)

    def add_device_devpath(self, dev, path):
        if os.path.islink(path):
            altpath = os.path.realpath(path)
            if altpath != path:
                dev.set_devpath(altpath)

    def load_disks(self):
        self.load_vxdisk_cache()
        if len(self.vxdisk_cache) > 0:
            self.load_vxdisk()
        else:
            #self.load_format()
            self.load_dsk()

    def load_vxdisk(self):
        for devpath, data in self.vxdisk_cache.items():
            if "size" not in data or "devpath" not in data:
                continue
            devname = os.path.basename(devpath)
            bdevpath = devpath.replace("/rdsk/", "/dsk/").replace("/rdmp/", "/dmp/")
            size = data["size"]
            d = self.add_dev(devname, size, "linear")
            d.set_devpath(data["devpath"])
            d.set_devpath(devpath)
            d.set_devpath(bdevpath)
            self.add_device_devpath(d, devpath)
            self.add_device_devpath(d, bdevpath)
            self.load_partitions(d)

    def load_dsk(self):
        for devpath in glob.glob("/dev/rdsk/*s2"):
            bdevpath = devpath.replace("/rdsk/", "/dsk/")
            devname = devpath.replace("/dev/rdsk/", "")[:-2]
            size = self.di.get_size(devpath)
            d = self.add_dev(devname, size, "linear")
            d.set_devpath(devpath)
            d.set_devpath(bdevpath)
            self.add_device_devpath(d, devpath)
            self.add_device_devpath(d, bdevpath)
            self.load_partitions(d)

    def load_format(self):
        """
        0. c3t0d0 <SUN36G cyl 24620 alt 2 hd 27 sec 107>
          /pci@1f,700000/scsi@2/sd@0,0
        4. c5t600508B4000971CD00010000024A0000d0 <HP-HSV210-6200 cyl 61438 alt 2 hd 128 sec 128>  EVA_SAVE
          /scsi_vhci/ssd@g600508b4000971cd00010000024a0000
        """
        out, err, ret = justcall(["format", "-e"], stdin=PIPE)
        for line in out.splitlines():
            line = line.strip()
            if re.match(r"[0-9]+\. ", line) is None:
                continue
            l = line.split()
            devname = l[1]
            devpath = '/dev/rdsk/'+devname+'s2'
            bdevpath = devpath.replace("/rdsk/", "/dsk/")
            size = self.di.get_size(devpath)
            d = self.add_dev(devname, size, "linear")
            d.set_devpath(devpath)
            d.set_devpath(bdevpath)
            self.add_device_devpath(d, devpath)
            self.add_device_devpath(d, bdevpath)
            self.load_partitions(d)

    def load_sds(self):
        if not os.path.exists("/usr/sbin/metastat"):
            return
        out, err, ret = justcall(["metastat", "-p"])
        if ret != 0:
            return
        lines = out.split('\n')
        lines.reverse()

        """
        # metastat -p
        d11 -m d2 d3 1
        d2 1 1 c3t0d0s1
        d3 1 1 c3t1d0s1
        """
        for line in lines:
            l = line.split()
            if len(l) < 3:
                continue
            childname = l[0]
            childpath = "/dev/md/dsk/"+childname
            childsize = self.di.get_size(childpath)
            if l[1] == "-m":
                childtype = "raid1"
            else:
                childtype = "linear"
            childdev = self.add_dev(childname, childsize, childtype)
            childdev.set_devpath(childpath)
            if l[1] == "-m":
                parentnames = l[2:-1]
            else:
                parentnames = [l[-1]]

            for parentname in parentnames:
                parentpath = "/dev/md/dsk/"+parentname
                parentsize = self.di.get_size(parentpath)
                parentdev = self.add_dev(parentname, parentsize, "linear")
                childdev.add_parent(parentname)
                parentdev.add_child(childname)

    def load_zpool(self):
        out = self.zpool_list()
        if out is None:
            return
        for line in out.splitlines():
            l = line.split()
            if len(l) == 0:
                continue
            poolname = l[0]
            self.load_zpool1(poolname)

    @cache("zpool.list")
    def zpool_list(self):
        out, err, ret = justcall(["zpool", "list", "-H"])
        if ret != 0:
            return
        return out

    @cache("zfs.list.snapshots")
    def zfs_list_snapshots(self):
        out, err, ret = justcall(["zfs", "list", "-H", "-t", "snapshot"])
        if ret != 0:
            return
        return out

    def load_zpool1(self, poolname):
        out, err, ret = justcall(["zpool", "status", poolname])
        if ret != 0:
            return
        self.zpool_members[poolname] = []
        for line in out.splitlines():
            l = line.split()
            if len(l) != 5:
                continue
            if l[0] == 'NAME':
                continue
            if l[0] == poolname:
                continue
            devname = l[0]

            # -d mode import ?
            if devname.startswith(Env.paths.pathvar):
                devname = devname.split('/')[-1]
            d = self.get_dev(devname)
            if d is None:
                continue
            self.zpool_members[poolname].append(d)

        out, err, ret = justcall(["zpool", "iostat", poolname])
        if ret != 0:
            return
        lines = out.split('\n')
        lines = [l for l in lines if len(l) > 0]
        zpool_iostats = lines[-1].split()
        if zpool_iostats[0] != poolname:
            # may be a FAULTY zpool, so no stats
            return
        self.zpool_used[poolname] = self.read_size(zpool_iostats[1])
        zpool_free = self.read_size(zpool_iostats[2])
        self.zpool_size[poolname] = self.zpool_used[poolname] + zpool_free

        out, err, ret = justcall(["zfs", "list", "-H", "-r", "-t", "filesystem", poolname])
        if ret != 0:
            return
        self.zpool_datasets[poolname] = []
        self.zpool_datasets_used[poolname] = 0
        for line in out.splitlines():
            l = line.split()
            if len(l) == 0:
                continue
            zfsname = l[0]
            size = self.read_size(l[1])
            refer = self.read_size(l[3])
            size -= refer
            mnt = l[4]
            if zfsname == poolname:
                self.zpool_used_zfs[poolname] = size
                continue
            self.zpool_datasets[poolname].append((zfsname, size))
            self.zpool_datasets_used[poolname] += size

        out = self.zfs_list_snapshots()
        if out is None:
            return

        for line in out.splitlines():
            l = line.split()
            if len(l) == 0:
                continue
            zfsname = l[0]
            if not zfsname.startswith(poolname+'/') and \
               not zfsname.startswith(poolname+'@'):
                continue
            size = self.read_size(l[1])
            #refer = self.read_size(l[3])
            self.zpool_datasets[poolname].append((zfsname, size))
            self.zpool_datasets_used[poolname] += size

        rest = self.zpool_used_zfs[poolname] - self.zpool_datasets_used[poolname]
        if rest < 0:
            rest = 0
        self.zpool_datasets[poolname].append((poolname, rest))
        self.zpool_datasets_used[poolname] += rest

        if self.zpool_datasets_used[poolname] == 0:
            ratio = 0
        else:
            ratio = 1.0 * self.zpool_used[poolname] / self.zpool_datasets_used[poolname]

        for zfsname, size in self.zpool_datasets[poolname]:
            used = int(size*ratio)
            d = self.add_dev(zfsname, used, "linear")
            d.set_devpath(zfsname)
            for m in self.zpool_members[poolname]:
                member_ratio = 1.0 * m.size / self.zpool_size[poolname]
                d.add_parent(m.devname)
                m.add_child(zfsname)
                m.dg = poolname
                self.set_relation_used(m.devname, zfsname, int(used*member_ratio))

    def read_size(self, s):
        if s == '0':
            return 0
        unit = s[-1]
        size = float(s[:-1].replace(',','.'))
        if unit == 'K':
            size = size / 1024
        elif unit == 'M':
            pass
        elif unit == 'G':
            size = size * 1024
        elif unit == 'T':
            size = size * 1024 * 1024
        elif unit == 'P':
            size = size * 1024 * 1024 * 1024
        elif unit == 'Z':
            size = size * 1024 * 1024 * 1024 * 1024
        else:
            raise Exception("unit not supported: %s"%unit)
        return int(size)

    def load(self, di=None):
        if di is not None:
            self.di = di
        if self.di is None:
            from utilities.diskinfo import DiskInfo
            self.di = DiskInfo(deferred=True)
        self.load_disks()
        self.load_zpool()
        self.load_sds()
        self.load_vx_dmp()
        self.load_vx_vm()

    def blacklist(self, devname):
        bl = [r'^loop[0-9]*.*', r'^ram[0-9]*.*', r'^scd[0-9]*', r'^sr[0-9]*']
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
