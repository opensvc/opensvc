import re
from subprocess import *

from .devtree import DevTree as BaseDevTree
from utilities.proc import which
from utilities.diskinfo import DiskInfo

di = DiskInfo()

class DevTree(BaseDevTree):
    pe_size = {}

    def add_part(self, parent_devpath, child_devpath):
        child_dev = self.add_disk(child_devpath)
        if child_dev is None:
            return
        parent_dev = self.get_dev_by_devpath(parent_devpath)
        if parent_dev is None:
            return
        child_dev.add_parent(parent_dev.devname)
        parent_dev.add_child(child_dev.devname)

    def add_disk(self, devpath):
        devname = devpath.split('/')[-1]
        if devpath in self.lunmap:
            devtype = "multipath"
        else:
            devtype = "linear"
        size = di.disk_size(devpath)

        # exclude 0-sized md, Symmetrix gatekeeper and vcmdb
        if size in [0, 2, 30, 45]:
            return

        d = self.add_dev(devname, size, devtype)
        d.set_devpath(devpath)
        d.set_devpath(devpath.replace('/disk/', '/rdisk/').replace('/dsk/', '/rdsk/'))
        if devpath in self.lunmap:
            d.set_alias(self.lunmap[devpath]['wwid'])
        else:
            wwid = di.disk_id(devpath)
            if wwid != "":
                d.set_alias(wwid)
        return d

    def load_ioscan(self):
        if not which("/usr/sbin/ioscan"):
            return
        cmd = ["/usr/sbin/ioscan", "-FunNC", "disk"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode:
            if "illegal option -- N" not in err:
                return
            cmd = ["/usr/sbin/ioscan", "-FunC", "disk"]
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
            if p.returncode:
                return
        """
        scsi:wsio:T:T:F:31:188:0:disk:sdisk:0/2/1/0.0.0.0.0:0 0 5 18 0 0 0 0 195 124 63 185 173 253 214 203 :0:root.sba.lba.sasd.sasd_vbus.tgt.sdisk:sdisk:CLAIMED:DEVICE:HP      DG146BB976:0:
                      /dev/disk/disk11      /dev/disk/disk11_p2   /dev/rdisk/disk11     /dev/rdisk/disk11_p2
                      /dev/disk/disk11_p1   /dev/disk/disk11_p3   /dev/rdisk/disk11_p1  /dev/rdisk/disk11_p3
        """
        for w in out.split():
            if not w.startswith('/dev/'):
                new = True
                continue
            if new:
                disk = w
                new = False
                d = self.add_disk(disk)
                continue
            if d is None or '/rdisk/' in w:
                continue
            elif '/pt/' in w:
                d.set_devpath(w)
            elif '_p' in w:
                self.add_part(disk, w)
            else:
                # arbitrary dsf alias
                d.set_devpath(w)

    def get_lunmap(self):
        if hasattr(self, "lunmap"):
            return
        self.lunmap = {}
        if not which("scsimgr"):
            return
        cmd = ["scsimgr", "lun_map"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode:
            return
        """

        LUN PATH INFORMATION FOR LUN : /dev/rdisk/disk10

Total number of LUN paths     = 1
World Wide Identifier(WWID)    = 0x5000c5000aba6793

LUN path : lunpath0
Class                         = lunpath
Instance                      = 0
Hardware path                 = 0/2/1/0.0x5000c5000aba6791.0x0
SCSI transport protocol       = sas
State                         = UNOPEN
Last Open or Close state      = ACTIVE
"""
        for line in out.split('\n'):
            if "INFORMATION" in line:
                disk = line.split()[-1]
                self.lunmap[disk] = {}
            if "WWID" in line:
                wwid = line.split()[-1].replace('0x', '')
                if wwid != "=":
                    self.lunmap[disk]['wwid'] = line.split()[-1].replace('0x', '')
                else:
                    del(self.lunmap[disk])

    def load_lv(self, lv):
        if not which("lvdisplay"):
            return
        cmd = ["lvdisplay", "-v", lv]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode:
            return

        vgname = lv.split('/')[2]

        # parser
        h = {}
        for line in out.split('\n'):
            line = line.strip()
            if 'LV Size' in line:
                size = int(line.split()[-1])
            if not line.startswith('/dev'):
                continue
            pv, le, pe = line.split()
            h[pv] = int(pe) * self.pe_size[vgname]

        # use the linux lvm naming convention
        devname = lv.replace('/dev/','').replace('-','--').replace('/','-')
        d = self.add_dev(devname, size, "linear")

        for pv in h:
            d.add_parent(pv.replace('/dev/disk/', ''), size=h[pv])
            d.set_devpath(lv)
            parent_dev = self.get_dev_by_devpath(pv)
            if parent_dev is None:
                continue
            parent_dev.add_child(d.devname)

    def load_lvm(self):
        if not which("vgdisplay"):
            return
        cmd = ["vgdisplay", "-v"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        for line in out.split('\n'):
            if 'VG Name' in line:
                vgname = line.split()[-1].replace('/dev/','')
            if 'PE Size' in line:
                self.pe_size[vgname] = int(line.split()[-1])
            if 'LV Name' not in line:
                continue
            self.load_lv(line.split()[-1])

    def load(self, di=None):
        self.get_lunmap()
        self.load_ioscan()
        self.load_lvm()

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
