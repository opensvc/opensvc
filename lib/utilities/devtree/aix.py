import re
from subprocess import *

from .devtree import DevTree as BaseDevTree
import utilities.subsystems.lvm.aix
from utilities.proc import which

class DevTree(BaseDevTree):
    def load_lvm(self):
        lvm = utilities.subsystems.lvm.aix.Lvm()
        for vg in lvm.vg.values():
            for lv in vg.lv.values():
                d = self.add_dev(lv.name, self.disk_size(lv.name), "linear")
                d.set_devpath('/dev/'+lv.name)
                for parentname in lv.pv_size:
                    size = lv.pv_size[parentname]
                    d.add_parent(parentname, size, "linear")
                    parent = self.get_dev(parentname)
                    parent.add_child(d.devname, size, "linear")

    def load_lsdev(self):
        if not which("lsdev"):
            return
        cmd = ["lsdev", "-Cc", "disk"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode:
            return
        """
        hdisk0 Available  Virtual SCSI Disk Drive
        hdisk1 Available  Virtual SCSI Disk Drive
        """
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            l = line.split()
            devname = l[0]
            d = self.add_dev(devname, self.disk_size(devname), "linear")
            d.set_devpath('/dev/'+devname)

    def disk_size(self, devname):
        cmd = ["bootinfo", "-s", devname]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode:
            return 0
        return int(out.strip())

    def load(self, di=None):
        self.load_lsdev()
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
