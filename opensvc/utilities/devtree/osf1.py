import re

from .devtree import DevTree as BaseDevTree
from utilities.diskinfo import DiskInfo
from utilities.proc import justcall


class DevTree(BaseDevTree):
    def get_parts(self, devname, d):
        if d is None:
            return
        data = self.di.h[devname]
        cmd = ['disklabel', '/dev/rdisk/'+devname+'a']
        out, err, ret = justcall(cmd)
        """
# /dev/rdisk/dsk15a:
type: SCSI
disk: HSV210
label:
flags:
bytes/sector: 512
sectors/track: 128
tracks/cylinder: 128
sectors/cylinder: 16384
cylinders: 15360
sectors/unit: 251658240
rpm: 3600
interleave: 1
trackskew: 7
cylinderskew: 26
headswitch: 0		# milliseconds
track-to-track seek: 0	# milliseconds
drivedata: 0

8 partitions:
#            size       offset    fstype  fsize  bsize   cpg  # ~Cyl values
  a:       131072            0    unused      0      0        #      0 - 7
  b:       262144       131072    unused      0      0        #      8 - 23
  c:    251658240            0     AdvFS                      #      0 - 15359
  d:            0            0    unused      0      0        #      0 - 0
  e:            0            0    unused      0      0        #      0 - 0
  f:            0            0    unused      0      0        #      0 - 0
  g:    125632512       393216    unused      0      0        #     24 - 7691
  h:    125632512    126025728    unused      0      0        #   7692 - 15359
"""
        if ret != 0:
            return
        for line in out.split("\n"):
            if line.startswith("bytes/sector"):
                bs = int(line.split()[-1])
            if re.match(r'\W*[a-h]:', line) is None:
                continue
            l = line.split()
            part = l[0].replace(':','')
            size = 1. * int(l[1]) * bs / 1024 / 1024
            size = int(size)

            partname = devname+part
            child_dev = self.add_dev(partname, size, "linear")
            child_dev.set_devpath('/dev/disk/'+partname)
            child_dev.set_devpath('/dev/rdisk/'+partname)
            if child_dev is None:
                continue
            child_dev.add_parent(devname)
            d.add_child(partname)

    def get_disks(self):
        for devname in self.di.h:
            if self.di.h[devname]['size'] in [0, 2, 30, 45]:
                continue
            d = self.add_dev(devname, self.di.h[devname]['size'], "linear")
            d.set_alias(self.di.h[devname]['wwid'])
            d.set_devpath('/dev/rdisk/'+devname)
            d.set_devpath('/dev/disk/'+devname)
            self.get_parts(devname, d)

    def load(self, di=None):
        self.di = DiskInfo()
        self.get_disks()

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
