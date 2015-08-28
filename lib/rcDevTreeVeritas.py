import glob
import os
from subprocess import *
from rcUtilities import which
import rcDevTree
from rcGlobalEnv import rcEnv
di = __import__("rcDiskInfo"+rcEnv.sysname)
_di = di.diskInfo()

class DevTreeVeritas(rcDevTree.DevTree):
    vxprint_cache = {}

    def vx_get_size(self, name):
        _dg, _vt = name.split("/")
        out = self.vxprint(_dg)
        lines = out.split("\n")
        lines.reverse()
        size = 0
        for line in lines:
            l = line.split()
            if len(l) < 5:
                continue
            if l[0] == "v":
                name = l[1]
                if l[2] == _vt or l[1] == _vt:
                    size += int(float(l[5].rstrip("m")))
                continue
        return size

    def vx_get_lv_disks(self, devname):
        """
         dg vg_sanperftest all        all      27000    1426245297.43.parcl1110221a
         dm 28785_281    3pardata0_281 auto    32.00m   34782.68m -
         sd 28785_281-01 lvset_sanperftest_01-01 28785_281 0.00m 16384.00m 0.00m 3pardata0_281 ENA
         sd 28785_281-02 lvset_sanperftest_02-01 28785_281 16384.00m 10240.00m 0.00m 3pardata0_281 ENA
         sd 28785_281-03 lv_sanperftest_01-01 28785_281 26624.00m 1024.00m 0.00m 3pardata0_281 ENA
         pl lv_sanperftest_01-01 lv_sanperftest_01 ENABLED ACTIVE 1024.00m CONCAT - RW
         pl lvset_sanperftest_01-01 lvset_sanperftest_01 ENABLED ACTIVE 16384.00m CONCAT - RW
         pl lvset_sanperftest_02-01 lvset_sanperftest_02 ENABLED ACTIVE 10240.00m CONCAT - RW
         v  lv_sanperftest_01 -       ENABLED  ACTIVE   1024.00m SELECT    -        fsgen
         v  lvset_sanperftest_01 vset_sanperftest ENABLED ACTIVE 16384.00m SELECT - fsgen
         v  lvset_sanperftest_02 vset_sanperftest ENABLED ACTIVE 10240.00m SELECT - fsgen
         vt vset_sanperftest -        ENABLED  ACTIVE   2           
        """
        _dg, _vt = devname.split("/")
        out = self.vxprint(_dg)
        sd = {}
        v = []
        pl = []
        lines = out.split("\n")
        lines.reverse()
        for line in lines:
            l = line.split()
            if len(l) < 5:
                continue
            if l[0] == "v":
                name = l[1]
                if l[2] == _vt or l[1] == _vt:
                    v.append(name)
                continue
            if l[0] == "pl":
                name = l[1]
                if l[2] in v:
                    pl.append(name)
                continue
            if l[0] == "sd":
                name = l[1]
                if l[2] in pl:
                    dm = l[3]
                    size = int(float(l[5].rstrip("m")))
                    if dm not in sd:
                        sd[dm] = {
                          "devname": dm,
                          "size": size
                        }
                    else:
                        sd[dm]["size"] += size
            if l[0] == "dm":
               dmname = l[1]
               dname = l[2]
               for dm in sd:
                   if sd[dm]["devname"] == dmname:
                       sd[dm]["devname"] = dname
        return sd.values()

    def vxprint(self, dg):
        if dg in self.vxprint_cache:
            return self.vxprint_cache[dg]
        cmd = ["vxprint", "-t", "-u", "m", "-g", dg]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        self.vxprint_cache[dg] = out
        return out

    def get_mp_dmp(self):
        self.dmp = {}
        if not which("vxdmpadm"):
            return {}
        cmd = ['vxdmpadm', 'getsubpaths']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return {}
        lines = out.split('\n')
        if len(lines) < 3:
            return {}
        lines = lines[2:]
        mp_h = {}
        for line in lines:
            l = line.split()
            if len(l) < 4:
                continue
            name = l[3]
            dev = "/dev/"+l[0]
            if name in self.dmp:
                self.dmp[name].append(dev)
            else:
                self.dmp[name] = [dev]
                mp_h[name] = _di.disk_id(dev)
        return mp_h

    def load_vx_dmp(self):
        wwid_h = self.get_mp_dmp()
        for devname in wwid_h:
            size = _di.disk_size("/dev/vx/dmp/"+devname)
            d = self.add_dev(devname, size, "multipath")
            if d is None:
                continue
            d.set_devpath("/dev/vx/dmp/"+devname)
            d.set_devpath("/dev/vx/rdmp/"+devname)
            d.set_alias(wwid_h[devname])

            for path in self.dmp[devname]:
                p = self.add_dev(path.replace('/dev/',''), size, "linear")
                p.set_devpath(path)
                p.add_child(devname)
                d.add_parent(path.replace('/dev/',''))

    def load_vx_vm(self):
        for devpath in glob.glob("/dev/vx/dsk/*/*"):
            devname = devpath.replace("/dev/vx/dsk/", "")
            size = self.vx_get_size(devname)
            d = self.add_dev(devname, size, "linear")
            if d is None:
                continue
            d.set_devpath("/dev/vx/dsk/"+devname)
            d.set_devpath("/dev/vx/rdsk/"+devname)

            for disk in self.vx_get_lv_disks(devname):
                cdevname = disk["devname"]
                csize = disk["size"]
                p = self.add_dev(cdevname, csize, "linear")
                p.set_devpath(cdevname)
                p.add_child(devname, csize, "linear")
                d.add_parent(cdevname, csize, "linear")


