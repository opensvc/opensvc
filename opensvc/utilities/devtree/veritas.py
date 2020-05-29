import glob
import os
from subprocess import *

from .devtree import DevTree as BaseDevTree
from core.capabilities import capabilities
from utilities.proc import justcall

class DevTreeVeritas(BaseDevTree):
    vxprint_cache = {}
    vxdisk_cache = {}

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
        if "node.x.vxdmpadm" not in capabilities:
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
            dev = self.devprefix+l[0]
            if name in self.dmp:
                self.dmp[name].append(dev)
            else:
                self.dmp[name] = [dev]
            if name not in mp_h or mp_h[name] == "unknown" or mp_h[name] == name:
                d = self.vxdisk_cache.get("/dev/vx/rdmp/"+name)
                if d is None:
                    wwid = name
                else:
                    wwid = d.get("wwid")
                mp_h[name] = wwid
        return mp_h

    def vx_vid(self, dev):
        self.load_vxdisk_cache()
        if dev in self.vxdisk_cache:
            return self.vxdisk_cache[dev].get("vid", "")
        return ""

    def vx_pid(self, dev):
        self.load_vxdisk_cache()
        if dev in self.vxdisk_cache:
            return self.vxdisk_cache[dev].get("pid", "")
        return ""

    def vx_inq(self, dev):
        self.load_vxdisk_cache()
        if dev in self.vxdisk_cache:
            return self.vxdisk_cache[dev].get("wwid", "unknown")
        return "unknown"

    def load_vxdisk_cache(self):
        if len(self.vxdisk_cache) != 0:
            return
        cmd = ["/usr/sbin/vxdisk", "-p", "list"]
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return "unknown"
        for line in out.split("\n"):
            l = line.split(": ")
            if len(l) != 2:
                continue
            key = l[0].strip()
            if key == "DISK":
                disk = l[1].strip()
                _key = "/dev/vx/rdmp/"+disk
                self.vxdisk_cache[_key] = {"wwid": disk}
            elif key == "SCSI3_VPD_ID":
                # NAA:6000... or 6000...
                self.vxdisk_cache[_key]["wwid"] = l[1].split(":")[-1].strip()
            elif key == "LUN_SIZE":
                self.vxdisk_cache[_key]["size"] = int(l[1].strip())/2048
            elif key == "DMP_SINGLE_PATH":
                self.vxdisk_cache[_key]["devpath"] = l[1].strip()
            elif key == "PID":
                self.vxdisk_cache[_key]["pid"] = l[1].replace("-SUN", "").strip()
            elif key == "VID":
                self.vxdisk_cache[_key]["vid"] = l[1].strip()

    def load_vx_dmp(self):
        self.load_vxdisk_cache()
        if os.path.exists("/dev/rdsk"):
            self.devprefix = "/dev/rdsk/"
        else:
            self.devprefix = "/dev/"
        wwid_h = self.get_mp_dmp()
        for devname in wwid_h:
            rdevpath = "/dev/vx/rdmp/"+devname
            if rdevpath not in self.vxdisk_cache:
                continue
            size = self.vxdisk_cache[rdevpath].get("size", 0)
            d = self.add_dev(devname, size, "multipath")
            if d is None:
                continue
            d.set_devpath("/dev/vx/dmp/"+devname)
            d.set_devpath(rdevpath)
            d.set_alias(wwid_h[devname])

            for path in self.dmp[devname]:
                pathdev = path.replace(self.devprefix, "")
                p = self.add_dev(pathdev, size, "linear")
                p.set_devpath(path)
                p.add_child(devname)
                d.add_parent(pathdev)
                if False and self.devprefix == "/dev/rdsk/" and path.endswith("s2"):
                    _pathdev = pathdev[:-2]
                    p = self.add_dev(_pathdev, size, "linear")
                    p.add_child(devname)
                    d.add_parent(_pathdev)

    def load_vx_vm(self):
        for devpath in glob.glob("/dev/vx/dsk/*/*"):
            devname = devpath.replace("/dev/vx/dsk/", "")
            disks = self.vx_get_lv_disks(devname)
            if len(disks) == 0:
                # discard snaps for now
                continue
            size = self.vx_get_size(devname)
            d = self.add_dev(devname, size, "linear")
            if d is None:
                continue
            d.set_devpath("/dev/vx/dsk/"+devname)
            d.set_devpath("/dev/vx/rdsk/"+devname)

            for disk in disks:
                cdevname = disk["devname"]
                csize = disk["size"]
                p = self.add_dev(cdevname, csize, "linear")
                p.set_devpath(cdevname)
                p.add_child(devname, csize, "linear")
                d.add_parent(cdevname, csize, "linear")


