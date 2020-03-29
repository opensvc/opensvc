import math
import os

import utilities.devtree.veritas
import utilities.devices.sunos
from utilities.proc import justcall
from env import Env
from utilities.subsystems.zone import is_zone
from .diskinfo import BaseDiskInfo

class DiskInfo(BaseDiskInfo):
    h = {}
    done = []

    def get_val(self, line):
        l = line.split(":")
        if len(l) != 2:
            return
        return l[-1].strip()

    def get_part_size(self, dev):
        part = dev[-1]
        basedev = dev[:-2] + "s2"
        size = 0
        out = utilities.devices.sunos.prtvtoc(basedev)
        if out is None:
            return size

        bytes_per_sect = 0
        for line in out.split('\n'):
            if not line.startswith('*'):
                continue
            if "bytes/sector" in line:
                bytes_per_sect = int(line.split()[1])

        if bytes_per_sect == 0:
            return 0

        for line in out.split('\n'):
            if line.startswith('*'):
                continue

            l = line.split()
            if len(l) != 6:
                continue

            if l[0] != part:
                continue

            return math.ceil(1.*int(l[4])*bytes_per_sect/1024/1024)

        return 0

    def get_size(self, dev):
        size = 0
        dev = dev.replace("/dev/dsk/", "/dev/rdsk/")
        dev = dev.replace("/dev/vx/dmp/", "/dev/vx/rdmp/")
        out = utilities.devices.sunos.prtvtoc(dev)
        if out is None:
            return size

        """
        *     512 bytes/sector
        *      63 sectors/track
        *     255 tracks/cylinder
        *   16065 sectors/cylinder
        *   19581 cylinders
        *   19579 accessible cylinders
        **  OR:
        *   188743612 accessible sectors
        """
        for line in out.split('\n'):
            if not line.startswith('*'):
                continue
            try:
                if "bytes/sector" in line:
                    n1 = int(line.split()[1])
                if "accessible sectors" in line:
                    s0 = int(line.split()[1])
                    size = math.ceil(1. * s0 * n1 / 1024 / 1024)
                    break
                if "sectors/cylinder" in line:
                    n2 = int(line.split()[1])
                if "cylinders" in line:
                    n3 = int(line.split()[1])
                size = math.ceil(1. * n1 * n2 * n3 / 1024 / 1024)
            except:
                pass

        return size

    def __init__(self, deferred=False):
        self.zone = is_zone()
        self.deferred = deferred
        if deferred:
            return
        self.scan()

    def scan(self):
        if 'scan' in self.done:
            return
        self.done.append('scan')
        cmd = ["/usr/bin/find", "/dev/rdsk", "-name", "c*s2"]
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return
        lines = out.split('\n')
        if len(lines) < 2:
            return
        for e in lines:
            if "/dev/" not in e:
                continue

            dev = e.strip()
            self.scan_dev(dev)

    def scan_dev(self, dev):
        dev = dev.replace("/dev/vx/dmp/", "/dev/vx/rdmp/")
        if "dmp/" in dev:
            tree = utilities.devtree.veritas.DevTreeVeritas()
            wwid = tree.vx_inq(dev)
            vid = tree.vx_vid(dev)
            pid = tree.vx_pid(dev)
            size = 0
        else:
            cmd = ["mpathadm", "show", "lu", dev]
            (out, err, ret) = justcall(cmd)
            if ret != 0:
                return
            if "Error: Logical-unit " + dev + " is not found" in err:
                dsk = dev.replace("/dev/rdsk/", "")
                dsk = dsk.replace("s2", "")
                wwid = Env.nodename + "." + dsk
                vid = "LOCAL"
                pid = ""
                size = 0
            else:
                wwid = ""
                vid = ""
                pid = ""
                size = 0

                for line in out.split('\n'):
                    if line.startswith("\tVendor:"):
                        vid = self.get_val(line)
                    elif line.startswith("\tProduct:"):
                        pid = self.get_val(line)
                    elif line.startswith("\tName:"):
                        wwid = self.get_val(line)

        size = self.get_size(dev)
        self.h[dev] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def get(self, dev, type):
        dev = dev.replace("/dev/vx/dmp/", "/dev/vx/rdmp/")
        if self.deferred or dev not in self.h:
            self.scan_dev(dev)
        dummy = dict(wwid="unknown", vid="unknown", pid="unknown", size=0)
        if dev not in self.h:
            if self.zone:
                return None
            return dummy[type]
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

    def scanscsi(self, hba=None, target=None, lun=None, log=None):
        os.system("cfgadm -al")
