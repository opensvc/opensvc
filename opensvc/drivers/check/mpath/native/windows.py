import os
import tempfile
from subprocess import *

import drivers.check
import foreign.wmi as wmi

class Check(drivers.check.Check):
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.sub_devs()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.path
        return ''

    def diskpart_rescan(self):
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        with open(tmpf, 'w') as f:
            f.write("rescan\n")
        p = Popen(["diskpart", "/s", tmpf], stdout=PIPE, stderr=PIPE, stdin=None, shell=True)
        out, err = p.communicate()
        os.unlink(tmpf)

    def do_check(self):
        self.wmi = wmi.WMI(namespace="root/wmi")
        self.diskpart_rescan()
        r = []
        try:
            l = self.wmi.MPIO_DISK_INFO()
        except:
            l = []
        for disk in l:
            if disk.driveinfo is None:
                continue
            for drive in disk.driveinfo:
                name = drive.name
                n = drive.numberpaths
                r.append({"instance": name,
                          "value": str(n),
                          "path": "",
                         })
        return r
