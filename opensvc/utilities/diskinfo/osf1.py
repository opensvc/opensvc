from __future__ import print_function

import re

from .diskinfo import BaseDiskInfo
from utilities.proc import justcall


class DiskInfo(BaseDiskInfo):

    def __init__(self):
        self.load_cache()

    def is_id(self, line):
        if re.match(r"^\W*[0-9]*:", line) is None:
            return False
        return True

    def cache_add(self, id, dev, wwid, path_count):
        d = self.devattr(id)
        vid = d['manufacturer']
        pid = d['model']
        size = d['mb']
        self.h[dev] = dict(
          wwid=wwid,
          vid=vid,
          pid=pid,
          size=size,
          id=id,
          path_count=path_count
        )

    def load_cache(self):
        self.h = {}
        cmd = ["hwmgr", "show", "scsi", "-type", "disk", "-active", "-full"]
        out, err, ret = justcall(cmd)
        path_count = -1
        id = None
        dev = None
        wwid = None
        for e in out.split('\n'):
            if len(e) == 0:
                continue
            if self.is_id(e):
                if path_count >= 0:
                    self.cache_add(id, dev, wwid, path_count)
                l = e.split()
                if len(l) < 8:
                    continue
                id = l[0].strip(':')
                dev = l[7]
                path_count = 0
            elif 'WWID' in e:
                wwid = e.split(":")[-1].replace('-','').lower()
                wwid = wwid.strip('"').replace(" ", "_")
            elif re.match(r'\W*[0-9]*\W+', e) is not None and 'valid' in e:
                path_count += 1
        if path_count >= 0:
            self.cache_add(id, dev, wwid, path_count)

    def devattr(self, id):
        d = {'capacity': 0, 'block_size': 0, 'manufacturer': '', 'model': '', 'mb': 0}
        cmd = ["hwmgr", "get", "att", "-id", id,
               "-a", "model",
               "-a", "manufacturer",
               "-a", "capacity",
               "-a", "block_size"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return d
        for line in out.split("\n"):
            if not line.startswith(' '):
                continue
            l = line.split('=')
            if len(l) !=2:
                continue
            d[l[0].strip()] = l[1].strip()
        d['mb'] = int(d['capacity']) * int(d['block_size']) // 1024 // 1024
        return d

    def get(self, dev, type):
        dev = dev.replace('/dev/rdisk/','')
        dev = dev.replace('/dev/disk/','')
        if dev not in self.h:
            return
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

if __name__ == "__main__":
    di = DiskInfo()
    print(di.h.items())
