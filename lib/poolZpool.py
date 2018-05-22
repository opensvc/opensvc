from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall

class Pool(pool.Pool):
    @lazy
    def zpool(self):
        return self.node.conf_get(self.section, "name")

    def translate(self, section, size=None, fmt=True, mnt=None):
        data = []
        fs = {
            "type": "zfs",
            "dev": self.zpool+"/" + "{id}_"+self.section_index(section),
            "mkfs_opt": " ".join(self.mkfs_opt),
            "rtype": "fs",
        }
        if mnt:
            fs["mnt"] = mnt
        else:
            fs["mnt"] = self.default_mnt
        if self.mkfs_opt:
            fs["mkfs_opt"] = " ".join(self.mkfs_opt)
        if self.mnt_opt:
            fs["mnt_opt"] = self.mnt_opt
        data.append(fs)
        return data

    def status(self):
        from converters import convert_size
        data = {
            "type": "zpool",
        }
        cmd = ["zpool", "list", "-H", "-o", "size,alloc,free", "-p", self.zpool]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = convert_size(l[2], default_unit="", _to="kb")
        data["used"] = convert_size(l[1], default_unit="", _to="kb")
        data["size"] = convert_size(l[0], default_unit="", _to="kb")
        data["head"] = self.zpool
        return data

