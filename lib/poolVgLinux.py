from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall

class Pool(pool.Pool):
    @lazy
    def vg(self):
        return self.node.conf_get(self.section, "name")

    @lazy
    def create_opt(self):
        try:
            return self.node.conf_get(self.section, "create_opt")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, section, size=None, fmt=True, mnt=None):
        data = []
        if fmt:
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": os.path.join(os.sep, "dev", self.vg, "{id}_"+self.section_index(section)),
                "vg": self.vg,
                "size": size,
            }
            if mnt:
                fs["mnt"] = mnt
            else:
                fs["mnt"] = self.default_mnt
            if self.mkfs_opt:
                fs["mkfs_opt"] = " ".join(self.mkfs_opt)
            if self.mnt_opt:
                fs["mnt_opt"] = self.mnt_opt
            if self.create_opt:
                fs["create_opt"] = " ".join(self.create_opt)
            data.append(fs)
        else:
            disk = {
                "rtype": "disk",
                "type": "lv",
                "name": "{id}_"+self.section_index(section),
                "vg": self.vg,
                "size": size,
            }
            if self.create_opt:
                disk["create_opt"] = " ".join(self.create_opt)
            data.append(disk)
        return data

    def status(self):
        from converters import convert_size
        data = {
            "type": "vg",
        }
        cmd = ["vgs", "-o", "Size,Free", "--units", "k", "--noheadings", self.vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = int(l[1].split(".")[0])
        data["size"] = int(l[0].split(".")[0])
        data["used"] = data["size"] - data["free"]
        data["head"] = self.vg
        return data

