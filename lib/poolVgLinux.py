from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall

class Pool(pool.Pool):
    type = "vg"
    capabilities = ["rox", "rwx", "roo", "rwo", "snap", "blk"]

    @lazy
    def vg(self):
        return self.node.conf_get(self.section, "name")

    @lazy
    def create_opt(self):
        try:
            return self.node.conf_get(self.section, "create_opt")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, size=None, fmt=True):
        data = []
        if fmt:
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": os.path.join(os.sep, "dev", self.vg, "{id}"),
                "vg": self.vg,
                "size": size,
            }
            fs["mnt"] = self.mount_point
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
                "name": "{id}",
                "vg": self.vg,
                "size": size,
                "standby": standby,
            }
            if self.create_opt:
                disk["create_opt"] = " ".join(self.create_opt)
            data.append(disk)
        return data

    def status(self):
        from converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
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

