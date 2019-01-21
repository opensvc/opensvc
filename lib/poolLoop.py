from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall

class Pool(pool.Pool):
    type = "loop"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk"]

    @lazy
    def path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, size=None, fmt=True):
        data = [
            {
                "rtype": "disk",
                "type": "loop",
                "file": os.path.join(self.path, "{id}.img"),
                "size": size,
            }
        ]
        if fmt:
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": os.path.join(self.path, "{id}.img"),
            }
            fs["mnt"] = self.mount_point
            if self.mkfs_opt:
                fs["mkfs_opt"] = " ".join(self.mkfs_opt)
            if self.mnt_opt:
                fs["mnt_opt"] = self.mnt_opt
            data.append(fs)
        return data

    def status(self):
        from converters import convert_size
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        data = {
            "name": self.name,
            "type": self.type,
            "capabilities": self.capabilities,
        }
        cmd = ["df", "-P", self.path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = convert_size(l[3], default_unit="K", _to="k")
        data["used"] = convert_size(l[2], default_unit="K", _to="k")
        data["size"] = convert_size(l[1], default_unit="K", _to="k")
        data["head"] = self.path
        return data

