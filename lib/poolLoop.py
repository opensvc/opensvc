from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall

class Pool(pool.Pool):
    @lazy
    def path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def evaluated_path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, size=None, fmt=True, mnt=None):
        data = [
            {
                "rtype": "disk",
                "type": "loop",
                "file": os.path.join(self.path, "{id}", "{rindex}.img"),
                "size": size,
            }
        ]
        if fmt:
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": os.path.join(self.path, "{id}", "{rindex}.img"),
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
        if not os.path.exists(self.evaluated_path):
            os.makedirs(self.evaluated_path)
        data = {
            "type": "loop",
        }
        cmd = ["df", "-P", self.evaluated_path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = convert_size(l[3], default_unit="K", _to="k")
        data["used"] = convert_size(l[2], default_unit="K", _to="k")
        data["size"] = convert_size(l[1], default_unit="K", _to="k")
        data["head"] = self.evaluated_path
        return data

