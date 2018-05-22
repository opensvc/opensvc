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
        fs = {
            "rtype": "fs",
            "type": "directory",
            "path": os.path.join(self.path, "{id}", "{rindex}"),
        }
        if mnt:
            fs["mnt"] = mnt
        else:
            fs["mnt"] = self.default_mnt
        return [fs]

    def status(self):
        from converters import convert_size
        if not os.path.exists(self.evaluated_path):
            os.makedirs(self.evaluated_path)
        data = {
            "type": "directory",
        }
        cmd = ["df", "-P", self.evaluated_path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = l[3]
        data["used"] = l[2]
        data["size"] = l[1]
        data["head"] = self.evaluated_path
        return data

