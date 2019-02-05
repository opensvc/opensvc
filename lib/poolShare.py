from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall
from rcGlobalEnv import rcEnv

class Pool(pool.Pool):
    type = "share"
    capabilities = ["rox", "rwx", "roo", "rwo", "shared"]

    @lazy
    def path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, name=None, size=None, fmt=True, shared=False):
        if shared:
            basename = os.path.join(self.path, "{id}")
        else:
            basename = os.path.join(self.path, "{id}.{nodename}")

        fs = {
            "rtype": "fs",
            "type": "directory",
            "path": os.path.join(self.path, "{id}"),
        }
        return [fs]

    def status(self):
        from converters import convert_size
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
            "head": self.path,
        }
        cmd = ["df", "-P", self.path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = int(l[3])
        data["used"] = int(l[2])
        data["size"] = int(l[1])
        return data

