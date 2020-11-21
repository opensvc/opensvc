from __future__ import print_function

import os

from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "share"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk", "shared"]

    @lazy
    def path(self):
        return self.oget("path")

    def translate_blk(self, path=None, size=None, shared=False):
        data = [
            {
                "rtype": "disk",
                "type": "loop",
                "file": "%s.img" % path,
                "size": size,
            }
        ]
        return data

    def translate(self, name=None, size=None, fmt=True, shared=False):
        if shared:
            path = os.path.join(self.path, name)
        else:
            path = os.path.join(self.path, "%s.{nodename}" % name)
        if not fmt:
            return self.translate_blk(path, size=size, shared=shared)

        fs = {
            "rtype": "fs",
            "type": "directory",
            "path": path,
        }
        return [fs]

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
            "head": self.path,
        }
        if not usage:
            return data
        cmd = ["df", "-P", self.path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            data["error"] = err
            return data
        l = out.splitlines()[-1].split()
        data["free"] = int(l[3])
        data["used"] = int(l[2])
        data["size"] = int(l[1])
        return data

