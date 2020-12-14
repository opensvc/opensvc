from __future__ import print_function

import os

from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "directory"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk"]

    @lazy
    def path(self):
        return self.oget("path")

    def translate_blk(self, name=None, size=None, shared=False):
        data = [
            {
                "rtype": "disk",
                "type": "loop",
                "file": os.path.join(self.path, "%s.img" % name),
                "size": size,
            }
        ]
        return data

    def translate(self, name=None, size=None, fmt=True, shared=False):
        if not fmt:
            return self.translate_blk(name=name, size=size, shared=shared)
        data = []
        path = os.path.join(self.path, name)
        data.append({
            "rtype": "fs",
            "type": "flag",
        })
        data.append({
            "rtype": "fs",
            "type": "directory",
            "path": path,
        })
        return data

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

