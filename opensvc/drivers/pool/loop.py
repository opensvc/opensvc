from __future__ import print_function

import os

from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "loop"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk"]

    @lazy
    def path(self):
        return self.oget("path")

    def translate(self, name=None, size=None, fmt=True, shared=False):
        data = [
            {
                "rtype": "disk",
                "type": "loop",
                "file": os.path.join(self.path, "%s.img" % name),
                "size": size,
            }
        ]
        if fmt:
            data += self.add_fs(name, shared)
        return data

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        data = {
            "name": self.name,
            "type": self.type,
            "capabilities": self.capabilities,
            "head": self.path,
        }
        if not usage:
            return data
        cmd = ["df", "-P", self.path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            data["err"] = err
            return data
        l = out.splitlines()[-1].split()
        data["free"] = convert_size(l[3], default_unit="K", _to="k")
        data["used"] = convert_size(l[2], default_unit="K", _to="k")
        data["size"] = convert_size(l[1], default_unit="K", _to="k")
        return data

