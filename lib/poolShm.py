from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy
from rcGlobalEnv import rcEnv
from converters import convert_size
from utilities.proc import justcall

class Pool(pool.Pool):
    type = "shm"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk"]

    @lazy
    def path(self):
        return "/dev/shm"

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
        size_opt = "size=%dm" % convert_size(size, _to="m")
        if self.mnt_opt:
            mnt_opt = ",".join((self.mnt_opt, size_opt))
        else:
            mnt_opt = size_opt
        data.append({
            "rtype": "fs",
            "type": "tmpfs",
            "dev": "shmfs",
            "mnt": self.mount_point(name),
            "mnt_opt": mnt_opt,
        })
        return data

    def pool_status(self):
        from converters import convert_size
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
        }
        cmd = ["df", "-P", self.path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = int(l[3])
        data["used"] = int(l[2])
        data["size"] = int(l[1])
        data["head"] = self.path
        return data

