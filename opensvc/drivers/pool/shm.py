from __future__ import print_function

import os

import core.exceptions as ex

from utilities.lazy import lazy
from utilities.converters import convert_size
from utilities.proc import justcall
from core.pool import BasePool
from env import Env

class Pool(BasePool):
    type = "shm"
    capabilities = ["rox", "rwx", "roo", "rwo", "blk"]

    @lazy
    def path(self):
        if Env.sysname == "FreeBSD":
            path = os.path.join(Env.paths.pathvar, "pool", "shm")
            self.freebsd_mount(path)
            return path
        return "/dev/shm"

    def freebsd_mount(self, path):
        from utilities.mounts.freebsd import Mounts
        m = Mounts()
        if m.has_mount("tmpfs", path):
            return
        out, err, ret = justcall(["mount", "-t", "tmpfs", "none", path])
        if ret:
            raise ex.Error("can not mount the 'shm' pool tmpfs in %s: %s" % (path, err))

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

