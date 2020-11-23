from __future__ import print_function

from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "zpool"
    capabilities = ["rox", "rwx", "roo", "rwo", "snap", "blk"]

    @lazy
    def zpool(self):
        return self.oget("name")

    def translate(self, name=None, size=None, fmt=True, shared=False):
        data = []
        if fmt:
            fs = {
                "rtype": "fs",
                "type": "zfs",
                "dev": self.zpool + "/" + name,
                "mnt": self.mount_point(name),
            }
            if self.mkfs_opt:
                fs["mkfs_opt"] = " ".join(self.mkfs_opt)
            if self.mnt_opt:
                fs["mnt_opt"] = self.mnt_opt
            data.append(fs)
        else:
            zvol = {
                "rtype": "disk",
                "type": "zvol",
                "dev": self.zpool + "/" + name,
            }
            if self.mkblk_opt:
                zvol["create_options"] = " ".join(self.mkblk_opt)
            data.append(zvol)
        return data

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
            "head": self.zpool,
        }
        if not usage:
            return data
        cmd = ["zpool", "get", "-H", "size,alloc,free", "-p", self.zpool]
        out, err, ret = justcall(cmd)
        if ret != 0:
            data["error"] = err
            return data
        lines = out.splitlines()
        data["size"] = convert_size(lines[0].split()[2], default_unit="", _to="kb")
        data["used"] = convert_size(lines[1].split()[2], default_unit="", _to="kb")
        data["free"] = convert_size(lines[2].split()[2], default_unit="", _to="kb")
        return data

