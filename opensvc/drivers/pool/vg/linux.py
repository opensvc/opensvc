from __future__ import print_function

from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "vg"
    capabilities = ["rox", "rwx", "roo", "rwo", "snap", "blk"]

    @lazy
    def vg(self):
        return self.oget("name")

    def translate(self, name=None, size=None, fmt=True, shared=False):
        data = []
        disk = {
            "rtype": "disk",
            "type": "lv",
            "name": name,
            "vg": self.vg,
            "size": size,
        }
        if self.mkblk_opt:
            disk["create_options"] = " ".join(self.mkblk_opt)
        data.append(disk)
        if fmt:
            data += self.add_fs(name, shared)
        return data

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "capabilities": self.capabilities,
            "head": self.vg,
        }
        if not usage:
            return data
        cmd = ["vgs", "-o", "Size,Free", "--units", "k", "--noheadings", self.vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = int(l[1].split(".")[0])
        data["size"] = int(l[0].split(".")[0])
        data["used"] = data["size"] - data["free"]
        return data

