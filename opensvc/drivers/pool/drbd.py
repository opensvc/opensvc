from __future__ import print_function

import os

from env import Env
from utilities.lazy import lazy
from utilities.proc import justcall
from core.pool import BasePool

class Pool(BasePool):
    type = "drbd"
    capabilities = ["rox", "rwx", "roo", "rwo", "snap", "blk", "shared"]

    @lazy
    def vg(self):
        return self.oget("vg")

    @lazy
    def zpool(self):
        return self.oget("zpool")

    @lazy
    def path(self):
        return self.oget("path") or os.path.join(Env.paths.pathvar, "pool", self.name)

    def translate(self, name=None, size=None, fmt=True, shared=False):
        data = []
        if self.vg:
            disk = {
                "rtype": "disk",
                "type": "lv",
                "name": name,
                "vg": self.vg,
                "size": size,
                "standby": True,
            }
            if self.mkblk_opt:
                disk["create_options"] = " ".join(self.mkblk_opt)
            data.append(disk)
            disk = {
                "rtype": "disk",
                "type": "drbd",
                "res": name,
                "disk": "/dev/%s/%s" % (self.vg, name),
                "standby": True,
            }
            data.append(disk)
            dev = "disk#2"
        elif self.zpool:
            disk = {
                "rtype": "disk",
                "type": "zvol",
                "name": "/".join([self.zpool, name]),
                "size": size,
                "standby": True,
            }
            if self.mkblk_opt:
                disk["create_options"] = " ".join(self.mkblk_opt)
            data.append(disk)
            disk = {
                "rtype": "disk",
                "type": "drbd",
                "res": name,
                "disk": "/dev/%s/%s" % (self.zpool, name),
                "standby": True,
            }
            data.append(disk)
            dev = "disk#2"
        else:
            disk = {
                "rtype": "disk",
                "type": "loop",
                "file": os.path.join(self.path, name + ".img"),
                "size": size,
                "standby": True,
            }
            data.append(disk)
            disk = {
                "rtype": "disk",
                "type": "vg",
                "pvs": os.path.join(self.path, name + ".img"),
                "name": name,
                "standby": True,
            }
            data.append(disk)
            disk = {
                "rtype": "disk",
                "type": "lv",
                "name": "lv",
                "vg": name,
                "size": "100%FREE",
                "standby": True,
            }
            if self.mkblk_opt:
                disk["create_options"] = " ".join(self.mkblk_opt)
            data.append(disk)
            disk = {
                "rtype": "disk",
                "type": "drbd",
                "res": name,
                "disk": "/dev/%s/lv" % name,
                "standby": True,
            }
            data.append(disk)
            dev = "disk#4"

        if fmt:
            data += self.add_fs(name, shared, dev=dev)
        return data

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        if self.zpool:
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
        elif self.vg:
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
        else:
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
                return data
            l = out.splitlines()[-1].split()
            data["free"] = int(l[3])
            data["used"] = int(l[2])
            data["size"] = int(l[1])
            return data

