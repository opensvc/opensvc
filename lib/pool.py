from __future__ import print_function

import os

import rcExceptions as ex
from rcUtilities import lazy

class Pool(object):
    def __init__(self, node=None, name=None):
        self.node = node
        self.name = name.strip(os.sep)

    @lazy
    def section(self):
        return "pool#"+self.name

    @lazy
    def fs_type(self):
        try:
            return self.node.conf_get(self.section, "fs_type")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mkfs_opt(self):
        try:
            return self.node.conf_get(self.section, "mkfs_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mnt_opt(self):
        try:
            return self.node.conf_get(self.section, "mnt_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mount_point(self):
        return os.path.join(os.sep, "srv", "{id}")

    def configure_volume(self, volume, size=None, fmt=True, access="rwo", nodes=None):
        data = self.translate(size=size, fmt=fmt)
        defaults = {
            "rtype": "DEFAULT",
            "kind": "vol",
            "access": access,
        }
        if access in ("rox", "rwx"):
            defaults["topology"] = "flex"
            defaults["flex_min_nodes"] = 0
        if nodes:
            defaults["nodes"] = nodes
        data.append(defaults)
        volume._update(data)

    def status(self):
        pass

