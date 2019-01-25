from __future__ import print_function

import os

import rcExceptions as ex
from rcUtilities import lazy, fmt_svcpath
from converters import convert_size
from svc import Svc

class Pool(object):
    type = None

    def __init__(self, node=None, name=None):
        self.node = node
        self.name = name.strip(os.sep)

    def conf_get(self, kw):
        return self.node.conf_get(self.section, kw)

    @lazy
    def section(self):
        return "pool#"+self.name

    @lazy
    def fs_type(self):
        try:
            return self.conf_get("fs_type")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mkfs_opt(self):
        try:
            return self.conf_get("mkfs_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mnt_opt(self):
        try:
            return self.conf_get("mnt_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mount_point(self):
        return os.path.join(os.sep, "srv", "{id}")

    def configure_volume(self, volume, size=None, fmt=True, access="rwo", nodes=None):
        data = self.translate(name=volume.id, size=size, fmt=fmt)
        defaults = {
            "rtype": "DEFAULT",
            "kind": "vol",
            "pool": self.name,
            "access": access,
        }
        if access in ("rox", "rwx"):
            defaults["topology"] = "flex"
            defaults["flex_min_nodes"] = 0
        if nodes:
            defaults["nodes"] = nodes
        data.append(defaults)
        volume._update(data)
        self.node.install_service_files(volume.svcname, namespace=volume.namespace)

    def status(self):
        pass

    def translate(self, name=None, size=None, fmt=True):
        return []

    def create_disk(self, name, size):
        return {}

    def delete_disk(self, name):
        return {}

    def delete_volume(self, name, namespace=None):
        volume = Svc(svcname=name, namespace=namespace, node=self.node)
        if not volume.exists():
            self.node.log("volume does not exist")
        self.node.log.info("delete volume %s", volume.svcpath)
        volume.action("delete", options={"wait": True, "unprovision": True, "time": "5m"})
        
    def create_volume(self, name, namespace=None, size=None, access="rwo", format=False, nodes=None):
        volume = Svc(svcname=name, namespace=namespace, node=self.node)
        if volume.exists():
            self.node.log.info("volume %s already exists", name)
            return volume
        if nodes is None:
            nodes = ""
        self.node.log.info("create volume %s (pool name: %s, pool type: %s, "
                           "access: %s, size: %s, format: %s, nodes: %s)",
                           volume.svcpath, self.name, self.type, access, size,
                           format, nodes)
        self.configure_volume(volume,
                              fmt=format,
                              size=convert_size(size),
                              access=access,
                              nodes=nodes)
        volume.action("provision", options={"wait": True, "time": "5m"})

