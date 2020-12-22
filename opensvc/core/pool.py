from __future__ import print_function

import os

import core.exceptions as ex
from utilities.naming import factory
from utilities.lazy import lazy
from utilities.converters import convert_size

class BasePool(object):
    type = None

    def __init__(self, name=None, node=None, **kwargs):
        try:
            name = name.strip(os.sep)
        except Exception:
            pass
        self.name = name
        self.node = node
        self.log = node.log

    @lazy
    def volume_env(self):
        return []

    @lazy
    def optional_volume_env(self):
        return []

    @lazy
    def array(self):
        """
        Implemented by child classes
        """
        return

    @lazy
    def section(self):
        return "pool#" + self.name

    def oget(self, key, **kwargs):
        return self.node.oget(self.section, key, **kwargs)

    @lazy
    def fs_type(self):
        return self.oget("fs_type")

    @lazy
    def mkfs_opt(self):
        return self.oget("mkfs_opt")

    @lazy
    def mkblk_opt(self):
        return self.oget("mkblk_opt")

    @lazy
    def mnt_opt(self):
        return self.oget("mnt_opt")

    @lazy
    def status_schedule(self):
        return self.oget("status_schedule")

    def mount_point(self, name):
        return os.path.join(os.sep, "srv", name)

    def default_disk_name(self, volume):
        return "%s.%s.vol.%s" % (
            volume.name,
            volume.namespace if volume.namespace else "root",
            self.node.cluster_name,
        )

    def configure_volume(self, volume, size=None, fmt=True, access="rwo", shared=False, nodes=None, env=None):
        name = self.default_disk_name(volume)
        data = self.translate(name=name, size=size, fmt=fmt, shared=shared)
        defaults = {
            "rtype": "DEFAULT",
            "pool": self.name,
            "size": size,
            "access": access,
        }
        if access in ("rox", "rwx"):
            defaults["topology"] = "flex"
            defaults["flex_min"] = 0
        if nodes:
            defaults["nodes"] = nodes
        if self.status_schedule is not None:
            defaults["status_schedule"] = self.status_schedule
        data.append(defaults)
        if env:
            data.append(env)
        volume._update(data)
        self.disable_sync_internal(volume)
        if volume.volatile:
            return volume
        return factory("vol")(name=volume.name, namespace=volume.namespace, node=self.node, volatile=volume.volatile)

    def pool_status(self):
        pass

    def translate(self, name=None, size=None, fmt=True, shared=False):
        return []

    def create_disk(self, name, size, nodes=None):
        return {}

    def delete_disk(self, name=None, disk_id=None):
        return {}

    def delete_volume(self, name, namespace=None):
        volume = factory("vol")(name=name, namespace=namespace, node=self.node)
        if not volume.exists():
            self.log.info("volume does not exist")
            return
        self.log.info("delete volume %s", volume.path)
        volume.action("delete", options={"wait": True, "unprovision": True, "time": "5m"})
        
    def create_volume(self, name, namespace=None, size=None, access="rwo", fmt=True, nodes=None, shared=False):
        volume = factory("vol")(name=name, namespace=namespace, node=self.node)
        if volume.exists():
            self.log.info("volume %s already exists", name)
            return volume
        if nodes is None:
            nodes = ""
        self.log.info("create volume %s (pool name: %s, pool type: %s, "
                           "access: %s, size: %s, format: %s, nodes: %s, shared: %s)",
                           volume.path, self.name, self.type, access, size,
                           fmt, nodes, shared)
        self.configure_volume(volume,
                              fmt=fmt,
                              size=convert_size(size),
                              access=access,
                              nodes=nodes,
                              shared=shared)
        volume.action("provision", options={"wait": True, "time": "5m"})

    def get_targets(self):
        return []

    def _get_mappings(self, nodes, transport="fc"):
        data = []
        tgts = self.get_targets()
        if self.node.nodes_info is None:
            raise ex.Error("nodes info is not available")
        for nodename, ndata in self.node.nodes_info.items():
            if nodes and nodename not in nodes:
                continue
            for mapping in ndata.get("targets", []):
                if transport == "iscsi" and not mapping["hba_id"].startswith("iqn"):
                    continue
                if mapping["tgt_id"] not in tgts:
                    continue
                data.append(":".join((mapping["hba_id"], mapping["tgt_id"])))
        self.log.info("mappings for nodes %s: %s", ",".join(sorted(list(nodes))), " ".join(data))
        return data

    def disable_sync_internal(self, volume):
        """
        Disable sync#i0 if the volume has no resource contributing files to sync.
        """
        tosync = []
        for res in volume.get_resources():
            tosync += res.files_to_sync()
        if len(tosync):
            return []
        volume._update([{"rid": "sync#i0", "disable": True}])

    def add_fs(self, name, shared=False, dev="disk#1"):
        data = []
        if self.fs_type == "zfs":
            disk = {
                "rtype": "disk",
                "type": "zpool",
                "name": name,
                "vdev": "{%s.exposed_devs[0]}" % dev,
                "shared": shared,
            }
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": "%s/root" % name,
                "mnt": self.mount_point(name),
                "shared": shared,
            }
            if self.mkfs_opt:
                fs["mkfs_opt"] = " ".join(self.mkfs_opt)
            if self.mnt_opt:
                fs["mnt_opt"] = self.mnt_opt
            data += [disk, fs]
        else:
            fs = {
                "rtype": "fs",
                "type": self.fs_type,
                "dev": "{%s.exposed_devs[0]}" % dev,
                "mnt": self.mount_point(name),
                "shared": shared,
            }
            if self.mkfs_opt:
                fs["mkfs_opt"] = " ".join(self.mkfs_opt)
            if self.mnt_opt:
                fs["mnt_opt"] = self.mnt_opt
            data += [fs]
        return data

