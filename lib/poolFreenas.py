from __future__ import print_function

import os
import sys

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall
from rcGlobalEnv import rcEnv
from rcFreenas import Freenass

class Pool(pool.Pool):
    type = "freenas"
    capabilities = ["roo", "rwo"]

    def get_mappings(self):
        data = []
        tgts = [tgt["iscsi_target_name"] for tgt in self.array.list_iscsi_target()]
        for ndata in self.node.nodes_info.values():
            for mapping in ndata.get("targets", []):
                if not mapping["hba_id"].startswith("iqn"):
                    continue
                if mapping["tgt_id"] not in tgts:
                    continue
                data.append(":".join((mapping["hba_id"], mapping["tgt_id"])))
        return data

    def delete_disk(self, name):
        self.array.del_iscsi_zvol(name=name)

    def create_disk(self, name, size):
        mappings = self.get_mappings()
        lock_id = None
        try:
            lock_id = self.node._daemon_lock("freenas_create_disk", timeout=120, on_error="raise")
            result = self.array.add_iscsi_zvol(name=name, size=size,
                                               volume=self.diskgroup,
                                               mappings=mappings,
                                               insecure_tpc=True,
                                               blocksize=512)
        finally:
            self.node._daemon_unlock("freenas_create_disk", lock_id)
        return result

    def translate(self, name=None, size=None, fmt=True):
        disk = {
            "rtype": "disk",
            "type": "disk",
            "name": name if name else "{id}",
            "scsireserv": True,
            "disk_id": "",
            "shared": True,
            "size": size,
        }
        if not fmt:
            return [disk]
        fs = {
            "rtype": "fs",
            "type": self.fs_type,
            "dev": "{disk#1.exposed_devs[0]}",
            "shared": True,
        }
        fs["mnt"] = self.mount_point
        if self.mkfs_opt:
            fs["mkfs_opt"] = " ".join(self.mkfs_opt)
        if self.mnt_opt:
            fs["mnt_opt"] = self.mnt_opt
        return [disk, fs]

    @lazy
    def array_name(self):
        return self.conf_get("array")

    @lazy
    def diskgroup(self):
        return self.conf_get("diskgroup")

    @lazy
    def array(self):
        o = Freenass()
        array = o.get_freenas(self.array_name)
        if array is None:
            raise ex.excError("array %s not found" % self.array_name)
        array.node = self.node
        return array

    def status(self):
        from converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.diskgroup),
            "capabilities": self.capabilities,
        }
        try:
            dg = [dg for dg in self.array.list_volume() if dg["name"] == self.diskgroup][0]
        except Exception as exc:
            print(exc, file=sys.stderr)
            return data
        data["free"] = convert_size(dg["avail"], _to="KB")
        data["used"] = convert_size(dg["used"], _to="KB")
        data["size"] = convert_size(dg["avail"] + dg["used"], _to="KB")
        return data


