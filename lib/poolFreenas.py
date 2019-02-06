from __future__ import print_function

import os
import sys

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall
from rcGlobalEnv import rcEnv
from rcFreenas import Freenass

LOCK_NAME = "freenas_create_disk"

class Pool(pool.Pool):
    type = "freenas"
    capabilities = ["roo", "rwo", "shared", "blk", "iscsi"]

    @lazy
    def insecure_tpc(self):
        try:
            return self.conf_get("insecure_tpc")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def compression(self):
        try:
            return self.conf_get("compression")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def sparse(self):
        try:
            return self.conf_get("sparse")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def blocksize(self):
        try:
            return self.conf_get("blocksize")
        except ex.OptNotFound as exc:
            return exc.default

    def delete_disk(self, name=None, disk_id=None):
        return self.array.del_iscsi_zvol(name=name, volume=self.diskgroup)

    def create_disk(self, name, size, nodes=None):
        mappings = self.get_mappings(nodes)
        if not mappings:
            raise ex.excError("refuse to create a disk with no mappings")
        lock_id = None
        result = {}
        try:
            lock_id = self.node._daemon_lock(LOCK_NAME, timeout=120, on_error="raise")
            self.log.info("lock acquired: name=%s id=%s", LOCK_NAME, lock_id)
            result = self.array.add_iscsi_zvol(name=name, size=size,
                                               volume=self.diskgroup,
                                               mappings=mappings,
                                               insecure_tpc=self.insecure_tpc,
                                               compression=self.compression,
                                               sparse=self.sparse,
                                               blocksize=self.blocksize)
        finally:
            self.node._daemon_unlock(LOCK_NAME, lock_id)
            self.log.info("lock released: name=%s id=%s", LOCK_NAME, lock_id)
        return result

    def translate(self, name=None, size=None, fmt=True, shared=False):
        disk = {
            "rtype": "disk",
            "type": "disk",
            "name": name if name else "{id}",
            "scsireserv": True,
            "shared": shared,
            "size": size,
        }
        if not fmt:
            return [disk]
        fs = {
            "rtype": "fs",
            "type": self.fs_type,
            "dev": "{disk#1.exposed_devs[0]}",
            "shared": shared,
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

    def get_targets(self):
        return [tgt["iscsi_target_name"] for tgt in self.array.list_iscsi_target()]

    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="iscsi")
