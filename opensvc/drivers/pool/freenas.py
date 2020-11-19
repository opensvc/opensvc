from __future__ import print_function

import sys

import core.exceptions as ex
from utilities.lazy import lazy
from drivers.array.freenas import Freenass
from core.pool import BasePool

LOCK_NAME = "freenas_create_disk"

class Pool(BasePool):
    type = "freenas"
    capabilities = ["roo", "rwo", "shared", "blk", "iscsi"]

    @lazy
    def insecure_tpc(self):
        return self.oget("insecure_tpc")

    @lazy
    def compression(self):
        return self.oget("compression")

    @lazy
    def sparse(self):
        return self.oget("sparse")

    @lazy
    def blocksize(self):
        return self.oget("blocksize")

    def delete_disk(self, name=None, disk_id=None):
        return self.array.del_iscsi_zvol(name=name, volume=self.diskgroup)

    def create_disk(self, name, size, nodes=None):
        mappings = self.get_mappings(nodes)
        if not mappings:
            raise ex.Error("refuse to create a disk with no mappings")
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
        data = []
        disk = {
            "rtype": "disk",
            "type": "disk",
            "name": name,
            "scsireserv": True,
            "shared": shared,
            "size": size,
        }
        data.append(disk)
        if fmt:
            data += self.add_fs(name, shared)
        return data

    @lazy
    def array_name(self):
        return self.oget("array")

    @lazy
    def diskgroup(self):
        return self.oget("diskgroup")

    @lazy
    def array(self):
        o = Freenass()
        array = o.get_freenas(self.array_name)
        if array is None:
            raise ex.Error("array %s not found" % self.array_name)
        array.node = self.node
        return array

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.diskgroup),
            "capabilities": self.capabilities,
        }
        if not usage:
            return data
        try:
            dg = [dg for dg in self.array.list_pools() if dg["name"] == self.diskgroup][0]
        except Exception as exc:
            data["error"] = str(exc)
            return data
        data["free"] = convert_size(dg["avail"], _to="KB")
        data["used"] = convert_size(dg["used"], _to="KB")
        data["size"] = convert_size(dg["avail"] + dg["used"], _to="KB")
        return data

    def get_targets(self):
        return [tgt["name"] for tgt in self.array.list_iscsi_target()]

    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="iscsi")
