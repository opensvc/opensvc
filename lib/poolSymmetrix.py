from __future__ import print_function

import os
import sys

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall
from rcGlobalEnv import rcEnv
from rcSymmetrix import Arrays

LOCK_NAME = "symmetrix_create_disk"

class Pool(pool.Pool):
    type = "symmetrix"
    capabilities = ["roo", "rwo", "shared", "blk", "fc"]

    @lazy
    def slo(self):
        try:
            return self.conf_get("slo")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def srp(self):
        try:
            return self.conf_get("srp")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def srdf(self):
        try:
            return self.conf_get("srdf")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def rdfg(self):
        try:
            return self.conf_get("rdfg")
        except ex.OptNotFound as exc:
            return exc.default

    def delete_disk(self, name=None, disk_id=None):
        self.array.del_disk(dev=disk_id)

    def create_disk(self, name, size, nodes=None):
        mappings = self.get_mappings(nodes)
        lock_id = None
        try:
            lock_id = self.node._daemon_lock(LOCK_NAME, timeout=120, on_error="raise")
            self.log.info("lock acquired: name=%s id=%s", LOCK_NAME, lock_id)
            result = self.array.add_disk(name=name, size=size,
                                         srp=self.srp,
                                         srdf=self.srdf,
                                         rdfg=self.rdfg,
                                         mappings=mappings)
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
    def array(self):
        o = Arrays()
        array = o.get_array(self.array_name)
        if array is None:
            raise ex.excError("array %s not found" % self.array_name)
        array.node = self.node
        return array

    def status(self):
        from converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.srp),
            "capabilities": self.capabilities,
        }
        try:
            dg = [dg for dg in self.array.get_srps() if dg["name"] == self.srp][0]
        except Exception as exc:
            print(exc, file=sys.stderr)
            return data
        data["free"] = convert_size(dg["free_capacity_gigabytes"], default_unit="G", _to="KB")
        data["used"] = convert_size(dg["used_capacity_gigabytes"], default_unit="G", _to="KB")
        data["size"] = convert_size(dg["usable_capacity_gigabytes"], default_unit="G", _to="KB")
        return data

    def get_targets(self):
        tgts = []
        for director in self.array.get_directors():
            for port in director.get("Port", []):
                pinfo = port.get("Port_Info", {})
                if "node_wwn" in pinfo and "port_wwn" in pinfo:
                    tgts.append(pinfo["port_wwn"].lower())
        return tgts

    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="fc")

