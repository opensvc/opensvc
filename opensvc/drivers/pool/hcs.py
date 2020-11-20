from __future__ import print_function

import sys

import core.exceptions as ex
from utilities.lazy import lazy
from env import Env
from drivers.array.hcs import Hcss
from core.pool import BasePool

def session(fn):
    """
    A decorator for caching the result of a function
    """
    attr_name = '_fcache_' + fn.__name__

    def wrapper(self, *args, **kwargs):
        try:
            data = fn(self, *args, **kwargs)
        finally:
            self.array.close_session()
        return data

    return wrapper

class Pool(BasePool):
    type = "hcs"
    capabilities = ["roo", "rwo", "shared", "blk", "fc"]


    @lazy
    def lock_name(self):
        return "hcs_%s_create_disk" % self.array_name


    def delete_disk(self, name=None, disk_id=None):
        result = self.delete_disk_simple(name=name, disk_id=disk_id)
        return result


    @session
    def delete_disk_simple(self, name=None, disk_id=None):
        return self.array.del_disk(name=name, naa=disk_id)


    def array_nodes(self, nodes=None):
        if nodes is None:
            nodes = self.node.cluster_nodes
        data = {}
        for node in nodes:
            an = self.oget("array", impersonate=node)
            if an not in data:
                data[an] = [node]
            else:
                data[an] += [node]
        return data


    def default_disk_name(self, volume):
        return "%s%s" % (
            self.oget("label_prefix") or self.node.cluster_id.split("-")[0] + "-",
            volume.id.split("-")[0],
        )

    def create_disk(self, name, size, nodes=None):
        return self.create_disk_simple(name, size, nodes=nodes)



    @session
    def create_disk_simple(self, name, size, nodes=None, array=None):
        if array is None:
            array = self.array
        if nodes:
            mappings = self.get_mappings(nodes)
            if not mappings:
                raise ex.Error("refuse to create a disk with no mappings")
        else:
            mappings = None
        lock_id = None
        result = array.create_disk(name=name, size=size,
                                   pool=self.storagepool,
                                   start_ldev_id=self.start_ldev_id,
                                   end_ldev_id=self.end_ldev_id,
                                   resource_group=self.resource_group,
                                   compression=self.compression,
                                   dedup=self.dedup,
                                   mappings=mappings)
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


    @property
    def resource_group(self):
        return self.oget("resource_group")

    @property
    def end_ldev_id(self):
        return self.oget("end_ldev_id")

    @property
    def start_ldev_id(self):
        return self.oget("start_ldev_id")

    @property
    def storagepool(self):
        return self.oget("diskgroup")

    @property
    def array_name(self):
        return self.oget("array")

    @property
    def compression(self):
        return self.oget("compression")

    @property
    def dedup(self):
        return self.oget("dedup")

    @lazy
    def array(self):
        o = Hcss(log=self.log)
        array = o.get_hcs(self.array_name)
        if array is None:
            raise ex.Error("array %s not found" % self.array_name)
        array.node = self.node
        return array


    @session
    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.storagepool),
            "capabilities": self.capabilities,
        }
        if not usage:
            return data
        try:
            status = self.array.get_pool_by_name(name=self.storagepool)
        except Exception as exc:
            data["error"] = str(exc)
            return data
        data["size"] = convert_size(int(status["totalPhysicalCapacity"])*1024*1024, _to="KB")
        data["free"] = convert_size(int(status["availablePhysicalVolumeCapacity"])*1024*1024, _to="KB")
        data["used"] = data["size"] - data["free"]
        return data


    def get_targets(self):
        return [tgt["wwn"] for tgt in self.array.list_fc_port()]


    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="fc")
