from __future__ import print_function

import sys

import core.exceptions as ex
from utilities.lazy import lazy
from env import Env
from drivers.array.dorado import Dorados
from core.pool import BasePool

def session(fn):
    """
    A decorator for caching the result of a function
    """
    attr_name = '_fcache_' + fn.__name__

    def wrapper(self, *args, **kwargs):
        data = fn(self, *args, **kwargs)
        self.array.close_session()
        if self.hypermetrodomain:
            self.remote_array.close_session()
        return data

    return wrapper

class Pool(BasePool):
    type = "dorado"
    capabilities = ["roo", "rwo", "shared", "blk", "fc"]


    @lazy
    def lock_name(self):
        return "dorado_%s_create_disk" % self.array_name


    def delete_disk(self, name=None, disk_id=None):
        if self.hypermetrodomain:
            result = self.delete_disk_hypermetro(name=name, disk_id=disk_id)
        else:
            result = self.delete_disk_simple(name=name, disk_id=disk_id)
        return result


    @session
    def delete_disk_simple(self, name=None, disk_id=None):
        return self.array.del_disk(name=name, naa=disk_id)


    @session
    def delete_disk_hypermetro(self, name=None, disk_id=None):
        data = {}
        response = self.array.unmap(name=name, naa=disk_id)
        data["unmap_local"] = response
        response = self.remote_array.unmap(name=name, naa=disk_id)
        data["unmap_remote"] = response
        response = self.array.del_disk(name=name, naa=disk_id)
        data["del_disk_local"] = response
        response = self.remote_array.del_disk(name=name, naa=disk_id)
        data["del_disk_remote"] = response
        return data


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


    def create_disk(self, name, size, nodes=None):
        if self.hypermetrodomain:
            return self.create_disk_hypermetro(name, size, nodes=nodes)
        else:
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
                                   storagepool=self.storagepool,
                                   compression=self.compression,
                                   hypermetrodomain=self.hypermetrodomain,
                                   dedup=self.dedup,
                                   mappings=mappings)
        return result


    @session
    def create_disk_hypermetro(self, name, size, nodes=None):
        if False:
            ans = self.array_nodes(nodes)
            r1_nodes = ans[self.array_name]
            r2_nodes = ans[self.remote_array_name]
            r2_mappings = self.get_mappings(r2_nodes)
        else:
            r1_nodes = nodes
            r2_nodes = nodes
            r2_mappings = self.get_mappings(r2_nodes)
        r1_result = self.create_disk_simple(name=name, size=size, nodes=r1_nodes, array=self.array)
        dev_id = r1_result["disk_devid"]
        self.log.info("local lun %s.%s created and mapped", self.array.name, dev_id)
        r2_result = self.create_disk_simple(name=name, size=size, nodes=None, array=self.remote_array)
        remote_dev_id = r2_result["disk_devid"]
        self.log.info("remote lun %s.%s created", self.remote_array.name, remote_dev_id)
        pair_result = self.array.pair_luns(dev_id, remote_dev_id, self.hypermetrodomain)
        self.log.info("paired")
        pair_result = self.array.synchronize_lun_hypermetropair(dev_id)
        self.log.info("synchronized")
        r2_mapping_result = self.remote_array.map_lun(id=remote_dev_id, mappings=r2_mappings)
        self.log.info("remote lun %s.%s mapped", self.remote_array.name, remote_dev_id)
        result = {
            "r1": r1_result,
            "r2": r2_result,
            "pair": pair_result,
            "r2_mapping": r2_mapping_result,
            "disk_id": r1_result.get("disk_id", ""),
        }
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
    def hypermetrodomain(self):
        return self.oget("hypermetrodomain")


    @lazy
    def storagepool(self):
        return self.oget("diskgroup")


    @lazy
    def array_name(self):
        return self.oget("array")


    @lazy
    def remote_array_name(self):
        for node in self.node.cluster_nodes:
             if node == Env.nodename:
                 continue
             an = self.oget("array", impersonate=node)
             if an and an != self.array_name:
                 return an


    @lazy
    def compression(self):
        return self.oget("compression")


    @lazy
    def dedup(self):
        return self.oget("dedup")


    @lazy
    def array(self):
        o = Dorados()
        array = o.get_dorado(self.array_name)
        if array is None:
            raise ex.Error("array %s not found" % self.array_name)
        array.node = self.node
        return array


    @lazy
    def remote_array(self):
        o = Dorados()
        array = o.get_dorado(self.remote_array_name)
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
            status = self.array.get_storagepool(name=self.storagepool)
        except Exception as exc:
            data["error"] = str(exc)
            return data
        data["size"] = convert_size(int(status["USERTOTALCAPACITY"])*512, _to="KB")
        data["free"] = convert_size(int(status["USERFREECAPACITY"])*512, _to="KB")
        data["used"] = data["size"] - data["free"]
        return data


    def get_targets(self):
        return [tgt["WWN"] for tgt in self.array.list_fc_port()]


    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="fc")
