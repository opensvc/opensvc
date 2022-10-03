from __future__ import print_function

import sys

import core.exceptions as ex
from utilities.lazy import lazy
from env import Env
from drivers.array.pure import Arrays
from core.pool import BasePool

class Pool(BasePool):
    type = "pure"
    capabilities = ["rox", "rwx", "roo", "rwo", "shared", "blk", "fc"]


    @lazy
    def lock_name(self):
        return "pure_%s_create_disk" % self.array_name


    def delete_disk(self, name=None, disk_id=None):
        result = self.delete_disk_simple(disk_id=disk_id)
        return result

    def serial_from_disk_id(self, disk_id):
        if disk_id is None:
            return
        return disk_id[8:]

    def delete_disk_simple(self, disk_id=None):
        serial = self.serial_from_disk_id(disk_id)
        return self.array.del_disk(serial=serial, now=self.delete_now)

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

    def sep(self):
        return "-"

    def create_disk(self, name, size, nodes=None):
        return self.create_disk_simple(name, size, nodes=nodes)

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
        if self.pod:
            name = self.pod + "::" + name
        elif self.volumegroup:
            name = self.volumegroup + "/" + name
        result = array.add_disk(name=name,
                                size=size,
                                pod=self.pod,
                                volumegroup=self.volumegroup,
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
    def delete_now(self):
        return self.oget("delete_now")

    @property
    def volumegroup(self):
        return self.oget("volumegroup")

    @property
    def pod(self):
        return self.oget("pod")

    @property
    def array_name(self):
        return self.oget("array")

    @lazy
    def array(self):
        o = Arrays()
        array = o.get_array(self.array_name)
        if array is None:
            raise ex.Error("array %s not found" % self.array_name)
        array.node = self.node
        return array

    @property
    def head(self):
        if self.pod:
            return self.pod
        elif self.volumegroup:
            return self.volumegroup
        else:
            return ""

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.head),
            "capabilities": self.capabilities,
        }
        if not usage:
            return data
        try:
            status = self.array.get_arrays()[0]
            space = status["space"]
        except Exception as exc:
            print(exc)
            data["error"] = str(exc)
            return data
        data["size"] = status["capacity"] / 1024
        data["used"] = space["total_physical"] / 1024
        data["free"] = data["size"] - data["used"]
        return data


    def get_targets(self):
        qfilter = "services='scsi-fc' and enabled='true'"
        tgts = [tgt["fc"]["wwn"].replace(":", "").lower() for tgt in self.array.get_network_interfaces(qfilter=qfilter)]
        return tgts

    def get_mappings(self, nodes):
        return self._get_mappings(nodes, transport="fc")
