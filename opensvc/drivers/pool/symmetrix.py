from __future__ import print_function

import sys

import core.exceptions as ex
from utilities.lazy import lazy
from env import Env
from drivers.array.symmetrix import Arrays
from core.pool import BasePool

LOCK_NAME = "symmetrix_create_disk"

class Pool(BasePool):
    type = "symmetrix"
    capabilities = ["roo", "rwo", "shared", "blk", "fc"]

    @lazy
    def slo(self):
        return self.oget("slo")

    @lazy
    def srp(self):
        return self.oget("srp")

    @lazy
    def srdf(self):
        return self.oget("srdf")

    @lazy
    def rdfg(self):
        return self.oget("rdfg")

    def delete_disk(self, name=None, disk_id=None):
        self.array.del_disk(dev=disk_id)

    def create_disk(self, name, size, nodes=None):
        if self.rdfg:
            return self.create_disk_srdf(name, size, nodes=nodes)
        else:
            return self.create_disk_simple(name, size, nodes=nodes)

    def create_disk_simple(self, name, size, nodes=None):
        mappings = self.get_mappings(nodes)
        if not mappings:
            raise ex.Error("refuse to create a disk with no mappings")
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

    def create_disk_srdf(self, name, size, nodes=None):
        ans = self.array_nodes(nodes)
        r1_nodes = ans[self.array_name]
        r2_nodes = ans[self.remote_array_name]
        r1_result = self.create_disk_simple(name=name, size=size, nodes=r1_nodes)
        dev_id = r1_result["disk_devid"]
        remote_mappings = self.get_mappings(r2_nodes)
        remote_dev_id = self.array.remote_dev_id(dev_id)
        self.log.info("local dev=%s.%s remote dev=%s.%s",
                      self.array.sid, dev_id,
                      self.remote_array.sid, remote_dev_id)
        try:
            lock_id = self.node._daemon_lock(LOCK_NAME, timeout=120, on_error="raise")
            self.log.info("lock acquired: name=%s id=%s", LOCK_NAME, lock_id)
            r2_result = self.remote_array.add_map(dev=remote_dev_id,
                                                  mappings=remote_mappings,
                                                  slo=self.slo,
                                                  srp=self.srp)
            self.log.info("%s", r2_result)
        finally:
            self.node._daemon_unlock(LOCK_NAME, lock_id)
            self.log.info("lock released: name=%s id=%s", LOCK_NAME, lock_id)
        result = {
            "r1": r1_result,
            "r2": r2_result,
            "disk_ids": {}
        }
        for node in r1_nodes:
            result["disk_ids"][node] = r1_result["disk_id"]
        for node in r2_nodes:
            result["disk_ids"][node] = r2_result["disk_id"]
        return result

    def translate(self, name=None, size=None, fmt=True, shared=False):
        data = []
        data.append({
            "rtype": "disk",
            "type": "disk",
            "name": name,
            "scsireserv": True,
            "shared": shared,
            "size": size,
        })
        if fmt:
            data += self.add_fs(name, shared)
        return data

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
    def array(self):
        o = Arrays()
        array = o.get_array(self.array_name)
        if array is None:
            raise ex.Error("array %s not found" % self.array_name)
        array.node = self.node
        return array

    @lazy
    def remote_array(self):
        o = Arrays()
        array = o.get_array(self.remote_array_name)
        if array is None:
            raise ex.Error("remote array %s not found" % self.remote_array_name)
        array.node = self.node
        return array

    def pool_status(self, usage=True):
        from utilities.converters import convert_size
        data = {
            "type": self.type,
            "name": self.name,
            "head": "array://%s/%s" % (self.array_name, self.srp),
            "capabilities": self.capabilities,
        }
        if not usage:
            return data
        try:
            dg = [dg for dg in self.array.get_srps() if dg["name"] == self.srp][0]
        except Exception as exc:
            data["error"] = str(exc)
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

