import rcExceptions as ex
import json
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

class GceDiskss(object):
    arrays = []

    def __init__(self, objects=[]):
        self.index = 0
        self.arrays.append(GceDisks())

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class GceDisks(object):
    def __init__(self):
        self.keys = ['disks', 'snapshots', 'quotas', 'instances']
        self.name = "gce project "+rcEnv.fqdn.split(".")[-2]

    def get_disks(self):
        cmd = ["gcloud", "compute", "disks", "list", "-q", "--format", "json"]
        out, err, ret = justcall(cmd)
        return out

    def get_snapshots(self):
        cmd = ["gcloud", "compute", "snapshots", "list", "-q", "--format", "json"]
        out, err, ret = justcall(cmd)
        return out

    def get_quotas(self):
        cmd = ["gcloud", "compute", "regions", "list", "-q", "--format", "json"]
        out, err, ret = justcall(cmd)
        return out

    def get_instances(self):
        cmd = ["gcloud", "compute", "instances", "list", "-q", "--format", "json"]
        out, err, ret = justcall(cmd)
        return out


if __name__ == "__main__":
    o = GceDiskss()
    for gcedisks in o:
        print(gcedisks.get_disks())
