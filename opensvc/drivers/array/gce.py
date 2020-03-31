from env import Env
from utilities.proc import justcall

class GceDiskss(object):
    arrays = []

    def __init__(self, objects=None):
        if objects is None:
            objects = []
        self.arrays.append(GceDisks())

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class GceDisks(object):
    def __init__(self):
        self.keys = ['disks', 'snapshots', 'quotas', 'instances']
        self.name = "gce project "+Env.fqdn.split(".")[-2]

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
