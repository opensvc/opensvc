import os
import glob
import time

import core.status
import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from core.objects.svcdict import KEYS
from utilities.subsystems.amazon import AmazonMixin

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "amazon"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "volumes",
        "convert": "list",
        "at": True,
        "required": True,
        "text": "A whitespace separated list of amazon volumes. Any member of the list can be set to a special <key=value,key=value> value. In this case the provisioner will allocate a new volume with the specified characteristics and replace this member with the allocated volume id. The supported keys are the same as those supported by the awscli ec2 create-volume command: size, iops, availability-zone, snapshot, type and encrypted.",
        "example": "vol-123456 vol-654321"
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if not which("ec2"):
        return data
    if os.path.exists("/sys/hypervisor/uuid"):
        with open("/sys/hypervisor/uuid", "r") as f:
           uuid = f.read().lower()
        if not uuid.startswith("ec2"):
           return data
    if os.path.exists("/sys/devices/virtual/dmi/id/product_uuid"):
        with open("/sys/devices/virtual/dmi/id/product_uuid", "r") as f:
           uuid = f.read().lower()
        if not uuid.startswith("ec2"):
           return data
    data.append("ip.amazon")
    return data


class DiskAmazon(BaseDisk, AmazonMixin):
    def __init__(self,
                 volumes=None,
                 client_id=None,
                 keyring=None,
                 **kwargs):

        BaseDisk.__init__(self, type="disk.amazon", **kwargs)

        self.volumes = volumes or set()
        self.label = self.fmt_label()
        self.existing_volume_ids = None
        self.mapped_bdevs = None
        self.dev_prefix = None

    def get_existing_volume_ids(self, refresh=False):
        if self.existing_volume_ids is not None and not refresh:
             return self.existing_volume_ids
        data = self.aws(["ec2", "describe-volumes", "--volume-ids"] + self.volumes, verbose=False)
        self.existing_volume_ids = [ b["VolumeId"] for b in data["Volumes"] ]
        return self.existing_volume_ids

    def get_state(self, vol):
        data = self.aws(["ec2", "describe-volumes", "--volume-ids", vol], verbose=False)
        try:
            avail = data["Volumes"][0]["State"]
        except:
            avail = "not present"
        return avail

    def wait_dev(self, dev):
        dev = self.mangle_devpath(dev)
        for i in range(60):
            if os.path.exists(dev):
                return
            self.log.info("%s is not present yet" % dev)
            time.sleep(1)
        raise ex.Error("timeout waiting for %s to appear." % dev)

    def wait_avail(self, vol):
        for i in range(30):
            state = self.get_state(vol)
            self.log.info("%s state: %s" % (vol, state))
            if state == "available":
                return
            time.sleep(1)
        raise ex.Error("timeout waiting for %s to become available. last %s" % (vol, state))

    def get_mapped_dev(self, volume):
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.Error("can't find instance data")

        devs = []
        for b in instance_data["BlockDeviceMappings"]:
            if b["Ebs"]["VolumeId"] != volume:
                continue
            return b["DeviceName"]

    def get_mapped_devs(self, volumes=None):
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.Error("can't find instance data")

        devs = []
        for b in instance_data["BlockDeviceMappings"]:
            if volumes is not None and b["Ebs"]["VolumeId"] not in volumes:
                continue
            try:
                devs.append(b["DeviceName"])
            except:
                pass
        return devs

    def get_next_dev(self):
        devs = self.get_mapped_devs()
        if (devs) == 0:
            return "/dev/sdb"
        devs = [ r.rstrip("0123456789") for r in devs ]
        devs = [ r.replace("/dev/sd", "") for r in devs ]
        devs += [ r.replace("/dev/sd", "") for r in glob.glob("/dev/sd[a-z]*") ]
        devs += [ r.replace("/dev/xvd", "") for r in glob.glob("/dev/xvd[a-z]*") ]
        chars = "abcdefghijklmnopqrstuvwxyz"
        for c in chars:
            if c not in devs:
                return "/dev/sd"+c
        for c in chars:
            for d in chars:
                if c+d not in devs:
                    return "/dev/sd"+c+d
        for c in chars:
            for d in chars:
                for e in chars:
                    if c+d+e not in devs:
                        return "/dev/sd"+c+d+e
        raise ex.Error("no available device name")

    def get_mapped_bdevs(self, refresh=False):
        if self.mapped_bdevs is not None and not refresh:
             return self.mapped_bdevs
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.Error("can't find instance data")

        self.mapped_bdevs = []
        for b in instance_data["BlockDeviceMappings"]:
            try:
                self.mapped_bdevs.append(b["Ebs"]["VolumeId"])
            except:
                pass
        return self.mapped_bdevs

    def fmt_label(self):
        s = "ec2 volumes "
        s += ", ".join(self.volumes)
        return s

    def has_it(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume in mapped:
            return True
        return False

    def up_count(self):
        mapped = self.get_mapped_bdevs()
        l = []
        for volume in self.volumes:
            if volume in mapped:
                l.append(volume)
        return l

    def validate_volumes(self):
        existing_volumes = self.get_existing_volume_ids()
        non_exist = set(self.volumes) - set(existing_volumes)
        if len(non_exist) > 0:
            raise Exception("non allocated volumes: %s" % ', '.join(non_exist))

    def _status(self, verbose=False):
        try:
            self.validate_volumes()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN
        l = self.up_count()
        n = len(l)
        unmapped = sorted(list(set(self.volumes) - set(l)))
        if n == len(self.volumes):
            return core.status.UP
        elif n == 0:
            return core.status.DOWN
        else:
            self.status_log("unattached: "+", ".join(unmapped))
            return core.status.DOWN

    def get_dev_prefix(self):
        if self.dev_prefix is not None:
            return self.dev_prefix
        if len(glob.glob("/dev/xvd*")) > 0:
            self.dev_prefix = "/dev/xvd"
        else:
            self.dev_prefix = "/dev/sd"
        return self.dev_prefix

    def mangle_devpath(self, dev):
        return dev.replace("/dev/sd", self.get_dev_prefix())

    def do_start_one(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume in mapped:
            self.log.info(volume+" is already attached")
            dev = self.get_mapped_dev(volume)
        else:
            dev = self.get_next_dev()
            data = self.aws([
              "ec2", "attach-volume",
              "--instance-id", self.get_instance_id(),
              "--volume-id", volume,
              "--device", dev
            ])
            self.can_rollback = True
        self.wait_dev(dev)
        self._create_static_name(self.mangle_devpath(dev), volume)

    def _create_static_name(self, dev, volume):
        suffix = str(self.volumes.index(volume))
        self.create_static_name(dev, suffix)

    def do_start(self):
        self.validate_volumes()
        for volume in self.volumes:
            self.do_start_one(volume)
        self.get_mapped_bdevs(refresh=True)

    def do_stop_one(self, volume):
        mapped = self.get_mapped_bdevs()
        if volume not in mapped:
            self.log.info(volume+" is already detached")
            return
        data = self.aws([
          "ec2", "detach-volume",
          "--instance-id", self.get_instance_id(),
          "--volume-id", volume
        ])

    def do_stop(self):
        self.validate_volumes()
        for volume in self.volumes:
            self.do_stop_one(volume)
        self.get_mapped_bdevs(refresh=True)

    def exposed_devs(self):
        devs = self.get_mapped_devs(volumes=self.volumes)
        if len(devs) == 0:
            return set(devs)
        return set([ self.mangle_devpath(r) for r in devs ])

    def exposed_disks(self):
        disks = set([ r.rstrip("1234567890") for r in self.sub_devs() ])
        return disks

    def provisioner(self):
        for volume in self.volumes:
            volumes_done = self._provisioner(volume)
        volumes = ' '.join(volumes_done)
        self.svc.set_multi(["%s.volumes=%s" % (self.rid, volumes)])
        self.volumes = volumes_done
        self.log.info("provisioned")
        self.start()
        self.svc.node.unset_lazy("devtree")

    def _provisioner(self, volume):
        volumes_done = []
        if not volume.startswith("<") and not volume.endswith(">"):
            self.log.info("volume %s already provisioned" % volume)
            volumes_done.append(volume)
            return

        s = volume.strip("<>")
        v = s.split(",")
        kwargs = {}
        for e in v:
            try:
                key, val = e.split("=")
            except:
                raise ex.Error("format error: %s. expected key=value." % e)
            kwargs[key] = val
        cmd = ["ec2", "create-volume"]
        if "size" in kwargs:
            cmd += ["--size", kwargs["size"]]
        if "iops" in kwargs:
            cmd += ["--iops", kwargs["iops"]]
        if "availability-zone" in kwargs:
            cmd += ["--availability-zone", kwargs["availability-zone"]]
        else:
            node = self.get_instance_data()
            availability_zone = node["Placement"]["AvailabilityZone"]
            cmd += ["--availability-zone", availability_zone]
        data = self.aws(cmd)
        self.wait_avail(data["VolumeId"])
        volumes_done.append(data["VolumeId"])
        self.svc.node.unset_lazy("devtree")
        return volumes_done


