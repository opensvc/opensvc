import os

import core.exceptions as ex

from utilities.converters import convert_size
from env import Env
from utilities.subsystems.zfs import Dataset
from core.objects.svcdict import KEYS
from core.capabilities import capabilities
from .. import KWS_POOLING

KEYWORDS = KWS_POOLING + [
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "size",
        "required": False,
        "convert": "size",
        "at": True,
        "text": "Used by default as the refquota of the provisioned dataset. The quota, refquota, reservation and refreservation values can be expressed as a multiplier of size (example: quota=x2).",
        "provisioning": True
    },
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "refquota",
        "required": False,
        "default": "x1",
        "at": True,
        "text": "The dataset 'refquota' property value to set on provision. The value can be 'none', or a size expression, or a multiplier of the size keyword value (ex: x2).",
        "provisioning": True
    },
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "quota",
        "required": False,
        "at": True,
        "text": "The dataset 'quota' property value to set on provision. The value can be 'none', or a size expression, or a multiplier of the size keyword value (ex: x2).",
        "provisioning": True
    },
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "refreservation",
        "required": False,
        "at": True,
        "text": "The dataset 'refreservation' property value to set on provision. The value can be 'none', or a size expression, or a multiplier of the size keyword value (ex: x2).",
        "provisioning": True
    },
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "reservation",
        "required": False,
        "at": True,
        "text": "The dataset 'reservation' property value to set on provision. The value can be 'none', or a size expression, or a multiplier of the size keyword value (ex: x2).",
        "provisioning": True
    },
]

KEYS.register_driver(
    "fs",
    "zfs",
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("zfs"):
        data.append("fs.zfs")
    return data

class FsZfsMixin():
    @property
    def poolname(self):
        return self.device.split("/")[0]

    def unprovisioner(self):
        if "node.x.zfs" not in capabilities:
            self.log.error("zfs command not found")
            raise ex.Error
        import core.status
        need_stop = None
        for r in self.svc.get_resources(["volume", "disk.zfs"]):
            if r.device != self.poolname:
                continue
            if r.status() not in (core.status.UP, core.status.STDBY_UP):
                r.start()
                need_stop = r
        dataset = Dataset(self.device, log=self.log)
        if dataset.exists():
            dataset.destroy(["-r"])
        if os.path.exists(self.mount_point) and os.path.isdir(self.mount_point):
            try:
                os.rmdir(self.mount_point)
                self.log.info("rmdir %s", self.mount_point)
            except OSError as exc:
                self.log.warning("failed to rmdir %s: %s", self.mount_point, exc)
        if need_stop:
            need_stop.stop()

    def provisioner(self):
        if "node.x.zfs" not in capabilities:
            self.log.error("zfs command not found")
            raise ex.Error
        dataset = Dataset(self.device, log=self.log)
        mkfs_opt = ["-p"]
        mkfs_opt += self.oget("mkfs_opt")

        if not any([True for e in mkfs_opt if e.startswith("mountpoint=")]):
            mkfs_opt += ['-o', 'mountpoint='+self.mount_point]
        if not any([True for e in mkfs_opt if e.startswith("canmount=")]):
            mkfs_opt += ['-o', 'canmount=noauto']

        if dataset.exists() is False:
            dataset.create(mkfs_opt)

        def convert(x, size):
            val = self.oget(x)
            if val in (None, "none", ""):
                return
            if val[0] == "x":
                if not size:
                    return
                try:
                    m = float(val[1:])
                except Exception:
                    raise ex.Error("%s set to a multiplier of size, but invalid: %s" % (x, val))
                return int(size * m)
            return convert_size(val, _to="m")

        nv_list = dict()
        size = self.oget("size")
        if size:
            size = convert_size(size, _to="m")
        for prop in ("refquota", "quota", "reservation", "refreservation"):
            val = convert(prop, size)
            if val:
                nv_list[prop] = "%dM" % val
        if not nv_list:
            return
        dataset.verify_prop(nv_list)

    def provisioned(self):
        dataset = Dataset(self.device, log=self.log)
        return dataset.exists()
