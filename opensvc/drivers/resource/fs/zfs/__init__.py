import os

import core.exceptions as ex

from utilities.converters import convert_size
from env import Env
from utilities.subsystems.zfs import Dataset
from utilities.proc import which
from core.objects.svcdict import KEYS
from .. import KWS_POOLING

KEYWORDS = KWS_POOLING + [
    {
        "section": "fs",
        "rtype": "zfs",
        "keyword": "size",
        "required": False,
        "convert": "size",
        "at": True,
        "text": "The quota in MB of the provisioned dataset.",
        "provisioning": True
    },
]

KEYS.register_driver(
    "fs",
    "zfs",
    name=__name__,
    keywords=KEYWORDS,
)

class FsZfsMixin():
    def unprovisioner(self):
        if not which(Env.syspaths.zfs):
            self.log.error("zfs command not found")
            raise ex.Error
        dataset = Dataset(self.device, log=self.log)
        if dataset.exists():
            dataset.destroy(["-r"])
        if os.path.exists(self.mount_point) and os.path.isdir(self.mount_point):
            try:
                os.rmdir(self.mount_point)
                self.log.info("rmdir %s", self.mount_point)
            except OSError as exc:
                self.log.warning("failed to rmdir %s: %s", self.mount_point, exc)

    def provisioner(self):
        if not which(Env.syspaths.zfs):
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

        nv_list = dict()
        size = self.oget("size")
        if not size:
            return
        if size:
            nv_list['refquota'] = "%dM" % convert_size(size, _to="m")
        dataset.verify_prop(nv_list)

    def provisioned(self):
        dataset = Dataset(self.device, log=self.log)
        return dataset.exists()
