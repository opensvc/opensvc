import os
import tempfile
import time

import core.exceptions as ex

from .. import KEYWORDS
from ..linux import Fs
from core.objects.svcdict import KEYS
from core.capabilities import capabilities
from utilities.subsystems.btrfs import Btrfs
from utilities.lazy import lazy
from utilities.proc import justcall

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "btrfs"

KEYS.register_driver(
    "fs",
    "btrfs",
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("btrfs"):
        data.append("fs.btrfs")
    return data

class FsBtrfs(Fs):
    queryfs = ['btrfs', 'device', 'ready']

    @lazy
    def mkfs(self):
        return ['mkfs.btrfs', '-f', '-L', self.btrfs_label]

    @lazy
    def raw_btrfs_label(self):
        return '{name}.' + self.rid.replace("#", ".")

    @lazy
    def btrfs_label(self):
        return self.svc.name + '.' + self.rid.replace("#", ".")

    def provisioned(self):
        ret = super(FsBtrfs, self).provisioned()
        if not ret:
            return ret
        if self.subvol is None:
            return ret
        mnt = tempfile.mkdtemp()
        self.mount(mnt)
        try:
            btrfs = Btrfs(path=mnt)
            return btrfs.has_subvol(self.subvol)
        finally:
            self.cleanup(mnt)

    def current_label(self, mnt):
        cmd = ["btrfs", "filesystem", "label", mnt]
        ret, out, err = self.call(cmd, errlog=False)
        if ret == 0 and len(out.strip()) > 0:
            return out.strip()

    @lazy
    def subvol(self):
        l = self.mount_options.split(",")
        for e in l:
            if not e.startswith("subvol="):
                continue
            subvol = e.replace("subvol=", "")
            return subvol

    def cleanup(self, mnt):
        cmd = ["umount", mnt]
        self.vcall(cmd)
        os.removedirs(mnt)

    def write_label(self, mnt):
        current_label = self.current_label(mnt)
        if current_label is not None:
            label = current_label
            raw_btrfs_label = current_label.replace(self.svc.name, "{name}")
        else:
            label = self.btrfs_label
            raw_btrfs_label = self.raw_btrfs_label
        self.svc.set_multi(["%s.dev=%s" % (self.rid, "LABEL="+raw_btrfs_label)])
        self.unset_lazy("device")
        self.wait_label(label)

    def wait_label(self, label):
        if "node.x.findfs" not in capabilities:
            self.log.info("findfs program not found, wait arbitrary 20 seconds for label to be usable")
            time.sleep(20)
        cmd = ["findfs", "LABEL="+label]
        for i in range(20):
            out, err, ret = justcall(cmd)
            self.log.debug("%s\n%s\n%s" % (" ".join(cmd), out, err))
            if ret == 0:
                return
            self.log.info("label is not usable yet (%s)" % err.strip())
            time.sleep(2)
        raise ex.Error("timeout waiting for label to become usable")

    def mount(self, mnt):
        self.set_loopdevice()
        if self.loopdevice is None:
            device = self.device
        else:
            device = self.loopdevice
        cmd = ["mount", "-t", "btrfs", "-o", "subvolid=0", device, mnt]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def create_subvol(self):
        if self.subvol is None:
            return
        mnt = tempfile.mkdtemp()
        self.mount(mnt)
        try:
            self.write_label(mnt)
            self._create_subvol(mnt)
            self.log.info("subvolume %s provisioned" % self.subvol)
            self.start()
        finally:
            self.cleanup(mnt)

    def _create_subvol(self, mnt):
        path = os.path.join(mnt, self.subvol)
        if os.path.exists(path):
            return
        cmd = ["btrfs", "subvol", "create", path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def provisioner(self):
        if self.device.startswith("LABEL=") or self.device.startswith("UUID="):
            self.log.info("skip formatting because dev is specified by LABEL or UUID")
        else:
            self.provisioner_fs()
        self.create_subvol()


