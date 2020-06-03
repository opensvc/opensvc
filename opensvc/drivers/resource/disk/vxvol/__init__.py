import re
import os
from collections import namedtuple

import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS
from utilities.cache import cache
from utilities.converters import convert_size
from utilities.proc import justcall, which

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vxvol"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the logical volume group"
    },
    {
        "keyword": "vg",
        "at": True,
        "text": "The name of the volume group hosting the logical volume.",
        "example": "vg1"
    },
    {
        "keyword": "create_options",
        "convert": "shlex",
        "default": [],
        "at": True,
        "provisioning": True,
        "text": "Additional options to pass to the logical volume create command (:cmd:`lvcreate` or :cmd:`vxassist`, depending on the driver). Size and name are alread set.",
        "example": "--contiguous y"
    },
    {
        "keyword": "size",
        "convert": "size",
        "provisioning": True,
        "text": "The logical volume size, in size expression format."
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
    if which("vxvol"):
        return ["disk.vxvol"]
    return []


class DiskVxvol(BaseDisk):
    def __init__(self, vg=None, create_options=None, size=None, **kwargs):
        super(DiskVxvol, self).__init__(type='disk.vxvol', **kwargs)
        self.fullname = "%s/%s" % (vg, self.name)
        self.label = "vxvol %s" % self.fullname
        self.vg = vg
        self.devpath  = "/dev/vx/dsk/%s/%s" % (self.vg, self.name)
        self.create_options = create_options
        self.size = size

    def _info(self):
        data = [
          ["name", self.name],
          ["vg", self.vg],
        ]
        return data

    def vxprint(self):
        cmd = ["vxprint", "-g", self.vg, "-v"]
        out, err, ret = justcall(cmd)
        if ret == 11:
            # no lv
            return {}
        if ret != 0:
            raise ex.Error(err)
        data = {}
        for line in out.splitlines():
            words = line.split()
            if len(words) < 7:
                continue
            if words[0] == "TY":
                headers = list(words)
                continue
            lv = namedtuple("lv", headers)._make(words)
            data[lv.NAME] = lv
        return data

    def has_it(self):
        return self.name in self.vxprint()

    def is_up(self):
        """
        Returns True if the logical volume is present and activated
        """
        data = self.vxprint()
        return self.name in data and data[self.name].STATE == "ACTIVE"

    def activate_lv(self):
        cmd = ['vxvol', '-g', self.vg, 'start', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def deactivate_lv(self):
        cmd = ['vxvol', '-g', self.vg, 'stop', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return 0
        self.activate_lv()
        self.can_rollback = True

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        if dev is None:
            self.log.error("the device %s is not in the devtree", devpath)
            return
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.sub_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.Error("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        tree = self.svc.node.devtree
        self.remove_dev_holders(self.devpath, tree)

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return
        self.remove_holders()
        self.deactivate_lv()

    def lv_devices(self):
        """
        Return the set of sub devices.
        """
        devs = set()
        return devs
        
    def sub_devs(self):
        if not self.has_it():
            return set()
        return self.lv_devices()

    def exposed_devs(self):
        if not self.has_it():
            return set()
        devs = set()
        if os.path.exists(self.devpath):
            devs.add(self.devpath)
        return devs

    def pre_unprovision_stop(self):
        # leave the vxvol active for wipe
        pass

    def unprovisioner(self):
        if not which('vxassist'):
            raise ex.Error("vxassist command not found")

        if not self.has_it():
            self.log.info("skip vxvol unprovision: %s already unprovisioned", self.fullname)
            return

        if which('wipefs') and os.path.exists(self.devpath) and self.is_up():
            self.vcall(["wipefs", "-a", self.devpath])

        cmd = ["vxassist", "-g", self.vg, "remove", "volume", self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.svc.node.unset_lazy("devtree")

    def provisioner(self):
        if not which('vxassist'):
            raise ex.Error("vxassist command not found")

        if self.has_it():
            self.log.info("skip vxvol provision: %s already exists" % self.fullname)
            return

        if not self.size:
            raise ex.Error("a size is required")

        size_parm = str(self.size).upper()
        size_parm = [str(convert_size(size_parm, _to="m"))+'M']
        create_options = self.create_options or self.oget("create_options")

        # strip dev dir in case the alloc vxassist parameter was formatted using sub_devs
        # lazy references
        for idx, option in enumerate(create_options):
            create_options[idx] = option.replace("/dev/vx/dsk/", "")

        # create the logical volume
        cmd = ['vxassist', '-g', self.vg, "make", self.name] + size_parm + create_options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        self.can_rollback = True
        self.svc.node.unset_lazy("devtree")

