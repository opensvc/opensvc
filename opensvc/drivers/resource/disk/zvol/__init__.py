import os
import time

import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from utilities.converters import convert_size
from env import Env
from utilities.lazy import lazy
from utilities.subsystems.zfs import dataset_exists, zpool_devs
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which
from utilities.string import bdecode

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "zvol"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "required": True,
        "at": True,
        "text": "The full name of the zfs volume in the ``<pool>/<name>`` form.",
        "example": "tank/zvol1"
    },
    {
        "keyword": "size",
        "provisioning": True,
        "convert": "size",
        "at": True,
        "text": "The size of the zfs volume to create.",
        "example": "1g"
    },
    {
        "keyword": "create_options",
        "provisioning": True,
        "convert": "shlex",
        "default": [],
        "at": True,
        "text": "The :cmd:`zfs create -V <name>` extra options.",
        "example": "-o dedup=on"
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
    if which("zfs"):
        return ["disk.zvol"]
    return []


class DiskZvol(BaseDisk):
    def __init__(self,
                 size=None,
                 create_options=None,
                 **kwargs):
        super(DiskZvol, self).__init__(type='disk.zvol', **kwargs)
        self.size = size
        self.create_options = create_options or []
        self.label = "zvol %s" % self.name
        if self.name:
            self.pool = self.name.split("/", 1)[0]
        else:
            self.pool = None

    def _info(self):
        data = [
          ["name", self.name],
          ["pool", self.pool],
          ["device", self.device],
        ]
        return data

    def has_it(self):
        return dataset_exists(self.name, "volume")

    def is_up(self):
        """
        Returns True if the zvol exists and the pool imported
        """
        return self.has_it()

    def do_start(self):
        pass

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        if dev is None:
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
        self.remove_dev_holders(self.device, tree)

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return
        self.remove_holders()

    @lazy
    def device(self):
        return "/dev/%s/%s" % (self.pool, self.name.split("/", 1)[-1])

    def sub_devs(self):
        resources = [res for res in self.svc.get_resources("disk.zpool") \
                     if res.name == self.pool]
        if resources:
            return resources[0].sub_devs()
        return set(zpool_devs(self.pool, self.svc.node))

    def exposed_devs(self):
        print(self.device)
        if os.path.exists(self.device):
            return set([self.device])
        return set()


    def provisioned(self):
        return self.has_it()

    def unprovisioner(self):
        if not self.has_it():
            self.log.info("zvol %s already destroyed", self.name)
            return
        cmd = [Env.syspaths.zfs, "destroy", "-f", self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.svc.node.unset_lazy("devtree")

    def provisioner(self):
        if self.has_it():
            self.log.info("zvol %s already exists", self.name)
            return
        cmd = [Env.syspaths.zfs, "create", "-V"]
        cmd += self.create_options
        cmd += [str(convert_size(self.size, _to="m"))+'M', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.can_rollback = True

        for i in range(3, 0, -1):
            if os.path.exists(self.device):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.log.error("timed out waiting for %s to appear" % self.device)
            raise ex.Error

        self.svc.node.unset_lazy("devtree")

