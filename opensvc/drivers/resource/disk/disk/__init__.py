import json
import time

import core.exceptions as ex

from .. import BASE_KEYWORDS
from env import Env
from utilities.lazy import lazy
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.render.color import format_str_flat_json

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "disk"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "size",
        "provisioning": True,
        "at": True,
        "convert": "size",
        "text": "A size expression for the disk allocation.",
        "example": "20g"
    },
    {
        "keyword": "pool",
        "provisioning": True,
        "at": True,
        "text": "The name of the pool this volume was allocated from.",
    },
    {
        "keyword": "name",
        "provisioning": True,
        "at": True,
        "text": "The name of the disk.",
    },
    {
        "keyword": "disk_id",
        "at": True,
        "text": "The wwn of the disk.",
        "example": "6589cfc00000097484f0728d8b2118a6"
    },
    {
        "keyword": "array",
        "at": True,
        "provisioning": True,
        "text": "The array to provision the disk from.",
        "example": "xtremio-prod1"
    },
    {
        "keyword": "diskgroup",
        "at": True,
        "provisioning": True,
        "text": "The array disk group to provision the disk from.",
        "example": "default"
    },
    {
        "keyword": "slo",
        "at": True,
        "provisioning": True,
        "text": "The provisioned disk service level objective. This keyword is honored on arrays supporting this (ex: EMC VMAX)",
        "example": "Optimized"
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class DiskDisk(Resource):
    """
    SAN Disk resource
    """

    def __init__(self, name=None, size=None, pool=None, disk_id=None, array=None, diskgroup=None, slo=None, **kwargs):
        super(DiskDisk, self).__init__(type="disk.disk", **kwargs)
        self.name = name
        self.size = size
        self.poolname = pool
        self.disk_id = disk_id
        self.array = array
        self.diskgroup = diskgroup
        self.slo = slo

    def __str__(self):
        return "%s disk disk_id=%s" % (
            super(DiskDisk, self).__str__(),
            self.disk_id
        )

    def on_add(self):
        self.set_label()

    def set_label(self):
        if self.disk_id is None:
            self.label = "unprovisioned disk"
        else:
            self.label = "disk "+str(self.disk_id)

    def _info(self):
        return [
            ["disk_id", self.disk_id],
        ]

    def configure(self, force=False):
        # OS specific
        pass

    def unconfigure(self):
        # OS specific
        pass

    def provisioned(self):
        if self.disk_id:
            return True
        return False

    def provisioner(self):
        if self.disk_id:
            self.log.info("skip disk creation: the disk_id keyword is already set")
            self.configure()
        else:
            self.create_disk()
            self.configure(force=True)

    def create_disk(self):
        pool = self.svc.node.get_pool(self.poolname)
        pool.log = self.log
        if self.shared:
            disk_id_kw = "disk_id"
            result = pool.create_disk(self.name, size=self.size, nodes=self.svc.nodes)
        else:
            disk_id_kw = "disk_id@" + Env.nodename
            name = self.name + "." + Env.nodename
            result = pool.create_disk(name, size=self.size, nodes=[Env.nodename])
        if not result:
            raise ex.Error("invalid create disk result: %s" % result)
        for line in format_str_flat_json(result).splitlines():
            self.log.info(line)
        changes = []
        if "disk_ids" in result:
            for node, disk_id in result["disk_ids"].items():
                changes.append("%s.disk_id@%s=%s" % (self.rid, node, disk_id))
        elif "disk_id" in result:
            disk_id = result["disk_id"]
            changes.append("%s.%s=%s" % (self.rid, disk_id_kw, disk_id))
        else:
            raise ex.Error("no disk id found in result")
        self.log.info("changes: %s", changes)
        self.svc.set_multi(changes, validation=False)
        self.log.info("changes applied")
        self.disk_id = self.oget("disk_id")
        self.log.info("disk %s provisioned" % result["disk_id"])

    def provisioner_shared_non_leader(self):
        self.configure()

    def unprovisioner_shared_non_leader(self):
        self.unconfigure()

    def unprovisioner(self):
        if not self.disk_id:
            self.log.info("skip unprovision: 'disk_id' is not set")
            return
        self.unconfigure()
        pool = self.svc.node.get_pool(self.poolname)
        pool.log = self.log
        if self.shared:
            disk_id_kw = "disk_id"
            name = self.name
        else:
            disk_id_kw = "disk_id@" + Env.nodename
            name = self.name + "." + Env.nodename
        result = pool.delete_disk(name=name, disk_id=self.disk_id)
        for line in format_str_flat_json(result).splitlines():
            self.log.info(line)
        self.svc.set_multi(["%s.%s=%s" % (self.rid, disk_id_kw, "")], validation=False)
        self.log.info("unprovisioned")

