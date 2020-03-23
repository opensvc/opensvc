import json
import time

import rcExceptions as ex

from .. import BASE_KEYWORDS
from rcColor import format_str_flat_json
from rcGlobalEnv import rcEnv
from rcUtilities import lazy
from resources import Resource
from svcdict import KEYS

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

    def __init__(self, rid=None, **kwargs):
        super().__init__(rid, "disk.disk", **kwargs)

    def on_add(self):
        self.set_label()

    @lazy
    def disk_id(self):
        try:
            return self.conf_get("disk_id").strip()
        except ex.OptNotFound as exc:
            return

    def set_label(self):
        if self.disk_id is None:
            self.label = "unprovisioned disk"
        else:
            self.label = "disk "+str(self.disk_id)

    def _info(self):
        return [
            ["disk_id", self.disk_id],
        ]

    def __str__(self):
        return "%s disk disk_id=%s" % (super().__str__(), str(self.disk_id))

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
        poolname = self.oget("pool")
        name = self.oget("name")
        size = self.oget("size")
        pool = self.svc.node.get_pool(poolname)
        pool.log = self.log
        if self.shared:
            disk_id_kw = "disk_id"
            result = pool.create_disk(name, size=size, nodes=self.svc.nodes)
        else:
            disk_id_kw = "disk_id@" + rcEnv.nodename
            name += "." + rcEnv.nodename
            result = pool.create_disk(name, size=size, nodes=[rcEnv.nodename])
        if not result:
            raise ex.excError("invalid create disk result: %s" % result)
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
            raise ex.excError("no disk id found in result")
        self.log.info("changes: %s", changes)
        self.svc.set_multi(changes, validation=False)
        self.log.info("changes applied")
        self.unset_lazy("disk_id")
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
        poolname = self.oget("pool")
        name = self.oget("name")
        pool = self.svc.node.get_pool(poolname)
        pool.log = self.log
        if self.shared:
            disk_id_kw = "disk_id"
        else:
            disk_id_kw = "disk_id@" + rcEnv.nodename
            name += "." + rcEnv.nodename
        result = pool.delete_disk(name=name, disk_id=self.disk_id)
        for line in format_str_flat_json(result).splitlines():
            self.log.info(line)
        self.svc.set_multi(["%s.%s=%s" % (self.rid, disk_id_kw, "")], validation=False)
        self.unset_lazy("disk_id")
        self.log.info("unprovisioned")

