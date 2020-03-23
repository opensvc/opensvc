import json
import time

import rcExceptions as ex

from .. import BASE_KEYWORDS
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