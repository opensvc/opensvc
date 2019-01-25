from __future__ import print_function

import json
import time

import resources as Res
from rcGlobalEnv import rcEnv
from rcUtilities import lazy
import rcExceptions as ex

class Disk(Res.Resource):
    """
    SAN Disk resource
    """

    def __init__(self, rid=None, **kwargs):
        Res.Resource.__init__(self, rid, "disk.disk", **kwargs)

    def on_add(self):
        self.set_label()

    @lazy
    def disk_id(self):
        try:
            return self.conf_get("disk_id")
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
        return "%s disk disk_id=%s" % (
            Res.Resource.__str__(self),
            str(self.disk_id),
        )

