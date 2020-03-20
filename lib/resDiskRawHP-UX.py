import resDiskRaw
import os
import rcStatus
import re
from rcUtilities import justcall
from svcdict import KEYS

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "raw"
KEYWORDS = resDiskRaw.KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    resDiskRaw.adder(svc, s, drv=Disk)


class Disk(resDiskRaw.Disk):
    def __init__(self,
                 rid=None,
                 devs=set(),
                 user=None,
                 group=None,
                 perm=None,
                 create_char_devices=False,
                 **kwargs):

        resDiskRaw.Disk.__init__(self,
                             rid=rid,
                             devs=devs,
                             user=user,
                             group=group,
                             perm=perm,
                             create_char_devices=False,
                             **kwargs)

    def verify_dev(self, dev):
        cmd = ["diskinfo", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True
