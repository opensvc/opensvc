import resDiskRaw
import os
import rcStatus
import re
from rcUtilities import justcall

class Disk(resDiskRaw.Disk):
    def __init__(self,
                 rid=None,
                 devs=set([]),
                 user=None,
                 group=None,
                 perm=None,
                 create_char_devices=False,
                 type=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        
        resDiskRaw.Disk.__init__(self,
                             rid=rid,
                             devs=devs,
                             user=user,
                             group=group,
                             perm=perm,
                             create_char_devices=False,
                             type=type,
                             optional=optional,
                             disabled=disabled,
                             tags=tags,
                             always_on=always_on,
                             monitor=monitor,
                             restart=restart,
                             subset=subset)

    def verify_dev(self, dev):
        cmd = ["diskinfo", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True
