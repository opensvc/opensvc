from __future__ import print_function

import os
import re

import rcStatus
import resDiskRaw

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "raw"
KEYWORDS = resDiskRaw.KEYWORDS


def adder(svc, s):
    resDiskRaw.adder(svc, s, drv=Disk)


class Disk(resDiskRaw.Disk):
    def sub_disks(self):
        devs = self.sub_devs()
        l = set()
        for dev in devs:
            if re.match("^/dev/rdsk/c[0-9]*", dev) is None:
                continue
            if not os.path.exists(dev):
                continue

            if re.match('^.*s[0-9]*$', dev) is None:
                dev += "s2"
            else:
                regex = re.compile('s[0-9]*$', re.UNICODE)
                dev = regex.sub('s2', dev)

            l.add(dev)
        return l

    def sub_devs(self):
        self.validate_devs()
        l = set()
        for dev in self.devs:
            if not os.path.exists(dev):
                continue
            if os.path.islink(dev) and not dev.startswith("/devices"):
                dev = os.path.realpath(dev)
            l.add(dev)
        return l

