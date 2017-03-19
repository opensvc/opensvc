from __future__ import print_function

import os
import re

import rcStatus
import resDiskRaw

class Disk(resDiskRaw.Disk):
    def disklist(self):
        devs = self.devlist()
        l = set([])
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

    def devlist(self):
        self.validate_devs()
        l = set([])
        for dev in self.devs:
            if not os.path.exists(dev):
                continue
            if os.path.islink(dev) and not dev.startswith("/dev"):
                dev = os.readlink(dev)
            l.add(dev)
        return l

