import resDiskRaw
import os
import rcStatus
import re

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

        devs = list(devs)
        for i, dev in enumerate(devs):
            if ":" in dev:
                suffix = dev[dev.index(":"):]
                dev = dev[0:dev.index(":")]
            else:
                suffix = ""
            if re.match("^.*[sp][0-9]*$", dev) is not None:
                # partition, substitute s2 to given part
                regex = re.compile("[sp][0-9]*$", re.UNICODE)
                dev = regex.sub("s2", dev)
            else:
                # base device, append s2
                dev += 's2'
            devs[i] = dev + suffix

        resDiskRaw.Disk.__init__(self,
                             rid=rid,
                             devs=set(devs),
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

    def disklist(self):
        l = set([])
        for dev in self.devs:
            if re.match("^/dev/rdsk/c[0-9]*", dev) is not None:
                if os.path.exists(dev):
                    if re.match('^.*s[0-9]*$', dev) is None:
                        dev += "s2"
                    else:
                        regex = re.compile('s[0-9]*$', re.UNICODE)
                        dev = regex.sub('s2', dev)
                    l.add(dev)
        return l

