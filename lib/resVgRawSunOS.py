#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import resVgRaw
import os
import rcStatus
import re

class Vg(resVgRaw.Vg):
    def __init__(self, rid=None, devs=set([]), type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False, restart=0):
        
        resVgRaw.Vg.__init__(self, rid=rid,
                             devs=devs,
                             type=type,
                             optional=optional,
                             disabled=disabled,
                             tags=tags,
                             always_on=always_on,
                             monitor=monitor,
                             restart=restart)

        self.devs = set([])
        self.devs_not_found = set([])

        for dev in devs:
            if re.match("^.*[sp][0-9]*$", dev) is not None:
                # partition, substitute s2 to given part
                regex = re.compile("[sp][0-9]*$", re.UNICODE)
                dev = regex.sub("s2", dev)
            else:
                # base device, append s2
                dev += 's2'

            if os.path.exists(dev):
                self.devs.add(dev)
            else:
                self.devs_not_found.add(dev)

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

