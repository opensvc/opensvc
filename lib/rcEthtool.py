# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

from rcUtilities import justcall, which

"""
Settings for eth0:
        Supported ports: [ TP ]
        Supported link modes:   1000baseT/Full
                                10000baseT/Full
        Supports auto-negotiation: Yes
        Advertised link modes:  1000baseT/Full
                                10000baseT/Full
        Advertised pause frame use: No
        Advertised auto-negotiation: No
        Speed: 5000Mb/s
        Duplex: Full
        Port: Twisted Pair
        PHYAD: 0
        Transceiver: internal
        Auto-negotiation: on
        MDI-X: Unknown
        Supports Wake-on: g
        Wake-on: d
        Link detected: yes
"""

class LoadError(Exception):
    pass

class Ethtool(object):
    def __init__(self, intf):
        self.intf = intf
        self.data = {}

    def __getattr__(self, attr):
        if len(self.data) == 0:
            self.load()
        if attr not in self.data:
            return None
        return self.data[attr]

    def load(self):
        if not which('ethtool'):
            raise LoadError("ethtool is not installed")
        cmd = ['ethtool', self.intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise LoadError("ret=%d\nout=%s\nerr=%s\n"%(ret, out, err))
        for line in out.split('\n'):
            if not line.startswith('\t'):
                continue
            l = line.split(': ')
            if len(l) != 2:
                continue
            param = l[0].strip().replace(" ", "_").replace("-", "_").lower()
            value = l[1].strip()
            self.data[param] = value

if __name__ == "__main__":
    o = Ethtool("eth0")
    o.load()
    for attr in o.data.keys():
        print("%-30s: %s"%(attr, str(o.data[attr])))

