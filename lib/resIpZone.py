#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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
# To change this template, choose Tools | Templates
# and open the template in the editor.
"Module implement SunOS specific ip management"

import resIpSunOS as Res
from subprocess import *

class Ip(Res.Ip):
    def __init__(self, rid=None, ipDev=None, ipName=None,
                 always_on=set([])):
        Res.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                        always_on=always_on)

    def startip_cmd(self):
        cmd=['ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up' , 'zone' , self.vmname ]
        return self.vcall(cmd)


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

