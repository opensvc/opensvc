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

import resIp as Res
from subprocess import *
from rcUtilitiesSunOS import check_ping
import rcExceptions as ex

class Ip(Res.Ip):
    """ define ip SunOS start/stop doAction """

    def arp_announce(self):
        """ arp_announce job is done by SunOS ifconfig... """
        return

    def check_ping(self, count=1, timeout=2):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, timeout=timeout)

    def startip_cmd(self):
        cmd=['/usr/sbin/ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        cmd = ['/usr/sbin/ifconfig', self.stacked_dev, 'unplumb']
        return self.vcall(cmd)

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

