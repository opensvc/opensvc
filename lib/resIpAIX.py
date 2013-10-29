#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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
"Module implement Linux specific ip management"

import resIp as Res
import rcExceptions as ex
from rcUtilitiesAIX import check_ping

class Ip(Res.Ip):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        return

    def startip_cmd(self):
        if ':' in self.addr:
            if '.' in self.mask:
                self.log.error("netmask parameter is mandatory for ipv6 adresses")
                raise ex.excError
            cmd = ['ifconfig', self.ipDev, 'inet6', 'alias', '/'.join([self.addr, self.mask])]
        else:
            cmd = ['ifconfig', self.ipDev, self.addr, 'netmask', self.mask, 'alias']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipDev, 'inet6', self.addr, 'delete']
        else:
            cmd = ['ifconfig', self.ipDev, self.addr, 'delete']
        return self.vcall(cmd)



if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

