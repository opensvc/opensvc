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
"Module implement Linux specific ip management"

import resIp as Res
u = __import__('rcUtilitiesHP-UX')

class Ip(Res.Ip):
    def check_ping(self):
        self.log.info("checking %s availability"%self.addr)
        return u.check_ping(self.addr)

    def arp_announce(self):
        """ arp_announce job is done by HP-UX ifconfig... """
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipDev, 'inet6', 'up']
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.excError
            cmd = ['ifconfig', self.stacked_dev, 'inet6', self.addr+'/'+self.mask, 'up']
        else:
            cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', self.mask, 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.stacked_dev, "inet6", "::"]
        else:
            cmd = ['ifconfig', self.stacked_dev, "0.0.0.0"]
        return self.vcall(cmd)



if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

