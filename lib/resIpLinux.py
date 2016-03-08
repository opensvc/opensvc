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
import rcExceptions as ex
from rcUtilitiesLinux import check_ping, which

class Ip(Res.Ip):
    def check_ping(self, timeout=5, count=1):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, timeout=timeout, count=count)

    def start_link(self):
        if which("ip"):
           cmd = ['ip', 'link', 'set', 'dev', self.ipDev, 'up']
        else:
           cmd = ['ifconfig', self.ipDev, 'up']
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

    def startip_cmd(self):
        if ':' in self.addr:
            if '.' in self.mask:
                self.log.error("netmask parameter is mandatory for ipv6 adresses")
                raise ex.excError
            cmd = ['ifconfig', self.ipDev, 'inet6', 'add', '/'.join([self.addr, self.mask])]
        else:
            cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', self.mask, 'up']

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # ip activation may still be incomplete
        # wait for activation, to avoid startapp scripts to fail binding their listeners
        for i in range(5, 0, -1):
            if check_ping(self.addr, timeout=1, count=1):
                return ret, out, err
        self.log.error("timed out waiting for ip activation")
        raise ex.excError

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipDev, 'inet6', 'del', '/'.join([self.addr, self.mask])]
        else:
            if self.stacked_dev is None:
                return 1, "", "no stacked dev found"
            cmd = ['ifconfig', self.stacked_dev, 'down']
        return self.vcall(cmd)



if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

