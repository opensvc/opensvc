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

class Ip(Res.Ip):
    def check_ping(self):
        count=1
        timeout=5
        cmd = ['ping', '-c', repr(count), '-W', repr(timeout), '-w', repr(timeout), self.addr]
        (ret, out) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def startip_cmd(self):
        cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', self.mask, 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        cmd = ['ifconfig', self.stacked_dev, 'down']
        return self.vcall(cmd)



if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

