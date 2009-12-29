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
"Module implement Linux/LXC specific ip management"

import rcExceptions as ex
import resIpLinux as Res
import resIp
import rcIfconfigLinuxlxc as rcIfconfig
import rcStatus
from subprocess import *

class Ip(Res.Ip):
    def is_up(self):
        for lxc in self.svc.get_res_sets("container.lxc"):
            pass
        if lxc.status() == rcStatus.DOWN:
            return False
        ifconfig = rcIfconfig.ifconfig(self.lxcname)
        if ifconfig.has_param("ipaddr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def start(self):
        try:
            self.allow_start()
        except (resIp.IpConflict, resIp.IpDevDown):
            raise ex.excError
        except resIp.IpAlreadyUp:
            return
        self.log.debug('pre-checks passed')

    def __init__(self, lxcname, ipdev, ipname):
        Res.Ip.__init__(self, ipdev, ipname)
        self.lxcname = lxcname


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

