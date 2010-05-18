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

import rcExceptions as ex
import rcStatus
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv
import resIp as Res

class Ip(Res.Ip):
    def is_up(self):
        for vm in self.svc.get_res_sets("container."+self.svc.svcmode):
            pass
        if vm.status() == rcStatus.DOWN:
            self.log.debug("container is down")
            self.status_log("container is down")
            return False
        rcIfconfig = __import__("rcIfconfig"+self.svc.guestos+self.svc.svcmode)
        try:
            ifconfig = rcIfconfig.ifconfig(self.svc.vmname)
        except:
            self.log.error("failed to fetch interface configuration")
            return False
        if ifconfig.has_param("ipaddr", self.addr) is not None or \
           ifconfig.has_param("ip6addr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def allow_start(self):
        for vm in self.svc.get_res_sets("container."+self.svc.svcmode):
            pass
        if self.check_ping() and vm.status() == rcStatus.DOWN:
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)
        return

    def start(self):
        try:
            self.allow_start()
        except ex.IpConflict:
            raise ex.excError

    def stop(self):
        pass

    def __init__(self, rid=None, ipDev=None, ipName=None,
                 mask=None, always_on=set([]),
                 disabled=False, optional=False):
        Res.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                        mask=mask, always_on=always_on,
                        disabled=disabled, optional=optional)


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

