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
import rcExceptions as ex
from subprocess import *
from rcGlobalEnv import rcEnv
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(Res.Ip):
    def __init__(self, rid=None, ipDev=None, ipName=None,
                 mask=None, always_on=set([]), monitor=False,
                 disabled=False, tags=set([]), optional=False):
        Res.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                        mask=mask, always_on=always_on,
                        disabled=disabled, tags=tags, optional=optional,
                        monitor=monitor)
        if 'exclusive' not in self.tags:
            self.tags.add('preboot')

    def get_ifconfig(self):
        if 'exclusive' in self.tags:
            out = Popen(['zlogin', self.svc.vmname, 'ifconfig', '-a'],
                        stdin=None, stdout=PIPE, stderr=PIPE,
                        close_fds=True).communicate()[0]
            return rcIfconfig.ifconfig(out)
        else:
            return rcIfconfig.ifconfig()

    def startip_cmd(self):
        if 'exclusive' in self.tags:
            if 'actions' in self.tags:
                return self.startip_cmd_exclusive()
            else:
                raise ex.excNotSupported()
        else:
            return self.startip_cmd_shared()

    def stopip_cmd(self):
        if 'exclusive' in self.tags:
            if 'actions' in self.tags:
                return self.stopip_cmd_exclusive()
            else:
                raise ex.excNotSupported()
        else:
            return self.stopip_cmd_shared()

    def stopip_cmd_exclusive(self):
        cmd=['zlogin', self.svc.vmname, 'ifconfig', self.stacked_dev, 'unplumb' ]
        return self.vcall(cmd)

    def startip_cmd_exclusive(self):
        cmd=['zlogin', self.svc.vmname, 'ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up' ]
        return self.vcall(cmd)

    def startip_cmd_shared(self):
        cmd=['ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up' , 'zone' , self.svc.vmname ]
        return self.vcall(cmd)

    def stopip_cmd_shared(self):
        cmd=['ifconfig', self.stacked_dev, 'unplumb']
        return self.vcall(cmd)

    def allow_start(self):
        if 'exclusive' in self.tags:
            if 'actions' not in self.tags:
                raise ex.IpNoActions(self.addr)
            retry = 10
            interval = 3
        else:
            retry = 1
            interval = 0
        import time
        ok = False
        if 'noalias' not in self.tags:
            for i in range(retry):
                if 'exclusive' in self.tags:
                    ifconfig = self.get_ifconfig()
                else:
                    ifconfig = rcIfconfig.ifconfig()
                intf = ifconfig.interface(self.ipDev)
                if intf is not None and intf.flag_up:
                    ok = True
                    break
                time.sleep(interval)
            if not ok:
                self.log.error("Interface %s is not up. Cannot stack over it." % self.ipDev)
                raise ex.IpDevDown(self.ipDev)
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipDev))
            raise ex.IpAlreadyUp(self.addr)
        if 'nonrouted' not in self.tags and self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)
        return

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

