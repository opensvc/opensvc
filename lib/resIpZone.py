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

import time
import resIpSunOS as Res
import rcExceptions as ex
from subprocess import *
from rcGlobalEnv import rcEnv
from rcUtilitiesSunOS import get_os_ver
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(Res.Ip):
    def __init__(self, rid=None, ipDev=None, ipName=None, zone=None,
                 mask=None, always_on=set([]), monitor=False,
                 disabled=False, tags=set([]), optional=False, gateway=None):
        Res.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                        mask=mask, always_on=always_on,
                        disabled=disabled, tags=tags, optional=optional,
                        monitor=monitor, gateway=gateway)
        self.type = "ip.zone"
        self.zone = zone
        self.osver = get_os_ver()
        if self.osver >= 11.0:
            self.tags.add('noalias')
        if 'exclusive' not in self.tags:
            self.tags.add('preboot')

    def get_ifconfig(self):
        if 'exclusive' in self.tags:
            out = Popen(['zlogin', self.zone, 'ifconfig', '-a'],
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
        if self.osver >= 11.0:
            return self._stopip_cmd_exclusive_11()
        else:
            return self._stopip_cmd_exclusive_10()

    def _stopip_cmd_exclusive_10(self):
        cmd=['zlogin', self.zone, 'ifconfig', self.stacked_dev, 'unplumb' ]
        return self.vcall(cmd)

    def _stopip_cmd_exclusive_11(self):
        ret,out,err = (0, '', '')
        if self.gateway is not None:
            cmd=['zlogin', self.zone, 'route', '-q', 'delete', 'default', self.gateway, '>/dev/null', '2>&1' ]
            r,o,e = self.vcall(cmd)
            ret += r
            out += o
            err += e
        objext = self.rid.replace('#', '')
        cmd=['zlogin', self.zone, 'ipadm', 'delete-addr', self.stacked_dev+'/v4osvcres'+objext ]
        r,o,e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        tst = Popen(['zlogin', self.zone, 'ipadm', 'show-addr', self.stacked_dev+'/' ],
                        stdin=None, stdout=PIPE, stderr=None, close_fds=True).communicate()[0].split("\n")
        if len(tst) >= 2:
            return (ret,out,err)
        cmd=['zlogin', self.zone, 'ipadm', 'delete-ip', self.stacked_dev ]
        r,o,e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        return (ret,out,err)

    def startip_cmd_exclusive(self):
        if self.osver >= 11.0:
            return self._startip_cmd_exclusive_11()
        else:
            return self._startip_cmd_exclusive_10()

    def _startip_cmd_exclusive_10(self):
        cmd=['zlogin', self.zone, 'ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up' ]
        return self.vcall(cmd)

    def _hexmask_to_str(self, mask):
        mask = mask.replace('0x', '')
        s = [str(int(mask[i:i+2], 16)) for i in range(0, len(mask), 2)]
        return '.'.join(s)

    def _dotted2cidr(self):
        if self.mask is None:
            return ''
        cnt = 0
        if '.' in self.mask:
            l = self.mask.split(".")
        else:
            l = self._hexmask_to_str(self.mask).split(".")
        for v in l:
            b = int(v)
            while b != 0:
                if b % 2 == 1:
                    cnt = cnt + 1
                    b = b / 2
        return '/'+str(cnt)
        
    def _startip_cmd_exclusive_11(self):
        ret,out,err = (0, '', '')
        tst = Popen(['zlogin', self.zone, 'ipadm', 'show-if', self.stacked_dev, '2>/dev/null' ],
                        stdin=None, stdout=PIPE, stderr=None, close_fds=True).communicate()[0].split("\n")
        if len(tst) < 2:
            cmd=['zlogin', self.zone, 'ipadm', 'create-ip', '-t', self.stacked_dev ]
            r,o,e = self.vcall(cmd)
        objext = self.rid.replace('#', '')
        n = 5
        while n != 0:
            cmd=['zlogin', self.zone, 'ipadm', 'create-addr', '-t', '-T', 'static', '-a', self.addr+self._dotted2cidr(), self.stacked_dev+'/v4osvcres'+objext ]
            r,o,e = self.vcall(cmd)
            if r == 0:
                break
            self.log.error("Interface %s is not up. ipadm cannot create-addr over it. Retrying..." % self.stacked_dev)
            time.sleep(5)
            n -= 1
        if r != 0:
            cmd=['zlogin', self.zone, 'ipadm', 'show-if' ]
            self.vcall(cmd)
        ret += r
        out += o
        err += e
        if self.gateway is not None:
            cmd=['zlogin', self.zone, 'route', '-q', 'add', 'default', self.gateway, '>/dev/null', '2>&1' ]
            r,o,e = self.vcall(cmd)
            ret += r
            out += o
            err += e
        return (ret,out,err)

    def startip_cmd_shared(self):
        cmd=['ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up' , 'zone' , self.zone ]
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

