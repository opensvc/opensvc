#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
from rcUtilities import which
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(Res.Ip):
    def __init__(self, rid=None, ipDev=None, ipName=None,
                 mask=None, always_on=set([]), monitor=False,
                 disabled=False, tags=set([]), optional=False, gateway=None,
                 ipDevExt="v4"):
        self.ipDevExt = ipDevExt
        Res.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                        mask=mask, always_on=always_on,
                        disabled=disabled, tags=tags, optional=optional,
                        monitor=monitor, gateway=gateway)
        self.label = self.label + "/" + self.ipDevExt
        self.type = "ip"
        if not which('ipadm'):
            raise ex.excInitError("crossbow ips are not supported on this system")
        if 'noalias' not in self.tags:
            self.tags.add('noalias')

    def stopip_cmd(self):
        ret, out, err = (0, '', '')
        if self.gateway is not None:
            cmd=['route', '-q', 'delete', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        cmd=['ipadm', 'delete-addr', self.stacked_dev+'/'+self.ipDevExt]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        cmd = ['ipadm', 'show-addr', self.stacked_dev+'/'+self.ipDevExt ]
        p = Popen(cmd, stdin=None, stdout=PIPE, stderr=PIPE, close_fds=True)
        _out = p.communicate()[0].split("\n")
        if len(_out) >= 2:
            return ret, out, err
        cmd=['ipadm', 'delete-ip', self.stacked_dev]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        return ret, out, err

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
        
    def startip_cmd(self):
        ret, out, err = (0, '', '')
        cmd = ['ipadm', 'show-if', self.stacked_dev]
        p = Popen(cmd, stdin=None, stdout=PIPE, stderr=PIPE, close_fds=True)
        _out = p.communicate()[0].split("\n")
        if len(_out) < 2:
            cmd=['ipadm', 'create-ip', '-t', self.stacked_dev ]
            r, o, e = self.vcall(cmd)
        n = 5
        while n != 0:
            cmd=['ipadm', 'create-addr', '-t', '-T', 'static', '-a', self.addr+self._dotted2cidr(), self.stacked_dev+'/'+self.ipDevExt]
            r, o, e = self.vcall(cmd)
            if r == 0:
                break
            self.log.error("Interface %s is not up. ipadm cannot create-addr over it. Retrying..." % self.stacked_dev)
            time.sleep(5)
            n -= 1
        if r != 0:
            cmd=['ipadm', 'show-if' ]
            self.vcall(cmd)
        ret += r
        out += o
        err += e
        if self.gateway is not None:
            cmd=['route', '-q', 'add', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        return ret, out, err

    def allow_start(self):
        if 'noaction' in self.tags:
            raise ex.IpNoActions(self.addr)
        retry = 10
        interval = 3
        import time
        ok = False
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipDev))
            raise ex.IpAlreadyUp(self.addr)
        if 'nonrouted' not in self.tags and self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

