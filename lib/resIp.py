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

import resources as Res
from rcGlobalEnv import *

from subprocess import Popen, PIPE
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)
import socket
import rcStatus

class MissImpl(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpDevDown(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpConflict(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IpUnknown(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Ip(Res.Resource):
    """ basic ip resource
    """
    def __init__(self, ipDev=None, ipName=None, optional=False, disabled=False):
        Res.Resource.__init__(self, "ip", optional, disabled)
        self.ipDev=ipDev
        self.ipName=ipName
        self.id = 'ip ' + ipName + '@' + ipDev
        try:
            self.addr = socket.gethostbyname(ipName)
        except:
            self.log.error("could not resolve %s to an ip address")
            raise IpUnknown(self.ipName)

    def __str__(self):
        return "%s ipdev=%s ipname=%s" % (Res.Resource.__str__(self),\
                                         self.ipDev, self.ipName)
    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def check_ping(self):
        raise MissImpl('check_ping')

    def startip_cmd(self):
        raise MissImpl('startip_cmd')

    def stopip_cmd(self):
        raise MissImpl('stopip_cmd')

    def is_up(self):
        ifconfig = rcIfconfig.ifconfig()
        if ifconfig.has_param("ipaddr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def allow_start(self):
        ifconfig = rcIfconfig.ifconfig()
        if not ifconfig.interface(self.ipDev).flag_up:
            self.log.error("Interface %s is not up. Cannot stack over it." % self.ipDev)
            raise IpDevDown(self.ipDev)
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipDev))
            return False
        if self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise IpConflict(self.addr)
        return True

    def start(self):
        try:
            if not self.allow_start():
                return 0
        except (IpConflict, IpDevDown):
            return 1
        self.log.debug('pre-checks passed')

        ifconfig = rcIfconfig.ifconfig()
        self.mask = ifconfig.interface(self.ipDev).mask
        if self.mask == '':
            self.log.error("No netmask set on parent interface %s" % self.ipDev)
            return None
        if self.mask == '':
            self.log.error("No netmask found. Abort")
            return 1
        self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                    self.addr, self.log)
        (ret, out) = self.startip_cmd()
        if ret != 0:
            self.log.error("failed")
            return 1
        return 0

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already down on %s" % (self.addr, self.ipDev))
            return 0
        ifconfig = rcIfconfig.ifconfig()
        self.stacked_dev = ifconfig.get_stacked_dev(self.ipDev,\
                                                                self.addr,\
                                                                self.log)
        (ret, out) = self.stopip_cmd()
        if ret != 0:
            self.log.error("failed")
            return 1
        return 0

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

    print """i1=Ip("eth0","192.168.0.173")"""
    i=Ip("eth0","192.168.0.173")
    print "show i", i
    print """i.do_action("start")"""
    i.do_action("start")

