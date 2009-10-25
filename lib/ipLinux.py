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

import ip
import socket
from subprocess import *

from rcLogger import *
import rcIfconfig
import rcStatus

def next_stacked_dev(dev, ifconfig):
    """Return the first available interfaceX:Y on  interfaceX
    """
    i = 0
    while True:
        stacked_dev = dev+':'+str(i)
        if not ifconfig.has_interface(stacked_dev):
            return stacked_dev
            break
        i = i + 1

def get_stacked_dev(dev, addr, log):
    """Upon start, a new interfaceX:Y will have to be assigned.
    Upon stop, the currently assigned interfaceX:Y will have to be
    found for ifconfig down
    """
    ifconfig = rcIfconfig.ifconfig()
    stacked_intf = ifconfig.has_param("ipaddr", addr)
    if stacked_intf is not None:
        if dev not in stacked_intf.name:
            log.error("%s is plumbed but not on %s" % (addr, dev))
            return
        stacked_dev = stacked_intf.name
        log.debug("found matching stacked device %s" % stacked_dev)
    else:
        stacked_dev = next_stacked_dev(dev, ifconfig)
        log.debug("allocate new stacked device %s" % stacked_dev)
    return stacked_dev

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

class Ip(ip.Ip):
    def check_ping(self):
        count=1
        timeout=5
        cmd = ['ping', '-c', repr(count), '-W', repr(timeout), self.addr]
        (ret, out) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_up(self):
        ifconfig = rcIfconfig.ifconfig()
        if ifconfig.has_param("ipaddr", self.addr) is not None:
            self.log.debug("%s@%s is up" % (self.addr, self.ipDev))
            return True
        self.log.debug("%s@%s is down" % (self.addr, self.ipDev))
        return False

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

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
        stacked_dev = get_stacked_dev(self.ipDev, self.addr, self.log)
        cmd = ['ifconfig', stacked_dev, self.addr, 'netmask', self.mask, 'up']
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed")
            return 1
        return 0

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already down on %s" % (self.addr, self.ipDev))
            return 0
        stacked_dev = get_stacked_dev(self.ipDev, self.addr, self.log)
        cmd = ['ifconfig', stacked_dev, 'down']
        self.log.info(' '.join(cmd))
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed")
            return 1
        return 0

    def __init__(self, ipDev, ipName):
        self.addr = socket.gethostbyname(ipName)
        ip.Ip.__init__(self, ipDev, ipName)

if __name__ == "__main__":
    for c in (next_stacked_dev,get_stacked_dev,IpDevDown,IpConflict,Ip,) :
        help(c)

