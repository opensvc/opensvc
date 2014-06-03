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
from subprocess import *
import logging
from rcGlobalEnv import *

class interface:
    def __str__(self):
        a = ['ifconfig %s:'%self.name]
        a += [' link_encap = ' + self.link_encap]
        a += [' scope = ' + str(self.scope)]
        a += [' bcast = ' + str(self.bcast)]
        a += [' mtu = ' + self.mtu]
        a += [' ipaddr = ' + str(self.ipaddr)]
        a += [' mask = ' + str(self.mask)]
        a += [' ip6addr = ' + str(self.ip6addr)]
        a += [' ip6mask = ' + str(self.ip6mask)]
        a += [' hwaddr = ' + self.hwaddr]
        a += [' flag_up = ' + str(self.flag_up)]
        a += [' flag_broadcast = ' + str(self.flag_broadcast)]
        a += [' flag_running = ' + str(self.flag_running)]
        a += [' flag_multicast = ' + str(self.flag_multicast)]
        a += [' flag_loopback = ' + str(self.flag_loopback)]
        return '\n'.join(a)

    def __init__(self, name):
        self.name = name
        # defaults
        self.link_encap = ''
        self.scope = ''
        self.bcast = ''
        self.mask = ''
        self.mtu = ''
        self.ipaddr = ''
        self.ip6addr = []
        self.ip6mask = []
        self.hwaddr = ''
        self.flag_up = False
        self.flag_broadcast = False
        self.flag_running = False
        self.flag_multicast = False
        self.flag_loopback = False

class ifconfig(object):
    def add_interface(self, name):
        i = interface(name)
        self.intf.append(i)

    def interface(self, name):
        for i in self.intf:
            if i.name == name:
                return i
        return None

    def has_interface(self, name):
        for i in self.intf:
            if i.name == name:
                return 1
        return 0

    def has_param(self, param, value):
        for i in self.intf:
            if isinstance(getattr(i, param), list):
                if value in getattr(i, param):
                    return i
            else:
                if getattr(i, param) == value:
                    return i
        return None

    def __str__(self):
        for intf in self.intf:
            print intf

    def __init__(self, mcast=False):
        self.intf = []
        self.mcast_data = {}

    def next_stacked_dev(self,dev):
        """Return the first available interfaceX:Y on  interfaceX
        """
        i = 1
        while True:
            stacked_dev = dev+':'+str(i)
            if not self.has_interface(stacked_dev):
                return stacked_dev
            i = i + 1

    def get_stacked_dev(self, dev, addr, log):
        """Upon start, a new interfaceX:Y will have to be assigned.
        Upon stop, the currently assigned interfaceX:Y will have to be
        found for ifconfig down
        """
        if ':' in addr:
            stacked_intf = self.has_param("ip6addr", addr)
        else:
            stacked_intf = self.has_param("ipaddr", addr)
        if stacked_intf is not None:
            if dev not in stacked_intf.name:
                log.error("%s is plumbed but not on %s" % (addr, dev))
                return
            stacked_dev = stacked_intf.name
            log.debug("found matching stacked device %s" % stacked_dev)
        else:
            stacked_dev = self.next_stacked_dev(dev)
            log.debug("allocate new stacked device %s" % stacked_dev)
        return stacked_dev


