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

from subprocess import *

import rcIfconfig
import wmi

class ifconfig(rcIfconfig.ifconfig):
    def parse(self, intf):
        i = rcIfconfig.interface(intf.Caption)
        self.intf.append(i)

        # defaults
        i.link_encap = ''
        i.scope = ''
        i.bcast = ''
        i.mask = []
        i.mtu = intf.MTU
        i.ipaddr = []
        i.ip6addr = []
        i.ip6mask = []
        i.hwaddr = intf.MACAddress
        i.flag_up = False
        i.flag_broadcast = False
        i.flag_running = False
        i.flag_multicast = False
        i.flag_loopback = False

        for idx, ip in enumerate(intf.IPAddress):
	    if ":" in ip:
	        i.ip6addr.append(ip)
	        i.ip6mask.append(intf.IPsubnet[idx])
	    else:
	        i.ipaddr.append(ip)
	        i.mask.append(intf.IPsubnet[idx])

    def __init__(self):
        self.wmi = wmi.WMI()
        self.intf = []
        for i in self.wmi.Win32_NetworkAdapterConfiguration(IPEnabled=1):
            self.parse(i)

