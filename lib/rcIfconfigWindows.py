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
    def parse(self, intf, intf_cf):
	if intf_cf.IPAddress is None:
	    return
        i = rcIfconfig.interface(intf.NetConnectionID)
        self.intf.append(i)

        # defaults
        i.link_encap = ''
        i.scope = ''
        i.bcast = ''
        i.mask = []
        i.mtu = intf_cf.MTU
        i.ipaddr = []
        i.ip6addr = []
        i.ip6mask = []
        i.hwaddr = intf_cf.MACAddress
        i.flag_up = intf.NetEnabled
        i.flag_broadcast = False
        i.flag_running = False
        i.flag_multicast = False
        i.flag_loopback = False

        for idx, ip in enumerate(intf_cf.IPAddress):
	    if ":" in ip:
	        i.ip6addr.append(ip)
	        i.ip6mask.append(intf_cf.IPsubnet[idx])
	    else:
	        i.ipaddr.append(ip)
	        i.mask.append(intf_cf.IPsubnet[idx])

    def __init__(self):
        self.wmi = wmi.WMI()
        self.intf = []
        for n, nc in zip(self.wmi.Win32_NetworkAdapter(), self.wmi.Win32_NetworkAdapterConfiguration()):
            self.parse(n, nc)

if __name__ == "__main__" :
    o = ifconfig()
    print(o)
