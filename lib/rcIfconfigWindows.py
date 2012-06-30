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

class ifconfig(rcIfconfig.ifconfig):
    def parse(self, out):
        for line in out.split('\n'):
	    if len(line) == 0:
	        continue
            if line.startswith('Ethernet'):
                i = rcIfconfig.interface("")
                self.intf.append(i)

                # defaults
                i.link_encap = ''
                i.scope = ''
                i.bcast = ''
                i.mask = ''
                i.mtu = ''
                i.ipaddr = ''
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False
            elif 'Physical Address' in line:
                i.hwaddr = line.split(':')[-1].strip().replace(':',':')
            elif 'Mask:' in line:
                i.mask = line.split(':')[-1].strip()
            if 'IP Address' in line:
                i.ipaddr = line.split(':')[-1].strip()
            if 'IPv6 Address' in line:
                ip6addr = line.split(': ')[-1].strip().replace('(Preferred)','')
		ip6addr = ip6addr.split('%')[0]
                i.ip6addr += [ip6addr]
                i.ip6mask += ['']

    def __init__(self):
        self.intf = []
        out = Popen(['ipconfig', '/all'], stdout=PIPE).communicate()[0]
        self.parse(out)
