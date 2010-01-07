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

import rcIfconfig

class ifconfig(rcIfconfig.ifconfig):
    def parse(self, out):
        if len(out) == 0:
            return
        intf = out.split()[0]
        if intf[len(intf)-1] == ':':
            intf = intf[0:len(intf)-1]

        i = rcIfconfig.interface(intf)
        self.intf.append(i)

        # defaults
        i.link_encap = ''
        i.scope = ''
        i.bcast = ''
        i.mask = ''
        i.mtu = ''
        i.ipaddr = ''
        i.ip6addr = ''
        i.hwaddr = ''
        i.flag_up = False
        i.flag_broadcast = False
        i.flag_running = False
        i.flag_multicast = False
        i.flag_loopback = False

        prev = ''
        for w in out.split():
            if 'broadcast' in prev:
                i.bcast = w
            elif 'netmask' in prev:
                i.mask = "%d.%d.%d.%d"%(
                    int(w[0:2], 16),
                    int(w[2:4], 16),
                    int(w[4:6], 16),
                    int(w[6:8], 16)
                )
            elif 'inet' in prev:
                i.ipaddr = w
            elif 'inet6' in prev:
                i.ip6addr = w

            if 'UP' in w:
                i.flag_up = True
            if 'BROADCAST' in w:
                i.flag_broadcast = True
            if 'RUNNING' in w:
                i.flag_running = True
            if 'MULTICAST' in w:
                i.flag_multicast = True
            if 'LOOPBACK' in w:
                i.flag_loopback = True

            prev = w

    def __init__(self):
        intf_list = Popen(['netstat', '-win'], stdout=PIPE).communicate()[0]
        for line in intf_list.split('\n'):
            if len(line) == 0:
                continue
            intf = line.split()[0]
            if intf[len(intf)-1] == ':':
                intf = intf[0:len(intf)-1]
            out = Popen(['ifconfig', intf], stdout=PIPE, stderr=PIPE).communicate()
            if "no such interface" in out[1]:
                continue
            self.parse(out[0])
