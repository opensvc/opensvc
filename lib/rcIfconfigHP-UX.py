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
import rcExceptions as ex

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
        i.ip6addr = []
        i.ip6mask = []
        i.hwaddr = ''
        if i.name in self.hwaddr:
            i.hwaddr = self.hwaddr[i.name]
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
                if w == '0':
                    i.mask = "0.0.0.0"
                elif len(w) == 8:
                    i.mask = "%d.%d.%d.%d"%(
                        int(w[0:2], 16),
                        int(w[2:4], 16),
                        int(w[4:6], 16),
                        int(w[6:8], 16)
                    )
                else:
                    raise ex.excError("malformed ifconfig %s netmask: %s"%(intf, w))
            elif 'inet' == prev:
                i.ipaddr = w
            elif 'inet6' == prev:
                i.ip6addr += [w]
            elif 'prefix' == prev:
                i.ip6mask += [w]

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

    def __init__(self, hwaddr=False, mcast=False):
        self.intf = []
        intf_list = []
        self.hwaddr = {}
        if hwaddr:
            lines = Popen(['lanscan', '-i', '-a'], stdout=PIPE).communicate()[0].split('\n')
            for line in lines:
                l = line.split()
                if len(l) < 2:
                    continue
                mac = l[0].replace('0x','').lower()
                if len(mac) < 11:
                    continue
                mac_l = list(mac)
                for c in (10, 8, 6, 4, 2):
                    mac_l.insert(c, ':')
                self.hwaddr[l[1]] = ''.join(mac_l)
        out = Popen(['netstat', '-win'], stdout=PIPE).communicate()[0]
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            if 'IPv4:' in line or 'IPv6' in line:
                continue
            intf = line.split()[0]
            intf_list.append(intf.replace('*', ''))
        for intf in intf_list:
            p = Popen(['ifconfig', intf], stdout=PIPE, stderr=PIPE)
            out = p.communicate()
            if "no such interface" in out[1]:
                continue
            elif p.returncode != 0:
                raise ex.excError
            self.parse(out[0])
