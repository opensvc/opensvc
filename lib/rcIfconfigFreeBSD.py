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
        prev = ''
        prevprev = ''
        for w in out.split():
            if 'flags=' in w:
                i = rcIfconfig.interface(prev.replace(':',''))
                self.intf.append(i)

                # defaults
                i.link_encap = ''
                i.scope = ''
                i.bcast = ''
                i.mtu = ''
                i.ipaddr = []
                i.mask = []
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False

                flags = w.split('<')[1].split('>')[0].split(',')
                if 'UP' in flags:
                    i.flag_up = True
                if 'BROADCAST' in flags:
                    i.flag_broadcast = True
                if 'RUNNING' in flags:
                    i.flag_running = True
                if 'MULTICAST' in flags:
                    i.flag_multicast = True
                if 'LOOPBACK' in flags:
                    i.flag_loopback = True
            elif 'inet' == prev:
                i.ipaddr += [w]
            elif 'inet6' == prev:
                i.ip6addr += [w.split('%')[0]]
            elif 'prefixlen' == [prev]:
                i.ip6mask += [w]
            elif 'ether' == prev:
                i.hwaddr = w

            prevprev = prev
            prev = w

    def __init__(self):
        self.intf = []
        out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]
        self.parse(out)
