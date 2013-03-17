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
from rcUtilities import which

import rcIfconfig

"""
ip addr:
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 16436 qdisc noqueue
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
...
4: eth0: <BROADCAST,MULTICAST,SLAVE,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast master bond0 qlen 1000
    link/ether 00:23:7d:a1:6f:96 brd ff:ff:ff:ff:ff:ff
6: sit0: <NOARP> mtu 1480 qdisc noop
    link/sit 0.0.0.0 brd 0.0.0.0
7: bond0: <BROADCAST,MULTICAST,MASTER,UP,LOWER_UP> mtu 1500 qdisc noqueue
    link/ether 00:23:7d:a1:6f:96 brd ff:ff:ff:ff:ff:ff
    inet 10.151.32.29/22 brd 10.151.35.255 scope global bond0
    inet 10.151.32.50/22 brd 10.151.35.255 scope global secondary bond0:1
    inet6 fe80::223:7dff:fea1:6f96/64 scope link
       valid_lft forever preferred_lft forever

"""

def octal_to_cidr(s):
    i = int(s)
    _in = ""
    _out = ""
    for i in range(i):
        _in += "1"
    for i in range(32-i):
        _in += "0"
    _out += str(int(_in[0:8], 2))+'.'
    _out += str(int(_in[8:16], 2))+'.'
    _out += str(int(_in[16:24], 2))+'.'
    _out += str(int(_in[24:32], 2))
    return _out

class ifconfig(rcIfconfig.ifconfig):
    def parse_ip(self, out):
        out = str(out)
        for line in out.split("\n"):
            if len(line) == 0:
                continue
            if line[0] != " " or "secondary" in line:
                if line[0] != " ":
                    i = rcIfconfig.interface(line.split()[1].strip(':'))
                    self.intf.append(i)
                    idx = len(self.intf) - 1
                if "secondary" in line:
                    i = rcIfconfig.interface(line.split()[-1])
                    self.intf.append(i)

                # defaults
                i.link_encap = ''
                i.scope = []
                i.bcast = []
                i.mask = []
                i.mtu = ''
                i.ipaddr = []
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False

            prev = ''
            _line = line.split()
            for w in _line:
                if 'link/' in w:
                    i.link_encap = w.split('/')[1]
                elif 'link/ether' == prev:
                    i.hwaddr = w
                elif 'mtu' == prev:
                    i.mtu = w
                elif w.startswith('<'):
                    w = w.strip('<').strip('>')
                    flags = w.split(',')
                    for w in flags:
                        if 'UP' == w:
                            i.flag_up = True
                        if 'BROADCAST' == w:
                            i.flag_broadcast = True
                        if 'RUNNING' == w:
                            i.flag_running = True
                        if 'MULTICAST' == w:
                            i.flag_multicast = True
                        if 'LOOPBACK' == w:
                            i.flag_loopback = True

                elif 'inet' == prev :
                    ipaddr, mask = w.split('/')
                    i.ipaddr += [ipaddr]
                    i.mask += [octal_to_cidr(mask)]
                elif 'inet6' == prev:
                    (ip6addr, ip6mask) = w.split('/')
                    i.ip6addr += [ip6addr]
                    i.ip6mask += [ip6mask]
                elif 'brd' == prev and 'inet' in line:
                    i.bcast += [w]
                elif 'scope' == prev and 'inet' in line:
                    i.scope += [w]
                elif 'secondary' == prev:
                    i = self.intf[idx]

                prev = w


    def parse_ifconfig(self, out):
        prev = ''
        prevprev = ''
        for w in out.split():
            if w == 'Link':
                i = rcIfconfig.interface(prev)
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
            elif 'encap:' in w:
                (null, i.link_encap) = w.split(':')
            elif 'Scope:' in w:
                (null, i.scope) = w.split(':')
            elif 'Bcast:' in w:
                (null, i.bcast) = w.split(':')
            elif 'Mask:' in w:
                (null, i.mask) = w.split(':')
            elif 'MTU:' in w:
                (null, i.mtu) = w.split(':')

            if 'inet' == prev and 'addr:' in w:
                (null, i.ipaddr) = w.split(':')
            if 'inet6' == prevprev and 'addr:' == prev:
                (ip6addr, ip6mask) = w.split('/')
                i.ip6addr += [ip6addr]
                i.ip6mask += [ip6mask]
            if 'HWaddr' == prev:
                i.hwaddr = w
            if 'UP' == w:
                i.flag_up = True
            if 'BROADCAST' == w:
                i.flag_broadcast = True
            if 'RUNNING' == w:
                i.flag_running = True
            if 'MULTICAST' == w:
                i.flag_multicast = True
            if 'LOOPBACK' == w:
                i.flag_loopback = True

            prevprev = prev
            prev = w

    def __init__(self):
        self.intf = []
        if which('ip'):
            out = Popen(['ip', 'addr'], stdout=PIPE).communicate()[0]
            self.parse_ip(out)
        else:
            out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]
            self.parse_ifconfig(out)
