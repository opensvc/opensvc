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
from rcUtilities import justcall

def ipv4_bitmask(s):
    if len(s) != 8:
        return
    import re
    regex = re.compile('^[0-9a-f]*$')
    if regex.match(s) is None:
        return
    r = []
    for i in range(4):
        pk = s[2*i:2*i+2]
        r.append(int(pk, 16))
    return '.'.join(map(str, r))
        
class ifconfig(rcIfconfig.ifconfig):

    def __init__(self, mcast=False):
        self.intf = []
        out = Popen(['ifconfig', '-a'], stdin=None, stdout=PIPE,stderr=PIPE,close_fds=True).communicate()[0]
        self.parse(out)

    def set_hwaddr(self, i):
        if i is None or i.hwaddr != '':
            return i
        if ":" in i.name:
            name = i.name.split(":")[0]
        else:
            name = i.name
        cmd = ["hwmgr", "get", "attribute", "-category", "network",
               "-a", "name="+name, "-a", "MAC_address"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return i
        for line in out.split('\n'):
            if not line.strip().startswith("MAC"):
                continue
            l = line.split("=")
            i.hwaddr = l[1].replace('-', ':').lower()
        return i

    def parse(self, out):
        i = None
        for l in out.split("\n"):
            if l == '' : continue
            if l[0]!=' ' :
                i = self.set_hwaddr(i)
                (ifname,ifstatus)=l.split(': ')

                i=rcIfconfig.interface(ifname)
                self.intf.append(i)

                # defaults
                i.link_encap = ''
                i.scope = ''
                i.bcast = []
                i.mtu = []
                i.mask = []
                i.ipaddr = []
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_ipv4 = False
                i.flag_ipv6 = False
                i.flag_loopback = False

                if 'UP' in ifstatus : i.flag_up = True
                elif 'BROADCAST' in ifstatus : i.flag_broadcast = True
                elif 'RUNNING' in ifstatus   : i.flag_running = True
                elif 'MULTICAST' in ifstatus : i.flag_multicast = True
                elif 'IPv4' in ifstatus      : i.flag_ipv4 = True
                elif 'IPv6' in ifstatus      : i.flag_ipv6 = True
            else:
                n=0
                w=l.split()
                while n < len(w) :
                    [p,v]=w[n:n+2]
                    if p == 'inet' :
                        i.ipaddr.append(v)
                        i.mask.append(ipv4_bitmask(w[n+3]))
                    elif p == 'ipmtu' : i.mtu.append(v)
                    elif p == 'inet6' :
                        (a, m) = v.split('/')
                        i.ip6addr += [a]
                        i.ip6mask += [m]
                    n+=2
        i = self.set_hwaddr(i)


if __name__ == "__main__":
    for c in (ifconfig,) :
        help(c)
