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

    def __init__(self, ifconfig=None, mcast=False):
        self.intf = []
        if mcast:
            self.mcast_data = self.get_mcast()
        if ifconfig is not None:
            out = ifconfig
        else:
            out = Popen(['/usr/sbin/ifconfig', '-a'], stdin=None, stdout=PIPE,stderr=PIPE,close_fds=True).communicate()[0]
        self.parse(out)

    def get_mcast(self):
        cmd = ['netstat', '-gn']
        out = Popen(cmd, stdout=PIPE).communicate()[0]
        return self.parse_mcast(out)

    def parse_mcast(self, out):
        lines = out.split('\n')
        found = False
        data = {}
        for i, line in enumerate(lines):
            if line.startswith('--'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
	    if len(line) == 0:
	        break 
            try:
                intf, addr, refcnt = line.split()
            except:
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
	if len(lines) <= i + 1:
	    return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
            if line.startswith('--'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
	    if len(line) == 0:
	        break 
            try:
                intf, addr, refcnt = line.split()
            except:
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
        return data


    def set_hwaddr(self, i):
        if i is None or i.hwaddr != '' or ':' not in i.name:
            return i
        base_ifname, index = i.name.split(':')
        base_intf = self.interface(base_ifname)
        if base_intf is not None and len(base_intf.hwaddr) > 0:
            i.hwaddr = base_intf.hwaddr
        else:
            i.hwaddr = self.mac_from_arp(i.ipaddr)
        return i

    def mac_from_arp(self, ipaddr):
        cmd = ['/usr/sbin/arp', ipaddr]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return ''
        for word in out.split():
            if ':' not in word:
                continue
            return word
        return ''

    def parse(self, out):
        i = None
        for l in out.split("\n"):
            if l == '' : break
            if l[0]!='\t' :
                i = self.set_hwaddr(i)
                (ifname,ifstatus)=l.split(': ')

                i=rcIfconfig.interface(ifname)
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
                i.groupname = ''
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
                    if p == 'inet' : i.ipaddr=v
                    elif p == 'netmask' : i.mask=v
                    elif p == 'broadcast' : i.bcast=v
                    elif p == 'ether' : i.hwaddr=v
                    elif p == 'groupname' : i.groupname=v
                    elif p == 'inet6' :
                        (a, m) = v.split('/')
                        i.ip6addr += [a]
                        i.ip6mask += [m]
                    n+=2
        i = self.set_hwaddr(i)


if __name__ == "__main__":
    ifaces = ifconfig(mcast=True)
    print(ifaces)
