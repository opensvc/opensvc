#
# Copyright (c) 2012 Lucien Hercaud <hercaud@hercaud.com>
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
import checks
import os
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

"""
# ifconfig -a
lo0: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        inet 127.0.0.1 netmask ff000000
lo0:1: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        zone frcp00vpd0385
        inet 127.0.0.1 netmask ff000000
lo0:2: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        zone frcp00vpd0388
        inet 127.0.0.1 netmask ff000000
lo0:3: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        zone frcp00vpd0192
        inet 127.0.0.1 netmask ff000000
lo0:4: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        zone frcp00vpd0192
        inet 128.1.1.192 netmask ffff0000
lo0:5: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
        zone frcp00vpd0179
        inet 127.0.0.1 netmask ff000000
aggr1: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 2
        inet 172.31.4.195 netmask ffffff00 broadcast 172.31.4.255
        ether 0:15:17:bb:85:58
aggr1:1: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 2
        zone frcp00vpd0385
        inet 172.31.4.180 netmask ffffff00 broadcast 172.31.4.255
aggr1:2: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 2
        zone frcp00vpd0388
        inet 172.31.4.183 netmask ffffff00 broadcast 172.31.4.255
aggr1:3: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 2
        zone frcp00vpd0179
        inet 172.31.4.67 netmask ffffff00 broadcast 172.31.4.255
aggr2: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 5
        inet 172.31.195.195 netmask ffffff00 broadcast 172.31.195.255
        ether 0:15:17:bb:85:59
bnx3: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 4
        inet 55.16.201.195 netmask fffffc00 broadcast 55.16.203.255
        ether 0:24:e8:35:9d:dd
bnx3:1: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 4
        zone frcp00vpd0385
        inet 55.16.201.142 netmask fffffc00 broadcast 55.16.203.255
bnx3:2: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 4
        zone frcp00vpd0388
        inet 55.16.201.145 netmask fffffc00 broadcast 55.16.203.255
bnx3:3: flags=1000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4> mtu 1500 index 4
        zone frcp00vpd0179
        inet 55.16.202.98 netmask fffffc00 broadcast 55.16.203.255

Solaris 10
==========
# ndd -get /dev/bnx3 link_speed
1000
# ndd -get /dev/bnx3 link_duplex
1
# ndd -get /dev/bnx3 link_status
1
kstat -p | grep link_ | grep  ce:0:ce0:link
ce:0:ce0:link_asmpause  0
ce:0:ce0:link_duplex    2
ce:0:ce0:link_pause     0
ce:0:ce0:link_speed     1000
ce:0:ce0:link_up        1

Solaris 11
==========
# dladm show-link -p -o link,class,over l226g0
l226g0:vnic:aggr0
# dladm show-link -p -o link,class,over aggr0
aggr0:aggr:net0 net2
# dladm show-link -p -o link,class,over net0
net0:phys:
# dladm show-phys -p -o state,speed,duplex net0
up:1000:full
# dladm show-link -p -o link,class,over net2
# dladm show-phys -p -o state,speed,duplex net2
up:1000:full
"""

class check(checks.check):
    chk_type = "eth"

    def _findphys(self, netif):
        res = ""
        cmd = ['/usr/sbin/dladm', 'show-link', '-p', '-o', 'link,class,over', netif]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return ""
        lines = out.split('\n')
        for line in lines:
            if len(line) == 0:
                break
            v = line.split(':')
            if v[1] == 'phys':
                self.l[self.topif].add(v[0])
            else:
                ifs = v[2].split(' ')
                for i in ifs:
                    res = self._findphys(i)
            return "OK"

    def do_check(self):
        cmd = ['uname', '-v']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        self.osver = float(lines[0])
        self.ifs = []
        cmd = ['/usr/sbin/ifconfig', '-a']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        for line in lines:
            if line.startswith(' '):
                continue
            if line.startswith('lo'):
                continue
            if self.osver < 11:
                if line.startswith('aggr'):
                    continue
            if 'index' not in line:
                continue
            l = line.split(':')
            if 'index' not in l[1]:
                continue
            if len(l[0]) < 3:
                continue
            if l[0] in self.ifs:
                continue
            else:
                self.ifs.append(l[0])
        if self.osver >= 11:
            self.l = {}
            for ifn in self.ifs:
                if ifn not in self.l:
                    self.l[ifn] = set([])
                    self.topif = ifn
                    ret = self._findphys(ifn)
            cmd = ['/usr/sbin/dladm', 'show-phys', '-p', '-o', 'link,state,speed,duplex,device']
            out, err, ret = justcall(cmd)
            if ret != 0:
                return self.undef
            lines = out.split('\n')
            if len(lines) == 0:
                return self.undef
            self.phys = lines

        r = []
        r += self.do_check_speed()
        r += self.do_check_duplex()
        r += self.do_check_link()
        return r

    def do_check_speed(self):
        r = []
        if self.osver >= 11:
            for ifn in self.ifs:
                for phy in self.l[ifn]:
                    for line in self.phys:
                        if line.startswith(phy+':'):
                            l = line.split(':')
                            val = l[2]
                            r.append({
                                      'chk_instance': '%s/%s.speed'%(ifn,l[4]),
                                      'chk_value': str(val),
                                      'chk_svcname': '',
                                     })
            return r
        for ifn in self.ifs:
            if ifn.startswith('ce'):
                val = self._do_spec_check('ce',ifn,ifn[2:],'link_speed')
            elif ifn.startswith('bge'):
                val = self._do_spec_check('bge',ifn,ifn[3:],'link_speed')
            else:
                cmd = ['/usr/sbin/ndd', '-get', '/dev/'+ifn, 'link_speed']
                out, err, ret = justcall(cmd)
                if ret != 0:
                    val = 0
                else:
                    lines = out.split('\n')
                    if len(lines) == 0:
                        val = 0
                    else:
                        val = lines[0]
            r.append({
                      'chk_instance': '%s.speed'%ifn,
                      'chk_value': str(val),
                      'chk_svcname': '',
                     })
        return r

    def do_check_duplex(self):
        r = []
        if self.osver >= 11:
            for ifn in self.ifs:
                for phy in self.l[ifn]:
                    for line in self.phys:
                        if line.startswith(phy+':'):
                            l = line.split(':')
                            if l[3] != 'full':
                                val = 1
                            else:
                                val = 0
                            r.append({
                                      'chk_instance': '%s/%s.duplex'%(ifn,l[4]),
                                      'chk_value': str(val),
                                      'chk_svcname': '',
                                     })
            return r
        for ifn in self.ifs:
            if ifn.startswith('ce'):
                val = self._do_spec_check('ce',ifn,ifn[2:],'link_duplex')
            elif ifn.startswith('bge'):
                val = self._do_spec_check('bge',ifn,ifn[3:],'link_duplex')
            else:
                cmd = ['/usr/sbin/ndd', '-get', '/dev/'+ifn, 'link_duplex']
                out, err, ret = justcall(cmd)
                if ret != 0:
                    val = 0
                else:
                    lines = out.split('\n')
                    if len(lines) == 0:
                        val = 0
                    else:
                        val = lines[0]
            r.append({
                      'chk_instance': '%s.duplex'%ifn,
                      'chk_value': str(val),
                      'chk_svcname': '',
                     })
        return r

    def do_check_link(self):
        r = []
        if self.osver >= 11:
            for ifn in self.ifs:
                for phy in self.l[ifn]:
                    for line in self.phys:
                        if line.startswith(phy+':'):
                            l = line.split(':')
                            if l[1] != 'up':
                                val = 1
                            else:
                                val = 0
                            r.append({
                                      'chk_instance': '%s/%s.link'%(ifn,l[4]),
                                      'chk_value': str(val),
                                      'chk_svcname': '',
                                     })
            return r
        i = 0
        for ifn in self.ifs:
            if ifn.startswith('ce'):
                val = self._do_spec_check('ce',ifn,ifn[2:],'link_up')
            elif ifn.startswith('bge'):
                val = self._do_spec_check('bge',ifn,ifn[3:],'link_up')
            else:
                cmd = ['/usr/sbin/ndd', '-get', '/dev/'+ifn, 'link_status']
                out, err, ret = justcall(cmd)
                if ret != 0:
                    val = 0
                else:
                    lines = out.split('\n')
                    if len(lines) == 0:
                        val = 0
                    else:
                        val = lines[0]
            r.append({
                      'chk_instance': '%s.link'%ifn,
                      'chk_value': str(val),
                      'chk_svcname': '',
                     })
        return r

    def _do_spec_check(self, gifn, ifn, inst, patt):
        inst = ifn[2:]
        cmd = ['/usr/bin/kstat', '-p']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return 0
        else:
            lines = out.split('\n')
            if len(lines) == 0:
                return 0
            else:
                for line in lines:
                    if line.startswith(gifn+':'+inst+':'+ifn+':'+patt):
                        l = line.split()
                        return l[1]
        return 0
