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
import checks
import os
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv

"""
key: 1 (0x0001) policy: L4      address: 0:15:17:bb:82:d2 (auto)
           device       address                 speed           duplex  link    state
           e1000g0      0:15:17:bb:82:d2          1000  Mbps    full    up      attached
           bnx0         0:24:e8:35:61:3b          1000  Mbps    full    up      attached
"""

class check(checks.check):
    chk_type = "lag"

    def do_check(self):
        if not which("dladm"):
            return self.undef
        cmd = ['dladm', 'show-aggr']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        self.lines = out.split('\n')
        if len(self.lines) == 0:
            return self.undef
        r = []
        r += self.do_check_speed()
        r += self.do_check_duplex()
        r += self.do_check_link()
        r += self.do_check_attach()
        return r

    def do_check_speed(self):
        r = []
        lag = ""
        i = 0
        for line in self.lines:
            l = line.split()
            if len(l) < 4:
                continue
            elif line.startswith('key'):
                lag = l[1]
                i = 0
                continue
            elif l[0] == 'device':
                continue
            val = l[2]
            r.append({
                      'chk_instance': 'lag %s.%d speed'%(lag, i),
                      'chk_value': str(val),
                      'chk_svcname': '',
                     })
            i += 1
        return r

    def do_check_duplex(self):
        return self._do_check("duplex", "full", 4)

    def do_check_link(self):
        return self._do_check("link", "up", 5)

    def do_check_attach(self):
        return self._do_check("attach", "attached", 6)

    def _do_check(self, key, target, col):
        r = []
        lag = ""
        i = 0
        for line in self.lines:
            l = line.split()
            if len(l) < col+1:
                continue
            elif line.startswith('key'):
                lag = l[1]
                i = 0
                continue
            elif l[0] == 'device':
                continue
            else:
                if l[col] != target:
                    val = 1
                else:
                    val = 0
                r.append({
                          'chk_instance': 'lag %s.%d %s'%(lag, i, key),
                          'chk_value': str(val),
                          'chk_svcname': '',
                         })
                i += 1
        return r
