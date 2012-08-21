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
import glob
import rcEthtool

class check(checks.check):
    chk_type = "eth"

    def do_check(self):
        cmd = ["lanscan", "-q"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        r = []
        intf = set([])
        for line in out.split("\n"):
            if len(line) == 0:
                continue
            l = line.split()
            n = len(l)
            if n == 1:
                # add interfaces with an inet config
                if self.has_inet(l[0]):
                    intf.add(l[0])
            elif n > 1:
                # add slaves for apa with an inet config
                if self.has_inet(l[0]):
                    for w in l[1:]:
                        intf.add(w)
            else:
                continue

        for i in intf:
            r += self.do_check_intf(i)

        return r

    def has_inet(self, intf):
        cmd = ["ifconfig", "lan"+intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'inet' in out:
            return True
        return False

    def do_check_intf(self, intf):
        r = []
        cmd = ["lanadmin", "-x", intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []

        intf = "lan"+intf
        inst = intf + ".link"
        if "link is down" in out:
            val = "0"
        else:
            val = "1"
        r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".speed"
        val = "0"
        for line in out.split('\n'):
            if "Speed" not in line:
                continue
            try:
                val = line.split()[2]
            except:
                pass
        r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".autoneg"
        val = "0"
        for line in out.split('\n'):
            if "Autoneg" not in line:
                continue
            if " On":
                val = "1"
        r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".duplex"
        val = '0'
        for line in out.split('\n'):
            if "Speed" not in line:
                continue
            if 'Full-Duplex' in line:
                val = "1"
        r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        return r
