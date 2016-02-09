#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

class check(checks.check):
    omreport = "/opt/dell/srvadmin/bin/omreport"
    chk_type = "om"
    chk_name = "OpenManage"

    def find_omreport(self):
        if which(self.omreport):
            return self.omreport
        return

    def do_check(self):
        r = self.do_check_system()
        r += self.do_check_chassis()
        return r

    def do_check_chassis(self):
        return self.do_check_gen("chassis")

    def do_check_system(self):
        return self.do_check_gen("system")

    def do_check_gen(self, command):
        omreport = self.find_omreport()
        if omreport is None:
            return self.undef
        cmd = [omreport, command]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        for line in lines:
            l = line.split(" : ")
            if len(l) != 2:
                continue
            inst = l[1].strip().lower()
            state = l[0].strip().lower()
            if state == "severity":
                continue
            elif state == "ok":
                state = 0
            else:
                state = 1
            r.append({
                      'chk_instance': inst,
                      'chk_value': str(state),
                      'chk_svcname': '',
                     })
        return r

if __name__ == "__main__":
    from rcUtilities import printplus
    o = check()
    tab = o.do_check()
    printplus(tab)

