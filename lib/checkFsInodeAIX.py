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
from rcUtilities import justcall

class check(checks.check):
    chk_type = "fs_i"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for rs in svc.get_res_sets('fs'):
                for r in rs.resources:
                    if r.mountPoint == mountpt:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['df', '-i']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 7:
                continue
            if l[1] == '-':
                continue
            if ":/" in l[0]:
                continue
            r.append({
                      'chk_instance': l[6],
                      'chk_value': l[5],
                      'chk_svcname': self.find_svc(l[6]),
                     })
        return r
