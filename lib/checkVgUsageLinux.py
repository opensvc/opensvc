#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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
from rcUtilities import call

class check(checks.check):
    chk_type = "vg_u"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for rs in svc.get_res_sets('disk.vg'):
                for r in rs.resources:
                    if r.name == vgname:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['vgs', '--units', 'b', '--noheadings',
               '-o', 'vg_name,vg_size,vg_free']
        (ret, out) = call(cmd, errlog=False)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        for line in lines:
            l = line.split()
            if len(l) != 3:
                continue
            size = int(l[1].replace('B',''))
            free = int(l[2].replace('B',''))
            val = int(100*(size-free)/size)
            r.append({'chk_instance': l[0],
                      'chk_value': str(val),
                      'chk_svcname': self.find_svc(l[0]),
                     }
                    )
        return r
