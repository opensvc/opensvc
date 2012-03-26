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
    chk_type = "mpath"

    def find_svc(self, dev):
        for svc in self.svcs:
            if dev in svc.disklist():
                return svc.svcname
        return ''

    def do_check(self):
        cmd = ['multipathd', '-kshow topo']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        wwid = None
        dev = None
        for line in lines:
            if ' dm-' in line:
                # new mpath
                # - store previous
                # - reset path counter
                if wwid is not None:
                    r.append({'chk_instance': wwid,
                              'chk_value': str(n),
                              'chk_svcname': self.find_svc(dev),
                             })
                n = 0
                if line.startswith(": "):
                    line = line.replace(": ", "")
                l = line.split()
                if l[0] == 'create:':
                    l = l[1:]
                if len(l) < 2:
                    continue
                if l[1].startswith('('):
                    wwid = l[1][2:-1]
                else:
                    wwid = l[0][1:]
            if "[active][ready]" in line or \
               "active ready" in line:
                n += 1
                dev = "/dev/"+line.split()[2]
        return r
