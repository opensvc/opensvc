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
from rcUtilitiesWindows import get_drives

class check(checks.check):
    chk_type = "fs_u"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for rs in svc.get_res_sets('fs'):
                for r in rs.resources:
                    if r.mountPoint == mountpt:
                        return svc.svcname
        return ''

    def do_check(self):
        import win32api
        cmd = ['df', '-lP']
        r = []
        for drive in get_drives():
            try:
                n_free_user, n_total, n_free = win32api.GetDiskFreeSpaceEx(drive+':\\')
            except:
                continue
            pct = 100 * (n_total - n_free) // n_total
            r.append({
                      'chk_instance': drive,
                      'chk_value': str(pct),
                      'chk_svcname': self.find_svc(drive),
                     })
        return r

if __name__ == "__main__":
    o = check()
    print(o.do_check())
