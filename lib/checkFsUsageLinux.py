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
from rcUtilities import justcall

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
        cmd = ['df', '-lP']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 6:
                continue
            # discard bind mounts: we get metric from the source anyway
            if l[0].startswith('/') and not l[0].startswith('/dev') and not l[0].startswith('//'):
                continue
            if l[5].startswith('/Volumes'):
                continue
            if l[5].startswith('/run'):
                continue
            if l[5].startswith('/sys/'):
                continue
            if l[5] == "/dev/shm":
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            r.append({
                      'chk_instance': l[5],
                      'chk_value': l[4],
                      'chk_svcname': self.find_svc(l[5]),
                     })
        return r
