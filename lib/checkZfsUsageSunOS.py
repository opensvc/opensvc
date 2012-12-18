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
import svcZone

class check(checks.check):
    def __init__(self, svcs=[]):
        checks.check.__init__(self, svcs)
        self.zpcache = {}

    chk_type = "fs_u"

    def convert(self, s):
        s = s.replace(',', '.').upper()
        if s == "0":
            return 0
        if len(s) < 2:
            raise
        if s.endswith('T'):
            s = float(s[:-1])*1024*1024*1024
        elif s.endswith('G'):
            s = float(s[:-1])*1024*1024
        elif s.endswith('M'):
            s = float(s[:-1])*1024
        elif s.endswith('K'):
            s = float(s[:-1])
        else:
            raise
        return s

    def get_zonepath(self, name):
        if name in self.zpcache:
            return self.zpcache[name]
        cmd = ['zonecfg', '-z', name, 'info', 'zonepath']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return None
        self.zpcache[name] = out.split()[-1].strip()
        return self.zpcache[name]

    def find_svc(self, name, mnt):
        for svc in self.svcs:
            for rs in svc.get_res_sets('container'):
                for r in rs.resources:
                    if  r.type == "container.zone": 
                        zp = self.get_zonepath(r.name)
                        if zp is not None and zp == mnt:
                            return svc.svcname
            for rs in svc.get_res_sets('fs'):
                for r in rs.resources:
                    if r.device == name:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['zfs', 'list', '-o', 'name,used,avail,mountpoint', '-H']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        for line in lines:
            l = line.split()
            if len(l) != 4:
                continue
            if "@" in l[0]:
                # do not report clone usage
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            used = self.convert(l[1])
            avail = self.convert(l[2])
            total = used + avail
            pct = used / total * 100
            r.append({
                      'chk_instance': l[0],
                      'chk_value': str(pct),
                      'chk_svcname': self.find_svc(l[0], l[3]),
                     })
        return r
