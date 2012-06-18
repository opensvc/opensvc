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
import glob
import re
import os
from rcUtilities import justcall

class check(checks.check):
    def __init__(self, svcs=[]):
        checks.check.__init__(self, svcs)

    chk_type = "fs_u"

    def find_svc(self, name):
        for svc in self.svcs:
            for rs in svc.get_res_sets('pool'):
                for r in rs.resources:
                    if r.poolname == name:
                        return svc.svcname
        return ''

    def do_check(self):
        doms = glob.glob('/etc/fdmns/*')
        regex = re.compile('\W*[0-9]*L')
        r = []
        for dom in doms:
            dom = os.path.basename(dom)
            if dom.startswith('.'):
                continue
            cmd = ['showfdmn', dom]
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            i = 0
            pct = 0
            for line in out.split('\n'):
                if regex.match(line) is None:
                    continue
                l = line.split()
                if len(l) < 4:
                    continue
                i += 1
                pct += int(l[3].replace('%',''))
            if i>0:
                r.append({
                          'chk_instance': dom,
                          'chk_value': str(int(pct/i)),
                          'chk_svcname': self.find_svc(dom),
                         })
        return r
