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
    """
    # mpathadm list LU
        /dev/rdsk/c6t600507680280809AB0000000000000E7d0s2
                Total Path Count: 4
                Operational Path Count: 4
        /scsi_vhci/disk@g60050768018085d7e0000000000004e5
                Total Path Count: 1
                Operational Path Count: 1
        /dev/rdsk/c6t60050768018085D7E0000000000004E4d0s2
                Total Path Count: 1
                Operational Path Count: 1
        /dev/rdsk/c6t60050768018085D7E00000000000056Bd0s2
                Total Path Count: 4
                Operational Path Count: 4
    """
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.disklist()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.svcname
        return ''

    def do_check(self):
        cmd = ['mpathadm', 'list', 'LU']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 4:
            return self.undef
        r = []
        dev = None
        wwid = ""
        for line in lines:
            if "/dev/" in line:
                # new mpath
                # - remember current dev
                # - remember current wwid
                # - reset path counter
                dev = line.strip()
                wwid = line[line.index('t')+1:line.rindex('d')]
                n = 0
            elif '/disk@g' in line:
                # unmapped dev
                # - remember current dev
                # - remember current wwid
                # - reset path counter
                dev = line.strip()
                wwid = '_'+line[line.index('@g')+2:]
                n = 0
            if "Total Path Count:" in line:
                continue
            if "Operational Path Count:" in line:
                # - store current dev if valid
                # - then:
                    # - reset path counter
                    # - reset dev
                n = int(line.split(':')[-1].strip())
                if dev is not None:
                    r.append({'chk_instance': wwid,
                              'chk_value': str(n),
                              'chk_svcname': self.find_svc(dev),
                             })
                    dev = None
                    n = 0
        return r

