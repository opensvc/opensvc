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
from rcUtilities import justcall
from rcDiskInfoOSF1 import diskInfo

class check(checks.check):
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        devpath = '/dev/rdisk/'+dev
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
        di = diskInfo()
        r = []
        for dev, data in di.h.items():
            r.append({'chk_instance': data['wwid'],
                      'chk_value': str(data['path_count']),
                      'chk_svcname': self.find_svc(dev),
                     })
        return r
