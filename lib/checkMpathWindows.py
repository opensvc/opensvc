#
# Copyright (c) 2013 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import wmi
import tempfile
from subprocess import *
import os

class check(checks.check):
    chk_type = "mpath"

    def find_svc(self, dev):
        for svc in self.svcs:
            if dev in svc.disklist():
                return svc.svcname
        return ''

    def diskpart_rescan(self):
        f = tempfile.NamedTemporaryFile()
	tmpf = f.name
	f.close()
	with open(tmpf, 'w') as f:
	    f.write("rescan\n")
	p = Popen(["diskpart", "/s", tmpf], stdout=PIPE, stderr=PIPE, stdin=None)
        out, err = p.communicate()
	os.unlink(tmpf)

    def do_check(self):
        self.wmi = wmi.WMI(namespace="root/wmi")
	self.diskpart_rescan()
	r = []
	for disk in self.wmi.MPIO_DISK_INFO():
	    for drive in disk.driveinfo:
                name = drive.name
		n = drive.numberpaths
                r.append({'chk_instance': name,
                          'chk_value': str(n),
                          'chk_svcname': "",
                         })
        return r
