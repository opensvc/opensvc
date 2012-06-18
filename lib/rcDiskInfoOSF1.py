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

from rcUtilities import justcall
import rcDiskInfo
import re

regex = re.compile("^\W*[0-9]*:")

class diskInfo(rcDiskInfo.diskInfo):

    def __init__(self):
        self.load_cache()

    def is_id(self, line):
	if regex.match(line) is None:
            return False
	return True

    def load_cache(self):
        self.h = {}
        cmd = ["hwmgr", "show", "scsi", "-type", "disk", "-active", "-full"]
        out, err, ret = justcall(cmd)
        for e in out.split('\n'):
            if len(e) == 0:
                continue
            if self.is_id(e):
                l = e.split()
                if len(l) < 8:
                    continue
                id = l[0].strip(':')
                dev = l[7]
            elif 'WWID' in e:
                wwid = e.split(":")[-1].replace('-','').lower()
                d = self.devattr(id)
                vid = d['manufacturer']
                pid = d['model']
                size = d['mb']
                self.h[dev] = dict(wwid=wwid, vid=vid, pid=pid, size=size, id=id)

    def devattr(self, id):
        d = {'capacity': 0, 'block_size': 0, 'manufacturer': '', 'model': '', 'mb': 0}
        cmd = ["hwmgr", "get", "att", "-id", id,
               "-a", "model",
               "-a", "manufacturer",
               "-a", "capacity",
               "-a", "block_size"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return d
        for line in out.split("\n"):
            if not line.startswith(' '):
                continue
            l = line.split('=')
            if len(l) !=2:
                continue
            d[l[0].strip()] = l[1].strip()
        d['mb'] = int(d['capacity']) * int(d['block_size']) // 1024 // 1024
        return d
        
    def get(self, dev, type):
        dev = dev.replace('/dev/rdisk/','')
        dev = dev.replace('/dev/disk/','')
        if dev not in self.h:
            return
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

