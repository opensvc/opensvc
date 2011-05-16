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

from rcUtilities import call
import rcDiskInfo

class diskInfo(rcDiskInfo.diskInfo):

    def __init__(self):
        self.h = {}

    def scan(self, lname):
        vid = 'unknown'
        pid = 'unknown'
        wwid = 'unknown'
        size = 'unknown'

        cmd = ['lscfg', '-vpl', lname]
        (ret, out, err) = call(cmd)

        for f in out.split('\n'):
            if "Manufacturer" in f:
                vid = f.split('.')[-1]
            if "Machine Type and Model" in f:
                pid = f.split('.')[-1]

        size = str(int(self.odmget(lname, 'size_in_mb'))//1024)
        wwid = self.odmget(lname, 'ww_name').replace('0x', '')

        self.h[lname] = dict(vid=vid, pid=pid, wwid=wwid, size=size)

    def odmget(self, lname, attr):
        cmd = ['odmget', '-q', 'name='+lname+' AND attribute='+attr, 'CuAt']
        (ret, out, err) = call(cmd)
        for f in out.split('\n'):
            if "value" not in f:
                continue
            return f.split(" = ")[-1].strip('"')
        return 'unknown'


    def devkey(self, dev):
        dev = dev.replace("/dev/", "")
        return dev

    def get(self, dev, type):
        if dev not in self.h:
            self.scan(dev)
        dev = self.devkey(dev)
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

