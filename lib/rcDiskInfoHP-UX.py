#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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
        self.load_cache()

    def load_cache(self):
        self.load_aliases()

        self.h = {}
        cmd = ["scsimgr", "-p", "get_attr", "all_lun", "-a", "wwid", "-a", "device_file", "-a", "vid", "-a", "pid", "-a", "capacity"]
        (ret, out, err) = call(cmd)
        for e in out.split('\n'):
            if len(e) == 0:
                continue
            (wwid, dev, vid, pid, size) = e.split(':')
            wwid = wwid.replace('0x', '')
            if len(size) != 0:
                size = int(size)/2048
            vid = vid.strip('" ')
            pid = pid.strip('" ')
            if dev in self.aliases:
                aliases = self.aliases[dev]
            else:
                aliases = [dev]
            for alias in aliases:
                self.h[alias] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def load_aliases(self):
        self.aliases = {}
        cmd = ['/usr/sbin/ioscan', '-FunNC', 'disk']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return
        l = []
        for line in out.split('\n')+[':']:
            if ':' in line:
                if len(l) > 0:
                    for name in l:
                         self.aliases[name] = l
                l = []
                continue
            for w in line.split():
                l.append(w)

    def dev2char(self, dev):
        dev = dev.replace("/dev/disk/", "/dev/rdisk/")
        dev = dev.replace("/dev/dsk/", "/dev/rdsk/")
        return dev

    def scan(self, dev):
        cmd = ["scsimgr", "-p", "get_attr", "-D", self.dev2char(dev), "-a", "wwid", "-a", "device_file", "-a", "vid", "-a", "pid", "-a", "capacity"]
        (ret, out, err) = call(cmd, errlog=False)
        if ret != 0:
            self.h[dev] = dict(wwid="", vid="", pid="", size="")
            return
        (wwid, foo, vid, pid, size) = out.split(':')
        wwid = wwid.replace('0x', '')
        if len(size) != 0:
            size = int(size)/2048
        vid = vid.strip('" ')
        pid = pid.strip('" ')
        self.h[dev] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def get(self, dev, type):
        if dev not in self.h:
            self.scan(dev)
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

