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

from rcUtilities import call, which

class diskInfo(object):
    def __init__(self):
        self.h = {}
        cmd = ["scsimgr", "-p", "get_attr", "all_lun", "-a", "wwid", "-a", "device_file", "-a", "vid", "-a", "pid", "-a", "capacity"]
        (ret, out) = call(cmd)
        for e in out.split('\n'):
            if len(e) == 0:
                continue
            (wwid, dev, vid, pid, size) = e.split(':')
            dev = self.devkey(dev)
            wwid = wwid.replace('0x', '')
            if len(size) != 0:
                size = int(size)/2097152
            vid = vid.strip('" ')
            pid = pid.strip('" ')
            self.h[dev] = dict(wwid=wwid, vid=vid, pid=pid, size=size)

    def devkey(self, dev):
        dev = dev.replace("/dev/rdisk/", "")
        dev = dev.replace("/dev/disk/", "")
        dev = dev.replace("/dev/dsk/", "")
        return dev

    def disk_id(self, dev):
        dev = self.devkey(dev)
        return self.h[dev]['wwid']

    def disk_vendor(self, dev):
        dev = self.devkey(dev)
        return self.h[dev]['vid']

    def disk_model(self, dev):
        dev = self.devkey(dev)
        return self.h[dev]['pid']

    def disk_size(self, dev):
        dev = self.devkey(dev)
        return self.h[dev]['size']

