# Copyright (c) 2012 Lucien Hercaud <hercaud@hercaud.com>
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
import os
import re
from rcGlobalEnv import rcEnv
 
class check(checks.check):
    """
# btrfs dev stats /mnt
[/dev/loop0].write_io_errs   0
[/dev/loop0].read_io_errs    0
[/dev/loop0].flush_io_errs   0
[/dev/loop0].corruption_errs 0
[/dev/loop0].generation_errs 0
[/dev/loop1].write_io_errs   0
[/dev/loop1].read_io_errs    0
[/dev/loop1].flush_io_errs   0
[/dev/loop1].corruption_errs 0
[/dev/loop1].generation_errs 0
    """
    chk_type = "btrfs"
 
    def _get_dev_stats(self, mntpt, data):
        cmd = ['btrfs', 'dev', 'stats', mntpt]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        for line in out.split('\n'):
            l = line.split()
            if len(l) != 2:
                continue
            key, val = l
            l = key.split('.')
            if len(l) != 2:
                continue
            dev, err_type = l
            dev = dev.lstrip('[').rstrip(']')
            if dev not in data:
                data[dev] = {}
            data[dev][err_type] = val
        return data

    def get_dev_stats(self):
        mntpts = self.get_btrfs_mounts()
        data = {}
        if mntpts is None:
            return data
        for mntpt in mntpts:
            data = self._get_dev_stats(mntpt, data)
        return data

    def get_btrfs_mounts(self):
        mntpts = []
        p = '/proc/mounts'
        if not os.path.exists(p):
            return
        with open(p, 'r') as f:
            buff = f.read()
        for line in buff.split('\n'):
            if 'btrfs' not in line:
                continue
            l = line.split()
            if len(l) < 2:
                continue
            mntpt = l[1]
            if not os.path.exists(mntpt):
                continue
            mntpts.append(mntpt)
        return mntpts

    def find_svc(self, dev):
        for svc in self.svcs:
            if dev in svc.disklist():
                return svc.svcname
        return ''
 
    def do_check(self):
        r = []
        data = self.get_dev_stats()
        if data is None:
            return r
        for dev, _data in data.items():
            for err_type, val in _data.items():
                r.append({'chk_instance': dev+'.'+err_type,
                          'chk_value': val,
                          'chk_svcname': self.find_svc(dev),
                        })
        return r

if __name__ == "__main__":
    o = check()
    r = o.do_check()
    print(r)
