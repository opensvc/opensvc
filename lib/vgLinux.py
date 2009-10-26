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
import re
import os

import rcStatus
import vg

def get_blockdev_sd_slaves(syspath):
    slaves = []
    for s in os.listdir(syspath):
        if re.match('^sd[a-z]*', s) is not None:
            slaves.append(s)
            continue
        deeper = os.path.join(syspath, s, 'slaves')
        if os.path.isdir(deeper):
            slaves += get_blockdev_sd_slaves(deeper)
    return slaves

class Vg(vg.Vg):
    def has_vg(self):
        """Returns True if the volume is present
        """
        cmd = [ 'vgs', '--noheadings', '-o', 'name' ]
        (ret, out) = self.call(cmd)
        if re.match('\s*'+self.vgName+'\s', out, re.MULTILINE) is None:
            return False
        return True

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.has_vg():
            return False
        cmd = [ 'lvs', '--noheadings', '-o', 'lv_attr', self.vgName ]
        (ret, out) = self.call(cmd)
        if re.match(' ....-[-o]', out, re.MULTILINE) is None:
            return True
        return False

    def start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.vgName)
            return 0
        cmd = [ 'vgchange', '-a', 'y', self.vgName ]
        (ret, out) = self.vcall(cmd)
        return ret

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.vgName)
            return 0
        cmd = [ 'vgchange', '-a', 'n', self.vgName ]
        (ret, out) = self.vcall(cmd)
        return ret

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def disklist(self):
        if not self.has_vg():
            return []
        minors = []
        disks = []
        cmd = [ 'lvs', '-o', 'lv_kernel_minor', '--noheadings', self.vgName ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise Exception()
        for minor in out.split():
            syspath = '/sys/block/dm-'+minor+'/slaves'
            disks += get_blockdev_sd_slaves(syspath)
        # remove duplicate entries in disk list
        disks = list(set(disks))
        self.log.debug("found disks %s held by vg %s" % (disks, self.vgName))
        return disks

    def __init__(self, vgName):
        vg.Vg.__init__(self, vgName)
