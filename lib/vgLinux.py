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

import resDg

def get_blockdev_sd_slaves(syspath):
    slaves = []
    for s in os.listdir(syspath):
        if re.match('^sd[a-z]*', s) is not None:
            slaves.append('/dev/' + s)
            continue
        deeper = os.path.join(syspath, s, 'slaves')
        if os.path.isdir(deeper):
            slaves += get_blockdev_sd_slaves(deeper)
    return slaves

class Vg(resDg.Dg):
    def __init__(self, name=None, type=None, optional=False, disabled=False, scsireserv=False):
        self.id = 'vg ' + name
        resDg.Dg.__init__(self, name, 'vg', optional, disabled, scsireserv)

    def has_it(self):
        """Returns True if the volume is present
        """
        cmd = [ 'vgs', '--noheadings', '-o', 'name' ]
        (ret, out) = self.call(cmd)
        if self.name in out.split():
            return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'lvs', '--noheadings', '-o', 'lv_attr', self.name ]
        (ret, out) = self.call(cmd)
        if re.match(' ....-[-o]', out, re.MULTILINE) is None:
            return True
        return False

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            return 0
        cmd = [ 'vgchange', '-a', 'y', self.name ]
        (ret, out) = self.vcall(cmd)
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        cmd = [ 'vgchange', '-a', 'n', self.name ]
        (ret, out) = self.vcall(cmd)
        return ret

    def stop(self):
        if self.scsirelease() != 0:
            return 1
        if self.do_stop() != 0:
            return 1
        return 0

    def start(self):
        if self.do_start() != 0:
            return 1
        if self.scsireserv() != 0:
            return 1
        return 0

    def disklist(self):
        if not self.has_it():
            return set()
        if self.disks != set() :
            return self.disks

        disks = set()
        cmd = [ 'lvs', '-o', 'lv_kernel_minor', '--noheadings', self.name ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise Exception()
        for minor in out.split():
            if minor == '-1':
                # means the lv is inactive
                continue
            syspath = '/sys/block/dm-'+minor+'/slaves'
            disks.add(get_blockdev_sd_slaves(syspath))
        self.log.debug("found disks %s held by vg %s" % (disks, self.name))
        self.disks = disks
        return disks
