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
    slaves = set()
    for s in os.listdir(syspath):
        if re.match('^sd[a-z]*', s) is not None:
            slaves.add('/dev/' + s)
            continue
        deeper = os.path.join(syspath, s, 'slaves')
        if os.path.isdir(deeper):
            slaves |= get_blockdev_sd_slaves(deeper)
    return slaves

def dm_major():
    path = os.path.join(os.path.sep, 'proc', 'devices')
    try:
        f = open(path)
    except:
        raise
    for line in f.readlines():
        words = line.split()
        if len(words) == 2 and words[1] == 'device-mapper':
            f.close()
            return int(words[0])
    f.close()
    raise

class Vg(resDg.Dg):
    def __init__(self, name=None, type=None, optional=False, disabled=False, scsireserv=False):
        self.id = 'vg ' + name
        resDg.Dg.__init__(self, name, 'disk.vg', optional, disabled, scsireserv)

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

    def disklist(self):
        if not self.has_it():
            return set()
        if self.disks != set() :
            return self.disks

        disks = set()

        """Get volume group's PV from the clear text metadata backup file
        """
        config = os.path.join(os.path.sep, 'etc', 'lvm', 'backup', self.name)
        if not os.path.exists(config):
            return disks
        try:
            f = open(config)
        except:
            self.log.error("can not open %s" % config)
            raise
        pvs = set()
        for line in f.readlines():
            words = line.split()
            if len(words) > 3 and words[0] == 'device':
                pvs.add(words[2].strip('"'))

        """If PV is a device map, replace by its sysfs name (dm-*)
        If device map has slaves, replace by its slaves
        """
        major = dm_major()
        for pv in pvs:
            try:
                statinfo = os.stat(pv)
            except:
                self.log.error("can not stat %s" % pv)
                raise
            if os.major(statinfo.st_rdev) == major:
                dm = 'dm-' + str(os.minor(statinfo.st_rdev))
                syspath = '/sys/block/' + dm + '/slaves'
                disks |= get_blockdev_sd_slaves(syspath)
            else:
                disks.add(pv)

        self.log.debug("found disks %s held by vg %s" % (disks, self.name))
        self.disks = disks
        return disks
