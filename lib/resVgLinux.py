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
import rcExceptions as ex
import resDg
from rcGlobalEnv import rcEnv
from rcUtilitiesLinux import major, get_blockdev_sd_slaves

class Vg(resDg.Dg):
    def __init__(self, rid=None, name=None, type=None,
                 optional=False, disabled=False,
                 always_on=set([])):
        self.label = name
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled)

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

    def remove_tag(self, tag):
        cmd = [ 'vgchange', '--deltag', '@'+tag, self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def remove_tags(self):
        cmd = ['vgs', '-o', 'tags', '--noheadings', self.name]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        out = out.strip(' \n')
        tags = out.split(',')
        for tag in tags:
            if len(tag) == 0:
                continue
            self.remove_tag(tag)

    def add_tags(self):
        cmd = [ 'vgchange', '--addtag', '@'+rcEnv.nodename, self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            return 0
        self.remove_tags()
        self.add_tags()
        cmd = [ 'vgchange', '-a', 'y', self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return
        self.remove_tags()
        cmd = [ 'vgchange', '-a', 'n', self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def disklist(self):
        if not self.has_it():
            return set()
        if self.disks != set():
            return self.disks

        disks = set()

        cmd = ['vgs', '--noheadings', '-o', 'pv_name', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            return disks
	pvs = set(out.split())
        self.disks = self.pvs_to_disks(pvs)
        self.log.debug("found disks %s held by vg %s" % (disks, self.name))
        return disks

    def pvs_to_disks(self, pvs):
        """If PV is a device map, replace by its sysfs name (dm-*)
        If device map has slaves, replace by its slaves
        """
        disks = set()
        dm_major = major('device-mapper')
        try: lo_major = major('loop')
        except: lo_major = 0
        for pv in pvs:
            try:
                statinfo = os.stat(pv)
            except:
                self.log.error("can not stat %s" % pv)
                raise
            if os.major(statinfo.st_rdev) == dm_major:
                dm = 'dm-' + str(os.minor(statinfo.st_rdev))
                syspath = '/sys/block/' + dm + '/slaves'
                disks |= get_blockdev_sd_slaves(syspath)
            elif lo_major != 0 and os.major(statinfo.st_rdev) == lo_major:
                self.log.debug("skip loop device %s from disklist"%pv)
                pass
            else:
                disks.add(pv)
        return disks

