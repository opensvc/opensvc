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
from rcUtilitiesLinux import major, get_blockdev_sd_slaves, \
                             devs_to_disks
from rcUtilities import which

class Vg(resDg.Dg):
    def __init__(self, rid=None, name=None, type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([])):
        self.label = name
        self.tag = '@'+rcEnv.nodename
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags)

    def has_it(self):
        """Returns True if the volume is present
        """
        cmd = ['vgdisplay', self.name]
        (ret, out) = self.call(cmd, cache=True)
        if ret == 0:
            return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'lvs', '--noheadings', '-o', 'lv_attr', self.name ]
        (ret, out) = self.call(cmd)
        if len(out) == 0 and ret == 0:
            # no lv ... happens in provisioning, where lv are not created yet
            return True
        if re.match(' ....-[-o]', out, re.MULTILINE) is None:
            return True
        return False

    def remove_tag(self, tag):
        cmd = [ 'vgchange', '--deltag', '@'+tag, self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def remove_tags(self, tags=[]):
        cmd = ['vgs', '-o', 'tags', '--noheadings', self.name]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        out = out.strip(' \n')
        curtags = out.split(',')
        if len(tags) > 0:
            """ remove only specified tags
            """
            for tag in tags:
                tag = tag.lstrip('@')
                if tag in curtags:
                   self.remove_tag(tag)
        else:
            """ remove all tags
            """
            for tag in curtags:
                if len(tag) == 0:
                    continue
                self.remove_tag(tag)

    def add_tags(self):
        cmd = [ 'vgchange', '--addtag', self.tag, self.name ]
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

    def remove_parts(self):
        if not which('partx'):
            return
        cmd = ['lvs', '-o', 'name', '--noheadings', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("can not fetch logical volume list from %s"%self.name)
            return
        base_cmd = ['kpartx', '-d']
        for lv in out.split():
             self.vcall(base_cmd+[os.path.join(os.sep, 'dev', self.name, lv)])

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return
        self.remove_tags([self.tag])
        self.remove_parts()
        cmd = [ 'vgchange', '-a', 'n', self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

        # wait for deactivation to take effect
        for i in range(3, 0, -1):
            if self.is_up() and i > 0:
                time.sleep(1)
                continue
            break
        if i == 0:
            self.log.error("deactivation failed to release all logical volumes")
            raise ex.excError

    def disklist(self):
        if not self.has_it():
            return set()
        if self.disks != set():
            return self.disks

        self.disks = set()

        cmd = ['vgs', '--noheadings', '-o', 'pv_name', self.name]
        (ret, out) = self.call(cmd, cache=True)
        if ret != 0:
            return self.disks
	pvs = set(out.split())
        self.disks = devs_to_disks(self, pvs)
        self.log.debug("found disks %s held by vg %s" % (self.disks, self.name))
        return self.disks

    def provision(self):
        m = __import__("provVgLinux")
        prov = getattr(m, "ProvisioningVg")(self)
        prov.provisioner()

