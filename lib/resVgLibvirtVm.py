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
import rcStatus
resVg = __import__("resDg")
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv
from subprocess import *

class Vg(resVg.Dg):
    def __init__(self, rid=None, name=None, type=None,
                 optional=False, disabled=False, tags=set([]),
                 monitor=False):
        self.label = name
        resVg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          optional=optional, disabled=disabled, tags=tags,
                          monitor=monitor)

    def has_it(self):
        return True

    def is_up(self):
        return True

    def _status(self, verbose=False):
        return rcStatus.NA

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def devlist(self):
        if self.devs != set():
            return self.devs

        self.devs = set()

        from xml.etree.ElementTree import ElementTree, SubElement
        tree = ElementTree()
        tree.parse(self.svc.resources_by_id['container'].cf)
        for dev in tree.getiterator('disk'):
            s = dev.find('source')
            if s is None:
                 continue
            il = s.items()
            if len(il) != 1:
                 continue
            attr, devp = il[0]
            self.devs |= set([devp])
        return self.devs

    def disklist(self):
        devps = self.devs
        try:
	    u = __import__('rcUtilities'+rcEnv.sysname)
            self.disks = u.devs_to_disks(self, devps)
        except:
            self.disks = devps

        return self.disks
