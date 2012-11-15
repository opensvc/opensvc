#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
    def __init__(self, rid=None, name=None, container_id=None, type=None,
                 optional=False, disabled=False, tags=set([]),
                 monitor=False):
        self.label = name
        self.container_id = container_id
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

    def devmap(self):
        if hasattr(self, "devmapping"):
            return self.devmapping

        self.devmapping = []

        cf = self.svc.resources_by_id[self.container_id].find_vmcf()
        with open(cf, 'r') as f:
            buff = f.read()

        for line in buff.split('\n'):
            if not line.startswith('disk'):
                continue
            disks = line[line.index('['):]
            if len(line) <= 2:
                break
            disks = disks[1:-1]
            disks = disks.split(', ')
            for disk in disks:
                disk = disk.strip("'")
                d = disk.split(',')
                if not d[0].startswith('phy:'):
                    continue
                l = [d[0].strip('phy:'), d[1]]
                self.devmapping.append(l)
            break

        return self.devmapping

    def devlist(self):
        if self.devs != set():
            return self.devs
        self.devs = set(map(lambda x: x[0], self.devmap()))
        return self.devs

    def disklist(self):
        if self.disks != set():
            return self.disks

        self.disks = set()
        devps = self.devlist()

        try:
	    u = __import__('rcUtilities'+rcEnv.sysname)
            self.disks = u.devs_to_disks(self, devps)
        except:
            self.disks = devps

        return self.disks
