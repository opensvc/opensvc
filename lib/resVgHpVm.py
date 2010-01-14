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
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv

class Vg(resDg.Dg):
    def __init__(self, name=None, type=None, optional=False, disabled=False, scsireserv=False):
        self.id = 'vm dg ' + name
        resDg.Dg.__init__(self, name, 'disk.vg', optional, disabled, scsireserv)

    def has_it(self):
        return True

    def is_up(self):
        return True

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def disklist(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-d', '-P', self.svc.vmname]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        for line in out.split('\n'):
            l = line.split(':')
            if len(l) < 5:
                continue
            if l[3] != 'disk':
                continue
            self.disks |= set([l[4]])
        return self.disks
