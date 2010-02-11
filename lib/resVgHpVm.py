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
resVg = __import__("resVgHP-UX")
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv
from subprocess import *

class Vg(resVg.Vg):
    def __init__(self, rid=None, name=None, type=None, scsireserv=False,
                 optional=False, disabled=False):
        self.id = name
        resVg.Vg.__init__(self, rid=rid, name=name,
                          type='disk.vg', scsireserv=scsireserv,
                          optional=optional, disabled=disabled)

    def has_it(self):
        return True

    def is_up(self):
        return True

    def dgstatus(self):
        return rcStatus.NA

    def do_start(self):
        self.do_mksf()

    def do_stop(self):
        pass

    def diskupdate(self):
        if self.svc.status() != 0:
            self.do_mksf()
        else:
            self.write_mksf()

    def disklist(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-d', '-P', self.svc.vmname]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            raise ex.excError

        for line in buff[0].split('\n'):
            l = line.split(':')
            if len(l) < 5:
                continue
            if l[3] != 'disk':
                continue
            self.disks |= set([l[4]])
        return self.disks
