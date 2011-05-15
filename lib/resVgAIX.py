#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>
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
import rcExceptions as ex
import resDg
from subprocess import *

class Vg(resDg.Dg):
    def __init__(self, rid=None, name=None, type=None,
                 always_on=set([]), dsf=True,
                 disabled=False, tags=set([]), optional=False):
        self.label = name
        self.dsf = dsf
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags)

    def has_it(self):
        """ returns True if the volume is present
        """
        if self.is_active():
            return True
        if self.is_imported():
            return True
        return False

    def is_active(self):
        cmd = [ 'lsvg', self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "active" in buff[0]:
            return False
        return True

    def is_imported(self):
        cmd = ['lsvg']
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        for vg in buff[0].split('\n'):
            if vg == self.name:
                return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.is_imported():
            return False
        if not self.is_active():
            return False
        return True

    def do_import(self):
        if self.is_imported():
            self.log.info("%s is already imported" % self.name)
            return
        cmd = ['importvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_export(self):
        if not self.is_imported():
            self.log.info("%s is already exported" % self.name)
            return
        cmd = ['exportvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_activate(self):
        if self.is_active():
            self.log.info("%s is already available" % self.name)
            return
        cmd = ['varyonvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_deactivate(self):
        if not self.is_active():
            self.log.info("%s is already unavailable" % self.name)
            return
        cmd = ['varyoffvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        self.do_import()
        self.do_activate()

    def do_stop(self):
        self.do_deactivate()

    def start(self):
        self.do_start()

    def disklist(self):
        cmd = ['lsvg', '-p', self.name]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        for e in out.split('\n'):
            x = e.split()
            if len(x) != 5:
                continue
            self.disks |= set([x[0]])

        return self.disks
