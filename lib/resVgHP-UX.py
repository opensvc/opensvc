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
        self.id = 'vg ' + name
        resDg.Dg.__init__(self, name, 'disk.vg', optional, disabled, scsireserv)

    def mapfile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.map')

    def has_it(self):
        """ returns True if the volume is present
        """
        if self.is_active():
            return True
        if not os.path.exists(self.mapfile_name()):
            return False
        if self.is_imported():
            return True
        return False

    def diskupdate(self):
        """ this one is exported as a service command line arg
        """
        cmd = [ 'vgexport', '-m', self.mapfile_name(), '-p', '-s', self.name ]
        ret = qcall(cmd)
        if ret != 0:
            raise ex.excError

    def is_active(self):
        cmd = [ 'vgdisplay', self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "available" in buff[0]:
            return False
        return True

    def is_imported(self):
        if not os.path.exists(self.mapfile_name()):
            return False
        if self.dsf:
            dsfflag = '-N'
        else:
            dsfflag = ''
        cmd = [ 'vgimport', '-m', self.mapfile_name(), '-s', '-p', dsfflag, self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "already exists" in buff[1]:
            return False
        return True

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
        if self.dsf:
            dsfflag = '-N'
        else:
            dsfflag = ''
        cmd = [ 'vgimport', '-m', self.mapfile_name(), '-s', dsfflag, self.name ]
        self.log.info(' '.join(cmd))
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if len(buff[1]) > 0 and buff[1] != "vgimport: ":
            self.log.error('error:\n' + buff[1])
        if len(buff[0]) > 0:
            self.log.debug('output:\n' + buff[0])
        if process.returncode != 0:
            raise ex.excError

    def do_export(self):
        if not self.is_imported():
            self.log.info("%s is already exported" % self.name)
            return
        cmd = [ 'vgexport', '-m', self.mapfile_name(), '-s', self.name ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_activate(self):
        if self.is_active():
            self.log.info("%s is already available" % self.name)
            return
        cmd = ['vgchange', '-a', 'y', self.name]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_deactivate(self):
        if not self.is_active():
            self.log.info("%s is already unavailable" % self.name)
            return
        cmd = ['vgchange', '-a', 'n', self.name]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        self.do_import()
        self.do_activate()

    def do_stop(self):
        self.do_deactivate()
        self.do_export()

    def disklist(self):
        need_export = False
        if not self.is_imported():
            self.do_import()
            need_export = True
        cmd = ['strings', '/etc/lvmtab']
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        tab = out.split('\n')
        insection = False
        self.disks = set([])
        for e in tab:
            """ move to the first disk of the vg
            """
            if e == '/dev/'+self.name:
                 insection = True
                 continue
            if not insection:
                 continue
            if e == "_KDI":
                 continue
            if "/dev/dsk" not in e and "/dev/disk" not in e:
                 break
            self.disks |= set([e])

        if need_export:
            self.do_export()
        return self.disks
