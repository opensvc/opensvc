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
    def __init__(self, rid=None, name=None, type=None,
                 always_on=set([]), scsireserv=False,
                 disabled=False, optional=False):
        self.id = 'vg ' + name
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled,
                          scsireserv=scsireserv)

    def mapfile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.map')

    def mkfsfile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.mksf')

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

    def dsf_name(self, dev):
        cmd = ['scsimgr', 'get_attr', '-D', dev, '-a', 'device_file', '-p']
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        return out.split()[0]

    def write_mksf(self):
        cmd = ['ioscan', '-F', '-m', 'dsf']
        (ret, buff) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        if len(buff) == 0:
            return
        mksf = {}
        if len(self.disks) == 0:
            self.disks = self.disklist()
        dsf_names = map(self.dsf_name, self.disks)
        with open(self.mkfsfile_name(), 'w') as f:
            for line in buff.split('\n'):
                if len(line) == 0:
                    return
                a = line.split(':')[0]
                if '/dev/pt/' not in a and '/dev/rdisk/disk' not in a and self.dsf_name(a) in dsf_names:
                    cmd = ['scsimgr', 'get_attr', '-D', a, '-a', 'wwid', '-p']
                    (ret, out) = self.call(cmd)
                    if ret != 0:
                        raise ex.excError
                    f.write(":".join([a, out.split()[0].replace('0x', '')])+'\n')

    def do_mksf(self):
        if not os.path.exists(self.mkfsfile_name()):
            return

        instance = {}
        cmd = ['scsimgr', 'get_attr', 'all_lun', '-a', 'wwid', '-a', 'instance', '-p']
        (ret, buff) = self.call(cmd)
        for line in buff.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            instance[l[0].replace('0x', '')] = l[1]

        with open(self.mkfsfile_name(), 'r') as f:
            for line in f.readlines():
               a = line.replace('\n', '').split(':')
               if len(a) == 0:
                   continue
               if os.path.exists(a[0]):
                   continue
               if a[1] not in instance.keys():
                   self.log.error("expected lun %s not present on this node"%a[1])
                   raise ex.excError
               cmd = ['mksf', '-r', '-C', 'disk', '-I', instance[a[1]], a[0]]
               (ret, buff) = self.vcall(cmd)
               if ret != 0:
                   raise ex.excError

    def diskupdate(self):
        """ this one is exported as a service command line arg
        """
        cmd = [ 'vgexport', '-m', self.mapfile_name(), '-p', '-s', self.name ]
        ret = qcall(cmd)
        if ret != 0:
            raise ex.excError
        self.write_mksf()

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

    def start(self):
        self.do_mksf()
        self.scsireserv()
        self.do_start()

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
