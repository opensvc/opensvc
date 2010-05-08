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
import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcUtilities import qcall
import resContainer

class Kvm(resContainer.Container):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False):
        resContainer.Container.__init__(self, rid="kvm", name=name, type="container.kvm",
                                        optional=optional, disabled=disabled)

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def ping(self):
        count=1
        timeout=1
        cmd = ['ping', '-c', repr(count), '-W', repr(timeout), '-w', repr(timeout), self.name]
        (ret, out) = self.call(cmd, errlog=False)
        if ret == 0:
            return True
        return False

    def container_start(self):
        cf = os.path.join(os.sep, 'etc', 'libvirt', 'qemu', self.name+'.xml')
        if not os.path.exists(cf):
            self.log.error("%s not found"%cf)
            raise ex.excError
        cmd = ['virsh', 'define', cf]
        (ret, buff) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        cmd = ['virsh', 'start', self.name]
        (ret, buff) = self.vcall(cmd)

    def container_stop(self):
        cmd = ['virsh', 'shutdown', self.name]
        (ret, buff) = self.vcall(cmd)

    def container_forcestop(self):
        cmd = ['virsh', 'destroy', self.name]
        (ret, buff) = self.vcall(cmd)

    def is_up(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        if "running" in out.split():
            return True
        return False

    def check_manual_boot(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        for line in out.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            if l[0] != "Autostart":
                continue
            if "disable" in l[1]:
                return True
        return False
