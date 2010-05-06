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

class Kvm(Res.Resource):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False):
        Res.Resource.__init__(self, rid="kvm", type="container.kvm",
                              optional=optional, disabled=disabled)
        self.name = name
        self.label = name

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

    def operational(self):
        timeout=1
        cmd = ['/usr/bin/ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardX11=no', '-o', 'PasswordAuthentication=no', '-o', 'ConnectTimeout='+repr(timeout), self.name, 'pwd']
        (ret, out) = self.call(cmd, errlog=False)
        if ret == 0:
            return True
        return False

    def wait_for_fn(self, fn, tmo, delay):
        for tick in range(tmo//2):
            if fn():
                return
            time.sleep(delay)
        self.log.error("Waited too long for startup")
        raise ex.excError

    def wait_for_startup(self):
        self.wait_for_fn(self.is_up, self.startup_timeout, 2)
        self.wait_for_fn(self.ping, self.startup_timeout, 2)
        self.wait_for_fn(self.operational, self.startup_timeout, 2)

    def wait_for_shutdown(self):
        for tick in range(self.shutdown_timeout):
            if not self.is_up():
                return
            time.sleep(1)
        self.log.error("Waited too long for shutdown")
        raise ex.excError

    def kvm_start(self):
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

    def kvm_stop(self):
        cmd = ['virsh', 'shutdown', self.name]
        (ret, buff) = self.vcall(cmd)

    def start(self):
        if self.is_up():
            self.log.info("kvm container %s already started" % self.name)
            return
        self.kvm_start()
        self.wait_for_startup()

    def stop(self):
        if not self.is_up():
            self.log.info("kvm container %s already stopped" % self.name)
            return 0
        self.kvm_stop()
        self.wait_for_shutdown()

    def is_up(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        if "running" in out.split():
            return True
        return False

    def status(self, verbose=False):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

