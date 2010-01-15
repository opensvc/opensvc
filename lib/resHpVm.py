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
import rcExceptions as ex
from rcUtilities import qcall

class HpVm(Res.Resource):
    """ HP-UX can have very long boot time
    """
    startup_timeout = 600
    shutdown_timeout = 60

    def __init__(self, name, optional=False, disabled=False):
        Res.Resource.__init__(self, "container.hpvm", optional, disabled)
        self.name = name
        self.id = "hpvm"

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def ping(self):
        count=1
        timeout=1
        cmd = ['ping', '-n', repr(count), '-m', repr(timeout), self.name]
        ret = qcall(cmd)
        if ret == 0:
            return True
        return False

    def operational(self):
        timeout=1
        cmd = ['/usr/bin/ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardX11=no', '-o', 'PasswordAuthentication=no', '-o', 'ConnectTimeout='+repr(timeout), self.name, 'pwd']
        ret = qcall(cmd)
        if ret == 0:
            return True
        return False

    def wait_for_startup(self):
        for tick in range(self.startup_timeout):
            if self.is_up() and self.ping() and self.operational():
                return
            time.sleep(1)
        raise ex.excError("Waited too long for startup")

    def wait_for_shutdown(self):
        for tick in range(self.shutdown_timeout):
            if not self.is_up():
                return
            time.sleep(1)
        raise ex.excError("Waited too long for shutdown")

    def hpvm_start(self):
        cmd = ['/opt/hpvm/bin/hpvmstart', '-P', self.name]
        (ret, buff) = self.vcall(cmd)

    def hpvm_stop(self):
        cmd = ['/opt/hpvm/bin/hpvmstop', '-F', '-P', self.name]
        (ret, buff) = self.vcall(cmd)

    def start(self):
        if self.is_up():
            self.log.info("hpvm container %s already started" % self.name)
            return 0
        self.hpvm_start()
        self.wait_for_startup()

    def stop(self):
        if not self.is_up():
            self.log.info("hpvm container %s already stopped" % self.name)
            return 0
        self.hpvm_stop()
        self.wait_for_shutdown()

    def is_up(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            return False
        if out.split(":")[10] == "On":
            return True
        return False

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

