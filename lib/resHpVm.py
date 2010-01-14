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


class HpVm(Res.Resource):
    shutdown_timeout = 60
    startup_timeout = 60

    def __init__(self, name, optional=False, disabled=False):
        Res.Resource.__init__(self, "container.hpvm", optional, disabled)
        self.name = name
        self.id = "hpvm"

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def wait_for_startup(self):
        self.log.warn("implement wait_for_startup")
        pass

    def wait_for_shutdown(self):
        self.log.warn("implement wait_for_shutdown")
        pass

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
        return self.wait_for_startup()

    def stop(self):
        if not self.is_up():
            self.log.info("hpvm container %s already stopped" % self.name)
            return 0
        self.hpvm_stop()
        return self.wait_for_shutdown()

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

