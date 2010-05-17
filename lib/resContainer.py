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

class Container(Res.Resource):
    """ in seconds
    """
    startup_timeout = 600
    shutdown_timeout = 60

    def __init__(self, name, rid=None, type=None, optional=False, disabled=False):
        Res.Resource.__init__(self, rid=rid, type=type,
                              optional=optional, disabled=disabled)
        self.name = name
        self.label = name

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def operational(self):
        timeout = 1
        cmd = ['/usr/bin/ssh', '-o', 'StrictHostKeyChecking=no',
                               '-o', 'ForwardX11=no',
                               '-o', 'BatchMode=yes',
                               '-o', 'ConnectTimeout='+repr(timeout), self.name, 'pwd']
        ret = qcall(cmd)
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
            if self.is_down():
                return
            time.sleep(1)
        self.log.error("Waited too long for shutdown")
        raise ex.excError

    def start(self):
        if self.is_up():
            self.log.info("container %s already started" % self.name)
            return
        self.container_start()
        self.wait_for_startup()

    def stop(self):
        if self.is_down():
            self.log.info("container %s already stopped" % self.name)
            return
        self.container_stop()
        try:
            self.wait_for_shutdown()
        except ex.excError:
            self.container_forcestop()
            self.wait_for_shutdown()

    def check_capabilities(self):
        #print "TODO: check_capabilities(self)"
        pass

    def container_start(self):
        print "TODO: container_start(self)"

    def container_stop(self):
        print "TODO: container_stop(self)"

    def check_manual_boot(self):
        print "TODO: check_manual_boot(self)"
        return False

    def is_up(self):
        print "TODO: is_up(self)"
        return False

    def is_down(self):
        return not self.is_up()

    def _status(self, verbose=False):
        if not self.check_capabilities():
            return rcStatus.WARN
        if not self.check_manual_boot():
            self.status_log("container auto boot is on")
            return rcStatus.WARN
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def get_container_info(self):
        print "TODO: get_container_info(self)"
        return {'vcpus': '0', 'vmem': '0'}
