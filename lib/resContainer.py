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
from rcUtilities import justcall
from rcGlobalEnv import rcEnv
import socket
from subprocess import *

class Container(Res.Resource):
    """ in seconds
    """
    startup_timeout = 600
    shutdown_timeout = 60

    def __init__(self, rid, name, guestos=None, type=None, optional=False,
                 disabled=False, monitor=False, tags=set([]), always_on=set([])):
        Res.Resource.__init__(self, rid=rid, type=type,
                              optional=optional, disabled=disabled,
                              monitor=monitor, tags=tags)
        self.sshbin = '/usr/bin/ssh'
        self.name = name
        self.label = name
        self.always_on = always_on
        if guestos is not None:
            self.guestos = guestos.lower()
        else:
            self.guestos = guestos
        self.runmethod = rcEnv.rsh.split() + [name]
        self.booted = False

    def vm_hostname(self):
        if hasattr(self, 'vmhostname'):
            return self.vmhostname
        if self.guestos == "windows":
            self.vmhostname = self.name
            return self.vmhostname
        cmd = self.runmethod + ['hostname']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            self.vmhostname = self.name
        else:
            self.vmhostname = out.strip()
        return self.vmhostname

    def getaddr(self):
        if hasattr(self, 'addr'):
            return
        try:
            a = socket.getaddrinfo(self.name, None)
            if len(a) == 0:
                raise Exception
            self.addr = a[0][4][0]
        except:
            if not disabled:
                raise ex.excInitError

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def operational(self):
        if self.guestos == "windows":
            """ Windows has no sshd.
            """
            return True
        timeout = 1
        if 'ssh' in self.runmethod[0]:
            cmd = [ self.sshbin, '-o', 'StrictHostKeyChecking=no',
                                 '-o', 'ForwardX11=no',
                                 '-o', 'BatchMode=yes',
                                 '-n',
                                 '-o', 'ConnectTimeout='+repr(timeout),
                                  self.name, 'pwd']
        else:
            cmd = self.runmethod + ['pwd']
        out, err, ret = justcall(cmd)
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
        self.log.info("wait for container up status")
        self.wait_for_fn(self.is_up, self.startup_timeout, 2)
        if hasattr(self, 'ping'):
            self.log.info("wait for container ping")
            self.wait_for_fn(self.ping, self.startup_timeout, 2)
        self.log.info("wait for container operational")
        self.wait_for_fn(self.operational, self.startup_timeout, 2)

    def wait_for_shutdown(self):
        self.log.info("wait for container down status")
        for tick in range(self.shutdown_timeout):
            if self.is_down():
                return
            time.sleep(1)
        self.log.error("Waited too long for shutdown")
        raise ex.excError

    def install_drp_flag(self):
        print("TODO: install_drp_flag()")

    def where_up(self):
        """ returns None if the vm is not found running anywhere
            or returns the nodename where the vm is found running
        """
        if self.is_up():
            return rcEnv.nodename
        if not hasattr(self, "is_up_on"):
            # to implement in Container child class
            return
        if rcEnv.nodename in self.svc.nodes:
            nodes = self.svc.nodes - set([rcEnv.nodename])
        elif rcEnv.nodename in self.svc.drpnodes:
            nodes = self.svc.drpnodes - set([rcEnv.nodename])
        else:
            nodes = []
        if len(nodes) == 0:
            return
        for node in nodes:
            if self.is_up_on(node):
                return node
        return

    def abort_start(self):
        nodename = self.where_up()
        if nodename is not None and nodename != rcEnv.nodename:
            return True
        return False

    def start(self):
        try:
            self.getaddr()
        except:
            self.log.error("could not resolve %s to an ip address"%self.name)
            raise ex.excError
        where = self.where_up()
        if where is not None:
            self.log.info("container %s already started on %s" % (self.name, where))
            return
        if rcEnv.nodename in self.svc.drpnodes:
            self.install_drp_flag()
        self.container_start()
        self.can_rollback = True
        self.wait_for_startup()
        self.booted = True

    def stop(self):
        try:
            self.getaddr()
        except:
            self.log.error("could not resolve %s to an ip address"%self.name)
            raise ex.excError
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
        #print("TODO: check_capabilities(self)")
        pass

    def container_start(self):
        print("TODO: container_start(self)")

    def container_stop(self):
        print("TODO: container_stop(self)")

    def container_forcestop(self):
        print("TODO: container_forcestop(self)")

    def check_manual_boot(self):
        print("TODO: check_manual_boot(self)")
        return False

    def is_up(self):
        return False

    def is_down(self):
        return not self.is_up()

    def _status(self, verbose=False):
        try:
            self.getaddr()
        except:
            self.status_log("could not resolve %s to an ip address"%self.name)
            return rcStatus.WARN
        if not self.check_capabilities():
            self.status_log("node capabilities do not permit this action")
            return rcStatus.WARN
        if not self.check_manual_boot():
            self.status_log("container auto boot is on")
            return rcStatus.WARN
        if self.is_up():
            return rcStatus.UP
        if self.is_down():
            return rcStatus.DOWN
        else:
            self.status_log("container status is neither up nor down")
            return rcStatus.WARN

    def get_container_info(self):
        print("TODO: get_container_info(self)")
        return {'vcpus': '0', 'vmem': '0'}
