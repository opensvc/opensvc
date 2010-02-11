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
import os
from datetime import datetime
from subprocess import *

import rcLogger
import rcStatus
import resources as Res

def lxc(self, action):
    outf = '/var/tmp/svc_'+self.name+'_lxc_'+action+'.log'
    if action == 'start':
        cmd = ['lxc-start', '-d', '-n', self.name, '-o', outf]
    elif action == 'stop':
        cmd = ['lxc-stop', '-n', self.name, '-o', outf]
    else:
        self.log.error("unsupported lxc action: %s" % action)
        return 1

    t = datetime.now()
    (ret, out) = self.vcall(cmd)
    len = datetime.now() - t
    self.log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
    return ret

def lxc_rootfs_path(self):
    return os.path.realpath(os.path.join(self.pathlxc, self.name, 'rootfs','rootfs'))

def lxc_is_created(self):
    cmd = [ 'lxc-info', '-n', self.name ]
    (ret, out) = self.call(cmd)
    return ret

def lxc_wait_for_startup(self):
    for t in range(self.startup_timeout):
        if self.is_up(): return 0
    log.error("timeout out waiting for %s startup", self.name)
    return 1

def lxc_wait_for_shutdown(self):
    for t in range(self.startup_timeout):
        if not self.is_up(): return 0
    log.error("timeout out waiting for %s shutdown", self.name)
    return 1

def lxc_exec(self, cmd):
    pass

class Lxc(Res.Resource):
    """
     container status transition diagram :
       ---------
      | STOPPED |<---------------
       ---------                 |
           |                     |
         start                   |
           |                     |
           V                     |
       ----------                |
      | STARTING |--error-       |
       ----------         |      |
           |              |      |
           V              V      |
       ---------    ----------   |
      | RUNNING |  | ABORTING |  |
       ---------    ----------   |
           |              |      |
      no process          |      |
           |              |      |
           V              |      |
       ----------         |      |
      | STOPPING |<-------       |
       ----------                |
           |                     |
            ---------------------
    """
    pathlxc = os.path.join('usr', 'local', 'var', 'lib', 'lxc')
    shutdown_timeout = 60
    startup_timeout = 60

    def start(self):
        if self.is_up():
            self.log.info("lxc container %s already started" % self.name)
            return 0
        lxc(self, 'start')
        return lxc_wait_for_startup(self)

    def stop(self):
        if not self.is_up():
            self.log.info("lxc container %s already stopped" % self.name)
            return 0
        lxc(self, 'stop')
        return lxc_wait_for_shutdown(self)

    def is_up(self):
        self.log.debug("call: lxc-ps --name %s | grep %s" % (self.name, self.name))
        p1 = Popen(['lxc-ps', '--name', self.name], stdout=PIPE)
        p2 = Popen(["grep", self.name], stdin=p1.stdout, stdout=PIPE)
        p2.communicate()[0]
        if p2.returncode == 0:
            return True
        return False

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def __init__(self, svcname, optional=False, disabled=False):
        Res.Resource.__init__(self, rid="lxc", type="container.lxc",
                              optional=optional, disabled=disabled)
        self.name = svcname
        self.label = svcname

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

