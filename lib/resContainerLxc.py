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

import rcStatus
import resources as Res
from rcUtilitiesLinux import check_ping
import resContainer

class Lxc(resContainer.Container):
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

    def container_start(self):
        self.lxc('start')

    def container_stop(self):
        self.lxc('stop')

    def ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self):
        self.log.debug("call: lxc-ps --name %s | grep %s" % (self.name, self.name))
        p1 = Popen(['lxc-ps', '--name', self.name], stdout=PIPE)
        p2 = Popen(["grep", self.name], stdin=p1.stdout, stdout=PIPE)
        p2.communicate()[0]
        if p2.returncode == 0:
            return True
        return False

    def get_container_info(self):
        print "TODO: get_container_info()"
        return {'vcpus': '0', 'vmem': '0'}

    def _status(self, verbose=False):
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def __init__(self, name, optional=False, disabled=False):
        resContainer.Container.__init__(self, rid="lxc", name=name, type="container.lxc",
                                        optional=optional, disabled=disabled)

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

