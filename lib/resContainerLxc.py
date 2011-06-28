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

import sys
import rcStatus
import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which
import resContainer
import rcExceptions as ex

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

    def files_to_sync(self):
        return [self.cf]

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
        (ret, out, err) = self.vcall(cmd)
        len = datetime.now() - t
        self.log.info('%s done in %s - ret %i - logs in %s' % (action, len, ret, outf))
        if ret != 0:
            raise ex.excError

    def get_cf_value(self, param):
        value = None
        if not os.path.exists(self.cf):
            return None
        with open(self.cf, 'r') as f:
            for line in f.readlines():
                if param not in line:
                    continue
                if line.strip()[0] == '#':
                    continue
                l = line.replace('\n', '').split('=')
                if len(l) < 2:
                    continue
                if l[0].strip() != param:
                    continue
                value = ' '.join(l[1:]).strip()
                break
        return value

    def get_rootfs(self):
        rootfs = self.get_cf_value("lxc.rootfs")
        if rootfs is None:
            self.log.error("could not determine lxc container rootfs")
            raise ex.excError
        return rootfs

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def container_start(self):
        self.lxc('start')

    def container_stop(self):
        self.lxc('stop')

    def container_forcestop(self):
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        raise ex.excError

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
        cpu_set = self.get_cf_value("lxc.cgroup.cpuset.cpus")
        if cpu_set is None:
            vcpus = 0
        else:
            vcpus = len(cpu_set.split(','))
        return {'vcpus': str(vcpus), 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        if not which('lxc-info'):
            self.log.debug("lxc-info is not in PATH")
            return False
        return True

    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        resContainer.Container.__init__(self, rid="lxc", name=name, type="container.lxc",
                                        optional=optional, disabled=disabled, tags=tags)
        self.prefix = None
        hint = os.path.join(os.sep, 'usr', 'bin', 'lxc-start')
        if os.path.exists(hint):
            self.prefix = os.sep
        else:
            hint = os.path.join(os.sep, 'usr', 'local', 'bin', 'lxc-start')
            if os.path.exists(hint):
                self.prefix = os.path.join(os.sep, 'usr', 'local')
        if self.prefix is None:
            print >>sys.stderr, "lxc install prefix not found"
            raise ex.excInitError
        self.d_lxc = os.path.join(self.prefix, 'var', 'lib', 'lxc')
        self.cf = os.path.join(self.d_lxc, name, 'config')
        if not os.path.exists(self.d_lxc):
            os.makedirs(self.d_lxc)

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def provision(self):
        m = __import__("provLxc")
        prov = m.ProvisioningLxc(self)
        prov.provisioner()
