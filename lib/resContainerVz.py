#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

import rcStatus
import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which
import resContainer
import rcExceptions as ex

class Vz(resContainer.Container):
    def files_to_sync(self):
        return [self.cf]

    def get_cf_value(self, param):
        value = None
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
        with open(self.cf, 'r') as f:
            for line in f:
                if 'VE_PRIVATE' in line:
                    return line.strip('\n').split('=')[1].strip('"').replace('$VEID', self.name)
        self.log.error("could not determine lxc container rootfs")
        return ex.excError

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def vzctl(self, action, options=[]):
        cmd = ['vzctl', action, self.name] + options
        ret, out = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        return out

    def container_start(self):
        self.vzctl('start')

    def container_stop(self):
        self.vzctl('stop')

    def container_forcestop(self):
        raise ex.excError

    def ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self):
        """ CTID 101 exist mounted running
        """
        cmd = ['vzctl', 'status', self.name]
        ret, out = self.call(cmd)
        if ret != 0:
            raise ex.excError
        l = out.split()
        if len(l) != 5:
            return False
        if l[2] != 'exist' or \
           l[3] != 'mounted' or \
           l[4] != 'running':
            return False
        return True

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        with open(self.cf, 'r') as f:
            for line in f:
                if 'ONBOOT' in line and 'yes' in line:
                    return False
        return True

    def check_capabilities(self):
        if not which('vzctl'):
            self.log.debug("vzctl is not in PATH")
            return False
        return True

    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        resContainer.Container.__init__(self, rid="vz", name=name, type="container.vz",
                                        optional=optional, disabled=disabled, tags=tags)
        self.cf = os.path.join(os.sep, 'etc', 'vz', 'conf', name+'.conf')
        if not os.path.exists(self.cf):
            raise ex.excInitError

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

