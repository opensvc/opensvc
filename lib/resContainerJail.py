#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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

import rcStatus
import resources as Res
from rcUtilitiesFreeBSD import check_ping
from rcUtilities import qcall
import resContainer
import rcExceptions as ex

class Jail(resContainer.Container):
    """ jail -c name=jail1
                path=/usr/local/opt/jail1.opensvc.com
                host.hostname=jail1.opensvc.com
                ip4.addr=192.168.0.208
                command=/bin/sh /etc/rc
    """
    def files_to_sync(self):
        return []

    def install_drp_flag(self):
        rootfs = self.svc.jailroot
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def container_start(self):
        ips = []
        ip6s = []
        for rs in self.svc.get_res_sets("ip"):
            for r in rs.resources:
                if ':' in r.addr:
                    ip6s.append(r.addr)
                else:
                    ips.append(r.addr)
        cmd = ['jail', '-c', 'name='+self.svc.basevmname, 'path='+self.svc.jailroot,
               'host.hostname='+self.svc.vmname]
        if len(ips) > 0:
            cmd += ['ip4.addr='+','.join(ips)]
        if len(ip6s) > 0:
            cmd += ['ip6.addr='+','.join(ip6s)]
        cmd += ['command=/bin/sh', '/etc/rc']
        self.log.info(' '.join(cmd))
        ret = qcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_stop(self):
        cmd = ['jail', '-r', self.svc.basevmname]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        self.log.error("no forced stop method")
        raise ex.excError

    def ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self):
        cmd = ['jls']
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if l[2] == self.svc.vmname:
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

    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        resContainer.Container.__init__(self, rid="jail", name=name, type="container.jail",
                                        optional=optional, disabled=disabled, tags=tags)

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

