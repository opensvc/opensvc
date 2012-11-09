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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import svc
import rcStatus
import rcCloudVcloud
import rcExceptions as ex
from rcGlobalEnv import rcEnv
import resContainerCloudVm as cloudvm

class SvcVcloud(svc.Svc):
    def __init__(self, svcname, vmname=None, cloud_id=None, guestos=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, optional=optional, disabled=disabled, tags=tags)
        if vmname is None:
            vmname = svcname
        self.cloud_id = cloud_id
        self.vmname = vmname
        self.guestos = guestos
        self += cloudvm.CloudVm(vmname, cloud_id, disabled=disabled)
        self.runmethod = rcEnv.rsh.split() + [vmname]

    def vm_hostname(self):
        if hasattr(self, 'vmhostname'):
            return self.vmhostname
        if self.guestos == "windows":
            self.vmhostname = self.vmname
            return self.vmhostname
        cmd = self.runmethod + ['hostname']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            self.vmhostname = self.vmname
        else:
            self.vmhostname = out.strip()
        return self.vmhostname
