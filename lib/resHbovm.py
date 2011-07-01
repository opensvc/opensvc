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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import resources as Res
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall, which
import rcOvm

class Hb(Res.Resource):
    """ HeartBeat ressource
    """
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        Res.Resource.__init__(self, rid, "hb.ovm",
                              optional=optional, disabled=disabled, tags=tags)
        self.ovsinit = os.path.join(os.sep, 'etc', 'init.d', 'ovs-agent')

    def process_running(self):
        cmd = [self.ovsinit, 'status']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return False
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            if not line.startswith('ok!'):
                return False
        return True

    def __str__(self):
        return "%s" % (Res.Resource.__str__(self))

    def stop(self):
        self.manager = rcOvm.Ovm(log=self.log)
        self.manager.vm_disable_ha(self.svc.vmname)

    def start(self):
        self.manager = rcOvm.Ovm(log=self.log)
        self.manager.vm_enable_ha(self.svc.vmname)

    def _status(self, verbose=False):
        if not os.path.exists(self.ovsinit):
            self.status_log("OVM agent is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("OVM agent daemons are not running")
            return rcStatus.WARN
        self.manager = rcOvm.Ovm(log=self.log)
        try:
            ha_enabled = self.manager.vm_ha_enabled(self.svc.vmname)
        except ex.excError, e:
            self.status_log(str(e))
            return rcStatus.WARN
        if not ha_enabled:
            self.status_log("HA not enabled for this VM")
            return rcStatus.WARN
        return rcStatus.UP

