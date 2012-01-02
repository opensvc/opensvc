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
import resContainerKvm as kvm
import rcStatus
import rcExceptions as ex
from rcGlobalEnv import rcEnv

class SvcKvm(svc.Svc):
    """ Define kvm services"""

    def __init__(self, svcname, vmname=None, guestos=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, optional=optional, disabled=disabled, tags=tags)
        if vmname is None:
            vmname = svcname
        self.vmname = vmname
        self.guestos = guestos
        self += kvm.Kvm(vmname, disabled=disabled)

    def vmcmd(self, cmd, verbose=False, timeout=10, r=None):
        return sshcmd(cmd, verbose, timeout, r)


