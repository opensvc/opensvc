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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import svc
import resContainerJail as jail

class SvcJail(svc.Svc):
    """ Define Jail services"""

    def __init__(self, svcname, vmname=None, guestos=None, jailroot=None,
                 optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, optional=optional, disabled=disabled, tags=tags)
        if vmname is None:
            vmname = svcname
        self.vmname = vmname
        self.guestos = guestos
        self.jailroot = jailroot
        self += jail.Jail(vmname, disabled=disabled)

        """ jail names cannot have dots
        """
        self.basevmname = vmname.split('.')[0]

    def vmcmd(self, cmd, verbose=False, timeout=10, r=None):
        runmethod = ['jexec', self.basevmname]
        return self.call(runmethod+[cmd], verbose=verbose, log=r.log)

if __name__ == "__main__":
    for c in (SvcJail,) :
        help(c)

