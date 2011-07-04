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
import resContainerLxc as lxc
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which
import os

class SvcLxc(svc.Svc):
    """ Define Lxc services"""

    def __init__(self, svcname, vmname=None, guestos=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, optional=optional, disabled=disabled, tags=tags)
        if vmname is None:
            vmname = svcname
        self.vmname = vmname
        self.guestos = guestos
        self += lxc.Lxc(vmname)
        if which('lxc-attach') and os.path.exists('/proc/1/ns/pid'):
            self.runmethod = ['lxc-attach', '-n', vmname, '--']
        else:
            self.runmethod = rcEnv.rsh.split() + [vmname]



if __name__ == "__main__":
    for c in (SvcLxc,) :
        help(c)
    import mountLinux as mount
    import ipLinux as ip
    print """
    Z=SvcLxc()
    Z+=mount.Mount("/mnt1","/dev/sda")
    Z+=mount.Mount("/mnt2","/dev/sdb")
    Z+=ip.Ip("eth0","192.168.0.173")
    Z+=ip.Ip("eth0","192.168.0.174")
    """

    Z=SvcLxc()
    Z+=mount.Mount("/mnt1","/dev/sda")
    Z+=mount.Mount("/mnt2","/dev/sdb")
    Z+=ip.Ip("eth0","192.168.0.173")
    Z+=ip.Ip("eth0","192.168.0.174")

    print "Show Z: ", Z
    print "start Z:"
    Z.start()

    print "stop Z:"
    Z.stop()

