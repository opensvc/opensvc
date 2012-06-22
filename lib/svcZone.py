#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@opensvc.com>'
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

__author__="cgaliber"
__date__ ="$11 oct. 2009 21:56:59$"

import svc
import resContainerZone as Zone

class SvcZone(svc.Svc):
    """ Define Zone services"""

    def __init__(self, svcname, vmname=None, guestos=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, type="container.zone",
            optional=optional, disabled=disabled, tags=tags)
        if vmname is None:
            vmname = svcname
        self.vmname = vmname
        self.guestos = guestos
        self.zone = Zone.Zone(vmname, disabled=disabled)
        self += self.zone
        self.runmethod = [ '/usr/sbin/zlogin', '-S', vmname ]

    def start(self):
        """start a zone
        check ping
        zone ready
        start ips
        start zone
        start VGs
        start fs
        start apps
        """
        self.sub_set_action("ip", "check_not_ping_raise")
        af_svc = self.get_non_affine_svc()
        if len(af_svc) != 0:
            self.log.error("refuse to start %s on the same node as %s"%(self.svcname, ', '.join(af_svc)))
            return
        self.sub_set_action("disk.scsireserv", "start", tags=set(['preboot']))
        self.sub_set_action("disk.vg", "start", tags=set(['preboot']))
        self.sub_set_action("disk.zpool", "start", tags=set(['preboot']))
        self.sub_set_action("fs", "start", tags=set(['preboot']))
        self.sub_set_action("container.zone", "attach")
        self.sub_set_action("container.zone", "ready")
        self.sub_set_action("ip", "start")
        self.sub_set_action("container.zone", "boot")
        self.sub_set_action("sync.netapp", "start", tags=set(['postboot']))
        self.sub_set_action("disk.scsireserv", "start", tags=set(['postboot']))
        self.sub_set_action("disk.vg", "start", tags=set(['postboot']))
        self.sub_set_action("disk.zpool", "start", tags=set(['postboot']))
        self.sub_set_action("fs", "start")
        self.sub_set_action("app", "start")

    def stop(self):
        """stop a zone:
        stop apps
        stop fs
        stop VGs
        stop zone
        stop ips
        """
        self.sub_set_action("app", "stop")
        self.sub_set_action("fs", "stop", tags=set(['postboot']))
        self.sub_set_action("disk.vg", "stop", tags=set(['postboot']))
        self.sub_set_action("disk.zpool", "stop", tags=set(['postboot']))
        self.sub_set_action("disk.scsireserv", "stop", tags=set(['postboot']))
        self.sub_set_action("container.zone", "stop")
        self.sub_set_action("container.zone", "detach")
        self.sub_set_action("fs", "stop", tags=set(['preboot']))
        self.sub_set_action("disk.vg", "stop", tags=set(['preboot']))
        self.sub_set_action("disk.zpool", "stop", tags=set(['preboot']))
        self.sub_set_action("disk.scsireserv", "stop", tags=set(['preboot']))
        self.sub_set_action("ip", "stop")

    def startip(self):
        self.sub_set_action("ip", "check_ping")
        self.sub_set_action("container.zone", "attach")
        self.sub_set_action("container.zone", "ready")
        self.sub_set_action("ip", "start")

if __name__ == "__main__":
    for c in (SvcZone,) :
        help(c)
    import mountSunOS as mount
    import ipSunOS as ip
    print """
    Z=SvcZone()
    Z+=mount.Mount("/mnt1","/dev/sda")
    Z+=mount.Mount("/mnt2","/dev/sdb")
    Z+=ip.Ip("eth0","192.168.0.173")
    Z+=ip.Ip("eth0","192.168.0.174")
    """

    Z=SvcZone()
    Z+=mount.Mount("/mnt1","/dev/sda")
    Z+=mount.Mount("/mnt2","/dev/sdb")
    Z+=ip.Ip("eth0","192.168.0.173")
    Z+=ip.Ip("eth0","192.168.0.174")

    print "Show Z: ", Z
    print "start Z:"
    Z.start()

    print "stop Z:"
    Z.stop()

