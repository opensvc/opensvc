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
import lxc
import rcStatus

class SvcLxc(svc.Svc):
    """ Define Lxc services"""
    status_types = ["container.lxc", "disk.loop", "mount", "disk.vg", "ip"]

    def __init__(self, svcname, optional=False, disabled=False):
        svc.Svc.__init__(self, svcname, optional, disabled)
        self += lxc.Lxc(svcname)

    def start(self):
        """start a Lxc
        check ping
        start loops
        start VGs
        start mounts
        start lxc
        start apps
        """
        self.sub_set_action("ip", "check_ping")
        self.sub_set_action("disk.loop", "start")
        self.sub_set_action("disk.vg", "start")
        self.sub_set_action("mount", "start")
        self.sub_set_action("container.lxc", "start")
        self.sub_set_action("app", "start")

    def stop(self):
        """stop a zone:
        stop apps
        stop lxc
        stop mounts
        stop VGs
        stop loops
        """
        self.sub_set_action("app", "stop")
        self.sub_set_action("container.lxc", "stop")
        self.sub_set_action("mount", "stop")
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.loop", "stop")

    def status(self):
        """status of the resources of a Lxc service"""
        return svc.Svc.status(self, self.status_types)

    def print_status(self):
        """status of the resources of a Lxc service"""
        return svc.Svc.print_status(self, self.status_types)

    def group_status(self):
        """status of the resources of a Lxc service"""
        return svc.Svc.group_status(self, self.status_types)

    def startlxc(self):
        self.sub_set_action("container.lxc", "start")

    def stoplxc(self):
        self.sub_set_action("container.lxc", "stop")

    def startip(self):
        self.sub_set_action("ip", "check_ping")
        self.sub_set_action("ip", "start")

    def stopip(self):
        self.sub_set_action("ip", "stop")

    def startloop(self):
        self.sub_set_action("disk.loop", "start")

    def stoploop(self):
        self.sub_set_action("disk.loop", "stop")

    def startvg(self):
        self.sub_set_action("disk.vg", "start")

    def stopvg(self):
        self.sub_set_action("disk.vg", "stop")

    def mount(self):
        self.sub_set_action("mount", "start")

    def umount(self):
        self.sub_set_action("mount", "stop")


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

