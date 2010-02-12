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
import resKvm
import rcStatus
import rcExceptions as ex

class SvcKvm(svc.Svc):
    """ Define kvm services"""

    def __init__(self, svcname=None, vmname=None, optional=False, disabled=False):
        svc.Svc.__init__(self, svcname, optional, disabled)
        if vmname is None:
            vmname = svcname
        self.vmname = vmname
        self += resKvm.Kvm(vmname)
        self.status_types = ["container.kvm", "disk.loop", "fs", "disk.scsireserv", "disk.vg", "ip", "sync.rsync", "sync.netapp"]

    def start(self):
        self.sub_set_action("ip", "start")
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.vg", "start")
        self.sub_set_action("fs", "start")
        self.sub_set_action("container.kvm", "start")
        self.sub_set_action("app", "start")

    def stop(self):
        self.sub_set_action("app", "stop")
        self.sub_set_action("container.kvm", "stop")
        self.sub_set_action("fs", "stop")
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.scsireserv", "stop")
        self.sub_set_action("disk.loop", "stop")

    def startcontainer(self):
        self.sub_set_action("container.kvm", "start")

    def stopcontainer(self):
        self.sub_set_action("container.kvm", "stop")

    def startip(self):
        self.sub_set_action("ip", "start")

    def stopip(self):
        self.sub_set_action("ip", "stop")

    def startvg(self):
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.vg", "start")

    def stopvg(self):
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.scsireserv", "stop")

    def mount(self):
        self.sub_set_action("fs", "start")

    def umount(self):
        self.sub_set_action("fs", "stop")

    def diskstart(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.vg", "start")
        self.sub_set_action("fs", "start")

    def diskstop(self):
        self.sub_set_action("fs", "stop")
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.scsireserv", "stop")

