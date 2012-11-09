#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import qcall
from rcUtilitiesLinux import check_ping
import resContainer
import rcCloudOpenstack as rccloud

class CloudVm(resContainer.Container):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, cloud_id, optional=False, disabled=False, monitor=False,
                 tags=set([])):
        resContainer.Container.__init__(self, rid="cloudvm", name=name,
                                        type="container.openstack",
                                        optional=optional, disabled=disabled,
                                        monitor=monitor, tags=tags)
        self.cloud_id = cloud_id

    def get_cloud(self):
        if hasattr(self, 'cloud'):
            return self.cloud
        c = self.svc.node.cloud_get(self.cloud_id)
        self.cloud = c
        return self.cloud

    def get_node(self):
        c = self.get_cloud()
        l = c.list_nodes()
        for n in l:
            if n.name == self.name:
                return n
        return

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def getaddr(self):
        if hasattr(self, 'addr'):
            return
        n = self.get_node()
        if len(n.public_ip) > 0:
            self.addr = n.public_ip

    def files_to_sync(self):
        return []

    def check_capabilities(self):
        return True

    def ping(self):
        return check_ping(self.addr, timeout=1, count=1)

    def container_start(self):
        print "not implemented"
        n = self.get_node()

    def container_stop(self):
        n = self.get_node()
        raise ex.excError("not implemented")

    def container_forcestop(self):
        n = self.get_node()
        raise ex.excError("not implemented")

    def is_up(self):
        n = self.get_node()
        if n.state == 0:
            return True
        return False

    def get_container_info(self):
        n = self.get_node()
        # n.size is empty
        self.info = {'vcpus': '0', 'vmem': '0'}
        return self.info

    def check_manual_boot(self):
        return True

    def install_drp_flag(self):
        pass

    def provision(self):
        m = __import__("provCloudOpenstack")
        prov = m.ProvisioningCloudVm(self)
        prov.provisioner()

