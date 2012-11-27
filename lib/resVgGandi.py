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
import resDg
import os
import rcStatus
import re
import pwd
import grp
import stat
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Vg(resDg.Dg):
    def __init__(self, rid=None, name=None, node=None, cloud_id=None,
                 user="root", group="root", perm="660",
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False):
        self.label = "gandi volume %s"%str(name)
        resDg.Dg.__init__(self, rid=rid, name="gandi",
                          type='disk.gandi',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags,
                          monitor=monitor)

        self.name = name
        self.node = node
        self.cloud_id = cloud_id
        self.user = user
        self.group = group
        self.perm = perm
        
        self.get_uid()
        self.get_gid()

    def print_obj(self, n):
        for k in dir(n):
            if '__' in k:
                continue
            print k, "=", getattr(n, k)

    def get_cloud(self):
        if hasattr(self, 'cloud'):
            return self.cloud
        c = self.svc.node.cloud_get(self.cloud_id)
        self.cloud = c
        return self.cloud

    def get_uid(self):
        self.uid = self.user
        if isinstance(self.uid, (str, unicode)):
            try:
                info=pwd.getpwnam(self.uid)
                self.uid = info[2]
            except:
                pass

    def get_gid(self):
        self.gid = self.group
        if isinstance(self.gid, (str, unicode)):
            try:
                info=grp.getgrnam(self.gid)
                self.gid = info[2]
            except:
                pass

    def check_uid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        uid = os.stat(rdev).st_uid
        if uid != self.uid:
            if verbose:
                self.status_log('%s uid should be %d but is %d'%(rdev, self.uid, uid))
            return False
        return True

    def check_gid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        gid = os.stat(rdev).st_gid
        if gid != self.gid:
            if verbose:
                self.status_log('%s gid should be %d but is %d'%(rdev, self.gid, gid))
            return False
        return True

    def check_perm(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        try:
            perm = oct(stat.S_IMODE(os.stat(rdev).st_mode))
        except:
            self.log.error('%s can not stat file'%rdev)
            return False
        perm = str(perm).lstrip("0")
        if perm != str(self.perm):
            if verbose:
                self.status_log('%s perm should be %s but is %s'%(rdev, str(self.perm), perm))
            return False
        return True

    def has_it(self):
        """Returns True if all devices are present
        """
        try:
            node = self.get_node()
        except:
            raise ex.excError("can't find cloud node to list volumes")

        c = self.get_cloud()
        disks = c.driver._node_info(node.id)['disks']
        for disk in disks:
            if disk['name'] == self.name:
                return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        return self.has_it()

    def _status(self, verbose=False):
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def get_node(self):
        c = self.get_cloud()
        if self.node is not None:
            n = self.node
        else:
            n = rcEnv.nodename
        try:
            nodes = c.driver.list_nodes()
        except:
            raise ex.excError()
        for node in nodes:
            if node.name == n:
                return node
        raise ex.excError()

    def get_disk(self):
        c = self.get_cloud()
        disks = c.driver.ex_list_disks()
        _disk = None
        for disk in disks:
            if disk.name == self.name:
                _disk = disk
        if _disk is None:
            raise ex.excError()
        return _disk
 
    def do_start(self):
        try:
            node = self.get_node()
        except:
            raise ex.excError("can't find cloud node to attach volume %s to"%(self.name))

        try:
            disk = self.get_disk()
        except:
            raise ex.excError("volume %s not found in %s"%(self.name, self.cloud_id))

        self.log.info("attach gandi volume %s"%self.name)
        c = self.get_cloud()
        c.driver.ex_node_attach_disk(node, disk)

    def do_stop(self):
        try:
            node = self.get_node()
        except:
            raise ex.excError("can't find cloud node to detach volume %s from"%(self.name))

        try:
            disk = self.get_disk()
        except:
            raise ex.excError("volume %s not found in %s"%(self.name, self.cloud_id))

        self.log.info("detach gandi volume %s"%self.name)
        c = self.get_cloud()
        c.driver.ex_node_detach_disk(node, disk)

    def shutdown(self):
        pass

    def disklist(self):
        return []
