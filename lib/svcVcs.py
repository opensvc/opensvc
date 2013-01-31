#
# Copyright (c) 2013 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

import os
import svc
import rcExceptions as ex
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

class SvcVcs(svc.Svc):

    def __init__(self, svcname, pkg_name=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, "vcs", optional=optional, disabled=disabled, tags=tags)
        self.pkg_name = pkg_name
        self.domainname = None
        self.builder()

    def get_res_val(self, res, p):
        cmd = ['hares', '-value', res, p]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        return out.strip()

    def get_grp_val(self, p):
        cmd = ['hagrp', '-value', self.pkg_name, p]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        return out.strip()

    def get_domainname(self):
        if self.domainname is not None:
            return self.domainname
        cmd = ['hostname', '-d']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.domainname = out
        return out.strip()

    def set_nodes(self):
        s = self.get_grp_val('SystemList')
        l = s.split()

        # SystemList goes in system/weight pairs
        if len(l) < 2 or len(l) % 2 != 0:
            raise ex.excError("unexpected SystemList value: %s"%s)

        self.nodes = set([])
        domainname = self.get_domainname()
        for i, w in enumerate(l):
            if i % 2 == 1:
                continue
            if len(domainname) > 0 and not w.endswith(domainname):
                w += '.' + domainname
            self.nodes.add(w)

    def builder(self):
        if self.pkg_name is None:
            self.error("pkg_name is not set")
            raise ex.excInitError()
        self.set_nodes()
        self.load_hb()
        self.load_resources()

    def load_hb(self):
        rid = 'hb#sg0'
        m = __import__("resHbVcs")
        r = m.Hb(rid, self.pkg_name)
        self += r

    def load_resources(self):
        cmd = ['hagrp', '-resources', self.pkg_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        resource_names = out.strip().split('\n')
        for resource_name in resource_names:
            self.load_resource(resource_name)

    def load_resource(self, name):
        n_ip = 0
        n_fs = 0
        n_vg = 0
        s = self.get_res_val(name, 'Type')
        if s == 'Mount':
            self.load_fs(name, n_fs)
            n_fs += 1
        elif s == 'DiskGroup':
            self.load_vg(name, n_vg)
            n_vg += 1

    def load_vg(self, name, n):
        vgname = self.get_res_val(name, 'DiskGroup')
        disabled = True if self.get_res_val(name, 'Enabled') == "0" else False
        monitor = True if self.get_res_val(name, 'Critical') == "1" else False
        rid = 'vg#vcs%d'%n
        m = __import__("resVgVcs"+rcEnv.sysname)
        r = m.Vg(rid, vgname, disabled=disabled, monitor=monitor)
        r.vcs_name = name
        self += r

    def load_ip(self, name, n):
        """
        <ip address="10.105.133.5" monitor_link="0">
        """
        if 'ref' in e.attrib:
            # load ref xml node and recurse
            return
        if not 'address' in e.attrib:
            return
        ipname = e.attrib['address']

        n = self.n_ip
        rid = 'ip#vcs%d'%n
        m = __import__("resIpVcs"+rcEnv.sysname)
        r = m.Ip(rid, "", ipname, "")
        r.monitor = True
        self += r
        self.n_ip += 1

    def load_fs(self, name, n):
        dev = self.get_res_val(name, 'BlockDevice')
        mnt = self.get_res_val(name, 'MountPoint')
        mntopt = self.get_res_val(name, 'MountOpt')
        fstype = self.get_res_val(name, 'FSType')
        disabled = True if self.get_res_val(name, 'Enabled') == "0" else False
        monitor = True if self.get_res_val(name, 'Critical') == "1" else False
        rid = 'fs#vcs%d'%n
        m = __import__("resMountVcs"+rcEnv.sysname)
        r = m.Mount(rid, mnt, dev, fstype, mntopt,
                    disabled=disabled, monitor=monitor)
        r.vcs_name = name
        self += r

    def resource_monitor(self):
        pass

