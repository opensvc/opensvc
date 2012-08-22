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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import os
import svc
import rcExceptions as ex
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

class SvcSg(svc.Svc):

    def __init__(self, svcname, pkg_name=None, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, "ServiceGuard", optional=optional, disabled=disabled, tags=tags)
        self.pkg_name = pkg_name
        self.load_paths()
        self.builder()

    def load_paths(self):
        p = '/usr/local/cmcluster/bin/'
        if os.path.exists(p):
            self.prefix = p
        else:
            self.prefix = ''
        self.cmviewcl_bin = p + 'cmviewcl'

    def load_cmviewcl(self):
        self.cmviewcl = {}

        cmd = [self.cmviewcl_bin, "-p", self.pkg_name, "-v", "-f", "line"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excInitError(err)
        for line in out.split("\n"):
            if "=" not in line:
                continue
            i = line.index('=')
            param = line[:i]
            value = line[i+1:]

            if '|' in param:
                l = param.split('|')
                if len(l) == 2:
                    res, param = l
                    node = None
                elif len(l) == 3:
                    res, node, param = l
                    node = node.replace('node:', '')
                else:
                    print l
                    continue
                restype, resname = res.split(':')
                if restype not in self.cmviewcl:
                    self.cmviewcl[restype] = {}
                if resname not in self.cmviewcl[restype]:
                    self.cmviewcl[restype][resname] = {}
                if node is not None:
                    self.cmviewcl[restype][resname][(param,node)] = value
                else:
                    self.cmviewcl[restype][resname][param] = value
            else:
                self.cmviewcl[param] = value
        #print self.cmviewcl

    def load_cntl(self):
        p = self.cmviewcl['run_script']
        try:
            f = open(p, 'r')
            buff = f.read()
            f.close()
        except:
            self.log.error("failed to load %s"%p)
            raise ex.excError
        self.cntl = {"vg": {}, "ip": {}, "fs": {}}
        for line in buff.split('\n'):
             line = line.strip()
             if line.startswith("#"):
                 continue
             if len(line) == 0:
                 continue
             for _line in line.split(';'):
                 _line = _line.strip()
                 if _line.startswith("VG[") or \
                    _line.startswith("CVM_DG[") or \
                    _line.startswith("VXVM_DG[") or \
                    _line.startswith("IP[") or \
                    _line.startswith("SUBNET[") or \
                    _line.startswith("LV[") or \
                    _line.startswith("FS_MOUNT_OPT[") or \
                    _line.startswith("FS_TYPE[") or \
                    _line.startswith("FS["):
                     self.cntl_parse(_line)

    def cntl_parse(self, s):
        i = s.index('[')
        param = s[:i]
        s = s[i+1:]
        i = s.index(']')
        index = s[:i]
        try:
            int(index)
        except:
            return
        value = s[i+2:].strip('"')

        if param in ["VG", "CVM_DG", "VXVM_DG"]:
            if index not in self.cntl['vg']:
                self.cntl['vg'][index] = {}
            self.cntl['vg'][index][param] = value.replace('/dev/', '')

        if param in ["IP", "SUBNET"]:
            if index not in self.cntl['ip']:
                self.cntl['ip'][index] = {}
            self.cntl['ip'][index][param] = value

        if param in ["FS", "LV", "FS_MOUNT_OPT", "FS_TYPE"]:
            if index not in self.cntl['fs']:
                self.cntl['fs'][index] = {}
            self.cntl['fs'][index][param] = value

    def builder(self):
        if self.pkg_name is None:
            self.error("pkg_name is not set")
            raise ex.excInitError()
        self.load_cmviewcl()
        if len(self.cmviewcl) == 0:
            raise ex.excInitError()
        self.load_cntl()
        self.nodes = set(self.cmviewcl['node'].keys())
        self.load_hb()
        self.load_resources()
        self.load_ip_addresses()
        self.load_vgs()

    def load_hb(self):
        if self.cmviewcl['highly_available'] != "yes":
            return
        rid = 'hb#sg0'
        m = __import__("resHbSg")
        r = m.Hb(rid, self.cmviewcl['name'])
        self += r

    def load_vgs(self):
        self.n_vg = 0
        for i in self.cntl['vg']:
            data = self.cntl['vg'][i]
            self.load_vg(data)

    def load_vg(self, data):
        if 'VG' in data:
            name = data['VG']
            type = ""
        elif 'VCM_DG' in data:
            name = data['CVM_DG']
            type = "Cvm"
        elif 'VXVM_DG' in data:
            name = data['VXVM_DG']
            type = "VxVm"
        n = self.n_vg
        rid = 'vg#sg%d'%n
        modname = "resVg"+type+"Sg"+rcEnv.sysname
        try:
            m = __import__(modname)
        except ImportError:
            self.log.error("module %s is not implemented"%modname)
            return
        r = m.Vg(rid, name)
        self += r
        self.n_vg += 1

    def load_ip_addresses(self):
        self.n_ip_address = 0
        for i in self.cntl['ip']:
            data = self.cntl['ip'][i]
            self.load_ip_address(data)

    def load_ip_address(self, data):
        ipname = data['IP']
        subnet = data['SUBNET']
        n = self.n_ip_address
        rid = 'ip#sg%d'%n
        m = __import__("resIpSg"+rcEnv.sysname)
        r = m.Ip(rid, "", ipname, "")
        if 'subnet' in self.cmviewcl and \
           subnet in self.cmviewcl['subnet']:
            r.monitor = True
        self += r
        self.n_ip_address += 1

    def load_resources(self):
        self.n_resource = 0
        for i in self.cntl['fs']:
            data = self.cntl['fs'][i]
            self.load_resource(data)

    def load_resource(self, data):
        dev = data['LV']
        mnt = data['FS']
        mntopt = data['FS_MOUNT_OPT']
        fstype = data['FS_TYPE']
        vgname = dev.split('/')[2]
        lvname = dev.split('/')[3]
        n = self.n_resource
        rid = 'fs#sg%d'%n
        m = __import__("resMountSg"+rcEnv.sysname)
        r = m.Mount(rid, mnt, dev, fstype, mntopt)
        r.mon_name = '/vg/%s/lv/status/%s'%(vgname, lvname)
        if 'resource' in self.cmviewcl and \
           r.mon_name in self.cmviewcl['resource']:
            r.monitor = True
        self += r
        self.n_resource += 1
