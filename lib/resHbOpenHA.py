#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

import resources as Res
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall

class Hb(Res.Resource):
    """ HeartBeat ressource
    """
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        Res.Resource.__init__(self, rid, "hb.openha",
                              optional=optional, disabled=disabled, tags=tags)
        self.basedir = os.path.join(os.sep, 'usr', 'local', 'cluster')
        self.bindir = os.path.join(self.basedir, 'bin')
        self.logdir = os.path.join(self.basedir, 'log')
        self.svcdir = os.path.join(self.basedir, 'services')
        self.cfsvcdir = os.path.join(self.basedir, 'conf', 'services')
        self.cfnoddir = os.path.join(self.basedir, 'conf', 'nodes')
        self.cfmondir = os.path.join(self.basedir, 'conf', 'monitor')
        os.environ['EZ_BIN'] = self.bindir
        os.environ['EZ_SERVICES'] = self.cfsvcdir
        os.environ['EZ_NODES'] = self.cfnoddir
        os.environ['EZ_MONITOR'] = self.cfmondir
        os.environ['EZ_LOG'] = self.logdir
        self.service_cmd = os.path.join(self.bindir, 'service')
        self.heartc = os.path.join(self.bindir, 'heartc')
        self.heartd = os.path.join(self.bindir, 'heartd')
        self.nmond = os.path.join(self.bindir, 'nmond')
        self.name = name

    def cluster_name(self):
        if self.name is None:
            return self.svc.svcname
        else:
            return self.name

    def service_status(self):
        service_state = os.path.join(self.svcdir, self.cluster_name(), 'STATE.'+rcEnv.nodename)
        try:
            f = open(service_state, 'r')
            buff = f.read().strip(' \n')
            f.close()
        except:
            import traceback
            traceback.print_exc()
            return 'unknown'
        if buff == '6':
            return 'frozen-stop'
        elif buff == '2':
            return 'started'
        elif buff == '0':
            return 'stopped'
        else:
            return 'unknown'

    def service_action(self, command):
        cmd = [service_cmd, '-A', self.cluster_name(), command]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def process_running(self):
        daemons = [self.heartc, self.heartd, self.nmond]
        (out, err, ret) = justcall(['ps', '-ef'])
        if ret != 0:
            return False
        h = {}
        for d in daemons:
            h[d] = 1
        for line in out.split('\n'):
            for d in daemons:
                if d in line:
                    h[d] = 0
        total = 0
        for d in daemons:
            total += h[d]
        if total > 0:
            return False
        return True

    def __str__(self):
        return "%s" % (Res.Resource.__str__(self))

    def stop(self):
        pass

    def start(self):
        pass

    def _status(self, verbose=False):
        if not os.path.exists(self.service_cmd):
            self.status_log("open-ha is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("open-ha daemons are not running")
            return rcStatus.WARN
        status = self.service_status()
        if status == 'unknown':
            self.status_log("unable to determine cluster service state")
            return rcStatus.WARN
        elif status == 'frozen-stop':
            self.status_log("cluster service is frozen")
            return rcStatus.WARN
        elif status == 'stopped':
            return rcStatus.DOWN
        elif status == 'started':
            return rcStatus.UP
        else:
            self.status_log("unknown cluster service status: %s"%status)
            return rcStatus.WARN
