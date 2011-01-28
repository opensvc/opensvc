#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@free.fr>'
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
import resources as Res
import rcStatus
import rcExceptions as ex
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv

class Drbd(Res.Resource):
    """ Drbd device resource

        The tricky part is that drbd devices can be used as PV
        and LV can be used as drbd base devices. Treat the ordering
        with 'prevg' and 'postvg' tags.

        Start 'ups' and promotes the drbd devices to primary.
        Stop 'downs' the drbd devices.
    """
    def __init__(self, rid=None, res=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        Res.Resource.__init__(self, rid, "disk.drbd",
                              optional=optional, disabled=disabled, tags=tags)
        self.res = res
        self.label = res
        self.drbdadm = None
        self.always_on = always_on
        self.disks = set()
        if 'prevg' not in self.tags and 'postvg' not in self.tags:
            tags |= set(['postvg'])

    def __str__(self):
        return "%s resource=%s" % (Res.Resource.__str__(self),\
                                 self.res)

    def files_to_sync(self):
        cf = os.path.join(os.sep, 'etc', 'drbd.d', self.res+'.res')
        if os.path.exists(cf):
            return [cf]
        self.log.error("%s does not exist"%cf)
        return []

    def drbdadm_cmd(self, cmd):
        if self.drbdadm is None:
            if which('drbdadm'):
                self.drbdadm = 'drbdadm'
            else:
                self.log("drbdadm command not found")
                raise exc.excError
        return [self.drbdadm] + cmd.split() + [self.res]

    def disklist(self):
        if self.disks != set():
            return self.disks

        self.disks = set()
        devps = set()

        (ret, out) = self.call(self.drbdadm_cmd('dump-xml'))
        if ret != 0:
            raise ex.excError

        from xml.etree.ElementTree import XML, fromstring
        tree = fromstring(out)
        
        for res in tree.getiterator('resource'):
            if res.attrib['name'] != self.res:
                continue
            for host in res.getiterator('host'):
                if host.attrib['name'] != rcEnv.nodename:
                    continue
                d = host.find('disk')
                if d is None:
                    continue
                devps |= set([d.text])

        try:
            u = __import__('rcUtilities'+rcEnv.sysname)
            self.disks = u.devs_to_disks(self, devps)
        except:
            self.disks = devps

        return self.disks

    def drbdadm_down(self):
        (ret, out) = self.vcall(self.drbdadm_cmd('down'))
        if ret != 0:
            raise ex.excError

    def drbdadm_up(self):
        (ret, out) = self.vcall(self.drbdadm_cmd('up'))
        if ret != 0:
            raise ex.excError

    def get_cstate(self):
        self.prereq()
        (out, err, ret) = justcall(self.drbdadm_cmd('cstate'))
        if ret != 0:
            if "Device minor not allocated" in err:
                return "Unattached"
            else:
                raise ex.excError
        return out.strip()

    def prereq(self):
        if not os.path.exists("/proc/drbd"):
            (ret, out) = self.vcall(['modprobe', 'drbd'])
            if ret != 0: raise ex.excError

    def start_connection(self):
        cstate = self.get_cstate()
        if cstate == "Connected":
            self.log.info("drbd resource %s is already up"%self.res)
        elif cstate == "StandAlone":
            self.drbdadm_down()
            self.drbdadm_up()
        elif cstate == "WFConnection":
            self.log.info("drbd resource %s peer node is not listening"%self.res)
            pass
        else:
            self.drbdadm_up()

    def get_roles(self):
        (ret, out) = self.call(self.drbdadm_cmd('role'))
        if ret != 0:
            raise ex.excError
        out = out.strip().split('/')
        if len(out) != 2:
            raise ex.excError
        return out

    def start_role(self, role):
        roles = self.get_roles()
        if roles[0] != role:
            (ret, out) = self.vcall(self.drbdadm_cmd(role.lower()))
            if ret != 0:
                raise ex.excError
        else:
            self.log.info("drbd resource %s is already %s"%(self.res, role))

    def startstandby(self):
        self.start_connection()
        roles = self.get_roles()
        if roles[0] == "Primary":
            return
        self.start_role('Secondary')

    def stopstandby(self):
        self.start_connection()
        roles = self.get_roles()
        if roles[0] == "Secondary":
            return
        self.start_role('Secondary')

    def start(self):
        self.start_connection()
        self.start_role('Primary')

    def stop(self):
        self.drbdadm_down()

    def _status(self, verbose=False):
        (ret, out) = self.call(self.drbdadm_cmd('dstate'))
        if ret != 0:
            self.status_log("drbdadm dstate %s failed"%self.res)
            return rcStatus.WARN
        out = out.strip()
        if out == "UpToDate/UpToDate":
            return self.status_stdby(rcStatus.UP)
        elif out == "Unconfigured":
            return self.status_stdby(rcStatus.DOWN)
        self.status_log("unexpected drbd resource %s state: %s"%(self.res, out))
        return rcStatus.WARN

if __name__ == "__main__":
    help(Drbd)
    v = Drbd(res='test')
    print v

