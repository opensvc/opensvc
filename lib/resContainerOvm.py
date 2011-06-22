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
import resources as Res
import os
import rcExceptions as ex
import resContainer

rcU = __import__("rcUtilities" + os.uname()[0])

class Ovm(resContainer.Container):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        resContainer.Container.__init__(self, rid="ovm", name=name, type="container.ovm",
                                        optional=optional, disabled=disabled, tags=tags)
        self.xen_d = os.path.join(os.sep, 'etc', 'xen')
        self.xen_auto_d = os.path.join(self.xen_d, 'auto')

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def list_conffiles(self):
        cf = os.path.join(self.xen_d, self.name)
        if os.path.exists(cf):
            return [cf]
        return []

    def files_to_sync(self):
        return self.list_conffiles()

    def check_capabilities(self):
        cmd = ['xm', 'info']
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            self.status_log("can not fetch xm info")
            return False
        return True

    def ping(self):
        return rcU.check_ping(self.addr, timeout=1, count=1)

    def find_vmcf(self):
        import glob
        l = glob.glob('/var/ovs/mount/*/running_pool/'+self.name+'/vm.cfg')+glob.glob(os.path.join(self.xen_d, self.name))
        if len(l) > 1:
            self.log.warning("%d configuration files found in repositories (%s)"%(len(l), str(l)))
        elif len(l) == 0:
            raise ex.excError("no configuration file found in repositories")
        return l[0]

    def container_start(self):
        cf = self.find_vmcf()
        cmd = ['xm', 'create', cf]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_stop(self):
        cmd = ['xm', 'shutdown', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        cmd = ['xm', 'destroy', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def is_up(self):
        cmd = ['xm', 'list', '--state=running']
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if self.name == l[0]:
                return True
        return False

    def get_container_info(self):
        cmd = ['xm', 'list', self.name]
        (ret, out, err) = self.call(cmd, errlog=False, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if self.name != l[0]:
                continue
            self.info['vcpus'] = l[3]
            self.info['vmem'] = l[2]
            return self.info           
        self.log.error("malformed 'xm list %s' output: %s"%(self.name, line))
        self.info = {'vcpus': '0', 'vmem': '0'}
        return self.info           

    def check_manual_boot(self):
        f = os.path.join(self.xen_auto_d, self.name)
        if os.path.exists(f):
            return False
        return True
