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
import resources as Res
import os
import rcExceptions as ex
import resContainer
from rcUtilities import which

rcU = __import__("rcUtilities" + os.uname()[0])

class Esx(resContainer.Container):
    startup_timeout = 180
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False, monitor=False,
                 tags=set([])):
        resContainer.Container.__init__(self, rid="esx", name=name,
                                        type="container.esx",
                                        optional=optional, disabled=disabled,
                                        monitor=monitor, tags=tags)
        self.vmx = None

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def vmcmd(self, l, verbose=False):
        self.find_vmx()
        if verbose:
            ret, out, err = self.vcall(['vmware-cmd', self.vmx] + l)
        else:
            ret, out, err = self.call(['vmware-cmd', self.vmx] + l)
        return ret, out, err

    def list_conffiles(self):
        return []

    def files_to_sync(self):
        return self.list_conffiles()

    def check_capabilities(self):
        return which('vmware-cmd')

    def ping(self):
        return rcU.check_ping(self.addr, timeout=1, count=1)

    def find_vmx(self):
        if self.vmx is not None:
            return self.vmx
        cmd = ['vmware-cmd', '-l']
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.excError
        l = out.split()
        pattern = '/'+self.name+'.vmx'
        for vmx in l:
            if pattern in vmx:
                self.vmx = vmx
        return self.vmx

    def _migrate(self):
        print "TODO"
        pass

    def container_start(self):
        cmd = ['start']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.excError

    def container_stop(self):
        cmd = ['stop', 'soft']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        cmd = ['stop', 'hard']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.excError

    def is_up(self):
        (ret, out, err) = self.vmcmd(['getstate'])
        if ret != 0:
            return False
        l = out.split()
        if len(l) != 3:
            return False
        if l[-1] == 'on':
            return True
        return False

    def getconfig(self, key):
        cmd = ['getconfig', key]
        ret, out, err = self.vmcmd(cmd)
        if ret != 0:
            return None
        l = out.split()
        if len(l) != 3:
            return None
        return l[-1]

    def get_container_info(self):
        self.info = {'vcpus': '0', 'vmem': '0'}
        self.info['vcpus'] = self.getconfig('numvcpus')
        self.info['vmem'] = self.getconfig('memsize')
        return self.info           

    def check_manual_boot(self):
        """ ESX will handle the vm startup itself """
        return True
