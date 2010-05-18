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
import rcExceptions as ex
from rcUtilities import qcall
from rcUtilitiesSunOS import check_ping
import resContainer
from rcGlobalEnv import rcEnv

class Ldom(resContainer.Container):
    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        resContainer.Container.__init__(self, rid="ldom", name=name, type="container.ldom",
                                        optional=optional, disabled=disabled, tags=tags)
        self.shutdown_timeout = 240
        self.sshbin = '/usr/local/bin/ssh'


    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def files_to_sync(self):
        import glob
        a = []
        ldomf = os.path.join(rcEnv.pathvar, 'ldom_'+self.name+'.*')
        files = glob.glob(ldomf)
        if len(files) > 0:
            a += files
        return a

    def check_capabilities(self):
        cmd = ['/usr/sbin/ldm', 'list' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            return False
        return True

    def state(self):
        """ ldm state : None/inactive/bound/active
            ldm list -p domainname outputs:
                VERSION
                DOMAIN|[varname=varvalue]*
        """
        cmd = ['/usr/sbin/ldm', 'list', '-p', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            return None
        for word in out.split("|"):
            a=word.split('=')
            if len(a) == 2:
                if a[0] == 'state':
                    return a[1]
        return None

    def ping(self):
        return check_ping(self.addr, timeout=1)

    def container_action(self,action):
        cmd = ['/usr/sbin/ldm', action, self.name]
        (ret, buff) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        return None

    def container_start(self):
        """ ldm bind domain
            ldm start domain
        """
        state = self.state()
        if state == 'None':
            raise ex.excError
        if state == 'inactive':
            self.container_action('bind')
            self.container_action('start')
        if state == 'bound' :
            self.container_action('start')

    def container_forcestop(self):
        """ ldm unbind domain
            ldm stop domain
        """
        if self.state == 'active':
            try:
                self.container_action('stop')
            except ex.excError:
                pass
        self.container_action('unbind')

    def container_stop(self):
        """ launch init 5 into container
            wait_for_shutdown
            ldm stop domain
            ldm unbind domain
        """
        state = self.state()
        if state == 'None':
            raise ex.excError
        if state == 'inactive':
            return None
        if state == 'bound' :
            self.container_action('unbind')
        if state == 'active' :
            cmd = rcEnv.rsh.split() + [ self.name, '/usr/sbin/init', '5' ]
            (ret, buff) = self.vcall(cmd)
            if ret == 0:
                try:
                    self.log.info("wait for container is_shutdown")
                    self.wait_for_fn(self.is_shutdown, self.shutdown_timeout, 2)
                except ex.excError:
                    pass
            self.container_forcestop()

    def check_manual_boot(self):
        cmd = ['/usr/sbin/ldm', 'list-variable', 'auto-boot?', self.name]
        (ret, out) = self.call(cmd)
        if ret != 0:
            return False
        if out != 'auto-boot?=False' :
            return True
        self.log.info("Auto boot should be turned off")
        return False

    def is_shutdown(self):
        state = self.state()
        if state == 'inactive' or state == 'bound':
            return True
        return False

    def is_down(self):
        if self.state() == 'inactive':
            return True
        return False

    def is_up(self):
        if self.state() == 'active':
            return True
        return False

