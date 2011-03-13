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
from subprocess import *
import os
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import qcall, justcall
import resApp
import rcStatus
import rcExceptions as ex

class Apps(resApp.Apps):
    app_d = os.path.join(os.sep, 'svc', 'etc', 'init.d')

    def set_perms(self, rc):
        (ret, out) = self.call(self.prefix+['/usr/bin/find',
                                            self.app_d,
                                            '-name', os.path.basename(rc),
                                            '-a', '-user', 'root',
                                            '-a', '-group', 'root'])
        if len(out) == 0 or rc != out.split()[0]:
            self.vcall(self.prefix+['chown', 'root:root', rc])
        (ret, out) = self.call(self.prefix+['test', '-x', rc])
        if ret != 0:
            self.vcall(self.prefix+['chmod', '+x', rc])

    def checks(self, verbose=False):
        container = self.svc.resources_by_id["container"]
        if container.status(refresh=True) != rcStatus.UP:
            self.log.debug("abort resApp action because container status is %s"%rcStatus.status_str(container.status()))
            self.status_log("container is %s"%rcStatus.status_str(container.status()))
            raise ex.excNotAvailable
        cmd = self.prefix + ['/bin/pwd']
        ret = qcall(cmd)
        if ret != 0:
            self.log.debug("abort resApp action because container is unreachable")
            self.status_log("container is unreachable")
            return False
        cmd = self.prefix + ['test', '-d', self.app_d]
        ret = qcall(cmd)
        if ret == 0:
            return True
        cmd = self.prefix + ['/bin/mkdir', '-p', self.app_d]
        ret = self.vcall(cmd)
        if ret != 0:
            return False
        return True

    def stop_checks(self):
        return self.checks()

    def start_checks(self):
        return self.checks()

    def status_checks(self, verbose=False):
        return self.checks(verbose=verbose)

    def sorted_app_list(self, pattern):
        cmd = self.prefix + ['/usr/bin/find', self.app_d, '-name', pattern]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            return []
        return sorted(buff[0].split('\n'))

    def app_exist(self, name):
        """ verify app_exists inside Vm
        """
        (out, err, ret) = justcall (self.prefix + ['/bin/ls', '-Ld', name ])
        if ret == 0:
            return True
        else:
            return False

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
