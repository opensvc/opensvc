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
from rcUtilities import is_exe, qcall
import resApp
import rcStatus

class Apps(resApp.Apps):
    app_d = os.path.join(os.sep, 'svc', 'etc', 'init.d')

    def set_perms(self, rc):
        (ret, out) = self.call(self.prefix+['find', self.app_d, '-name', os.path.basename(rc), '-a', '-uid', '0', '-a', '-gid', '0'])
        if len(out) == 0 or rc != out.split()[0]:
            self.vcall(self.prefix+['chown', '0:0', rc])
        (ret, out) = self.call(self.prefix+['test', '-x', rc])
        if ret != 0:
            self.vcall(self.prefix+['chmod', '+x', rc])

    def checks(self):
        container = self.svc.resources_by_id["container"]
        container.rstatus.status = container.status()
        if container.rstatus.status != rcStatus.UP:
            self.log.debug("abort resApp action because container status is %s"%container.rstatus)
            return False
        cmd = self.prefix + ['pwd']
        ret = qcall(cmd)
        if ret != 0:
            self.log.debug("abort resApp action because container is unreachable")
            return False
        cmd = self.prefix + ['test', '-d', self.app_d]
        ret = qcall(cmd)
        if ret != 0:
            self.log.error("%s is not present inside vm"%(self.app_d))
            return False
        return True

    def stop_checks(self):
        return self.checks()

    def start_checks(self):
        return self.checks()

    def status_checks(self):
        return self.checks()

    def sorted_app_list(self, pattern):
        cmd = self.prefix + ['ls', os.path.join(self.app_d, pattern)]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            return []
        return buff[0].split('\n')

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
