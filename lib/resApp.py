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
from datetime import datetime
import os
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import is_exe
import resources as Res

class Apps(Res.Resource):
    prefix = []

    def __init__(self, hostname=None, optional=False, disabled=False):
        Res.Resource.__init__(self, rid="app", type="app",
                              optional=optional, disabled=disabled) 
        if hostname is not None:
            self.prefix = rcEnv.rsh.split() + [hostname]
            self.hostname = hostname

    def checks(self):
        if not os.path.exists(self.svc.initd):
            self.log.error("%s is not present"%self.svc.initd)
            return False
        elif not os.path.islink(self.svc.initd):
            self.log.error("%s is not a link"%self.svc.initd)
            return False
        return True

    def app(self, name, action):
        if len(name) == 0:
            return 0
        if not is_exe(name):
            self.vcall(self.prefix+['chmod', '+x', name])
        cmd = self.prefix+[name, action]
        self.log.info('spawn: %s' % ' '.join(cmd))
        outf = '/var/tmp/svc_'+self.svc.svcname+'_'+os.path.basename(name)+'.log'
        f = open(outf, 'w')
        t = datetime.now()
        p = Popen(cmd, stdin=None, stdout=f.fileno(), stderr=f.fileno())
        p.communicate()
        _len = datetime.now() - t
        self.log.info('%s done in %s - ret %d - logs in %s' % (action, _len, p.returncode, outf))
        f.close()
        return p.returncode

    def sorted_app_list(self, pattern):
        return sorted(glob.glob(os.path.join(self.svc.initd, pattern)))

    def start(self):
        """Execute each startup script (S* files). Log the return code but
           don't stop on error.
        """
        if not self.checks():
            return 1
        for name in self.sorted_app_list('S*'):
            self.app(name, 'start')
        return 0

    def stop(self):
        """Execute each shutdown script (K* files). Log the return code but
           don't stop on error.
        """
        if not os.path.exists(self.svc.initd):
            self.log.info("%s is not present, perhaps already stopped"
                            %self.svc.initd)
            return 0
        if not self.checks():
            return 1
        for name in self.sorted_app_list('K*'):
            self.app(name, 'stop')
        return 0

    def status(self):
        pass

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
