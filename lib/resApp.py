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

from rcUtilities import is_exe, justcall
from rcGlobalEnv import rcEnv
import resources as Res
import rcStatus
import rcExceptions as ex

class Apps(Res.Resource):
    prefix = []

    def __init__(self, runmethod=[], optional=False, disabled=False, tags=set([])):
        Res.Resource.__init__(self, rid="app", type="app",
                              optional=optional, disabled=disabled, tags=tags) 
        self.prefix = runmethod
        self.label = "app"

    def set_perms(self, rc):
        s = os.stat(rc)
        if s.st_uid != 0 or s.st_gid != 0:
            self.log.info("set %s ownership to uid 0 gid 0"%rc)
            os.chown(rc, 0, 0)
        if not is_exe(rc):
            self.vcall(self.prefix+['chmod', '+x', rc])

    def stop_checks(self):
        if not os.path.exists(self.svc.initd):
            self.log.info("%s is not present, perhaps already stopped"
                            %self.svc.initd)
            return True
        elif not os.path.islink(self.svc.initd):
            self.log.error("%s is not a link"%self.svc.initd)
            return False
        return True

    def start_checks(self):
        if not os.path.exists(self.svc.initd):
            self.log.error("%s is not present"%self.svc.initd)
            return False
        elif not os.path.islink(self.svc.initd):
            self.log.error("%s is not a link"%self.svc.initd)
            return False
        return True

    def status_checks(self, verbose=False):
        if not os.path.exists(self.svc.initd):
            if verbose: self.status_log("%s does not exist"%self.svc.initd)
            return False
        status = self.svc.group_status(excluded_groups=set(["sync", "app", "disk"]))
        if status["overall"] != rcStatus.UP:
            self.log.debug("abort resApp status because ip+fs status is %s"%status["overall"])
            if verbose: self.status_log("ip+fs status is %s, skip check"%status["overall"])
            return False
        return True

    def app(self, name, action, dedicated_log=True):
        if len(name) == 0:
            return 0
        if not os.path.exists(name):
            return 0
        self.set_perms(name)
        cmd = self.prefix+[name, action]
        if dedicated_log:
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
        else:
            (out, err, ret) = justcall(cmd)
            return ret

    def sorted_app_list(self, pattern):
        return sorted(glob.glob(os.path.join(self.svc.initd, pattern)))

    def _status(self, verbose=False):
        """Execute each startup script (C* files). Log the return code but
           don't stop on error. Count errors.
        """
        rets = {}
        errs = 0
        nb = 0
        try:
            if not self.status_checks(verbose=verbose):
                return rcStatus.NA
        except ex.excNotAvailable:
                return rcStatus.NA
        for name in self.sorted_app_list('C*'):
            if len(name) == 0:
                continue
            ret = self.app(name, 'status', dedicated_log=False)
            nb += 1
            errs += ret
            rets[name] = ret
        if nb == 0:
            self.status_log("no startup script")
            return rcStatus.NA
        elif errs == 0:
            return rcStatus.UP
        elif 0 in rets.values():
            names = ', '.join([n for n in rets if rets[n] != 0 ])
            self.status_log("%s returned errors"%(names))
            return rcStatus.WARN
        else:
            return rcStatus.DOWN

    def startstandby(self):
        """Execute each startup script (SS* files). Log the return code but
           don't stop on error.
        """
        try:
            if not self.start_checks():
                raise ex.excError
        except ex.excNotAvailable:
            return
        for name in self.sorted_app_list('S*.standby@'+rcEnv.nodename):
            self.app(name, 'start')

    def start(self):
        """Execute each startup script (S* files). Log the return code but
           don't stop on error.
        """
        try:
            if not self.start_checks():
                raise ex.excError
        except ex.excNotAvailable:
            return
        for name in self.sorted_app_list('S*'):
            self.app(name, 'start')

    def stop(self):
        """Execute each shutdown script (K* files). Log the return code but
           don't stop on error.
        """
        try:
            if not self.stop_checks():
                raise ex.excError
        except ex.excNotAvailable:
            return
        for name in self.sorted_app_list('K*'):
            self.app(name, 'stop')

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
