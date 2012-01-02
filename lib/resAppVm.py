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
import datetime

from rcGlobalEnv import rcEnv
from rcUtilities import qcall, justcall
import resApp
import rcStatus
import rcExceptions as ex

class Apps(resApp.Apps):
    app_d = os.path.join(os.sep, 'svc', 'etc', 'init.d')

    def vmcmd(self, cmd, verbose=False, timeout=10):
        return self.svc.vmcmd(cmd, verbose, timeout, r=self)

    def set_perms(self, rc):
        ret, out, err = self.vmcmd('/usr/bin/find %s -name %s -a -user root -a -group root'%(self.app_d, os.path.basename(rc)))
        if len(out) == 0 or rc != out.split()[0]:
            self.vmcmd('chown root:root %s'%rc, verbose=True)
        ret, out, err = self.vmcmd('test -x %s'%rc)
        if ret != 0:
            self.vmcmd('chmod +x %s'%rc, verbose=True)

    def checks(self, verbose=False):
        container = self.svc.resources_by_id["container"]
        if container.status(refresh=True) != rcStatus.UP:
            self.log.debug("abort resApp action because container status is %s"%rcStatus.status_str(container.status()))
            self.status_log("container is %s"%rcStatus.status_str(container.status()))
            raise ex.excNotAvailable
        ret, out, err = self.vmcmd('/bin/pwd')
        if ret != 0:
            self.log.debug("abort resApp action because container is unreachable")
            self.status_log("container is unreachable")
            return False
        ret, out, err = self.vmcmd('test -d %s'%self.app_d)
        if ret == 0:
            return True
        ret, out, err = self.vmcmd('/bin/mkdir -p %s'%self.app_d, verbose=True)
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
        ret, out, err = self.vmcmd('/usr/bin/find %s -name %s'%(self.app_d, pattern.replace('*', '\*')))
        if ret != 0:
            self.log.debug("failed to fetch container startup scripts list")
            return []

        l = out.split('\n')

        # most unix find commands don't support maxdepth.
        # discard manually the startup scripts found in subdirs of app_d
        n = self.app_d.count("/")
        if not self.app_d.endswith("/"):
            n += 1
        l = [e for e in l if e.count("/") == n]

        return sorted(l)

    def app_exist(self, name):
        """ verify app_exists inside Vm
        """
        ret, out, err = self.vmcmd('/bin/ls -Ld %s'%name)
        if ret == 0:
            return True
        else:
            return False

    def app(self, name, action, dedicated_log=True, return_out=False):
        if len(name) == 0:
            return 0
        if not self.app_exist(name):
            return 0

        self.set_perms(name)
        cmd = "%s %s"%(name, action)
        try:
            if dedicated_log:
                self.log.info('spawn: %s' % cmd)
                outf = os.path.join(rcEnv.pathtmp, 'svc_'+self.svc.svcname+'_'+os.path.basename(name)+'.log')
                f = open(outf, 'w')
                t = datetime.datetime.now()
                ret, out, err = self.vmcmd(cmd)
                _len = datetime.datetime.now() - t
                self.log.info('%s done in %s - ret %d - logs in %s' % (action, _len, ret, outf))
                f.write(out)
                f.write(err)
                f.close()
                return ret
            elif return_out:
                ret, out, err = self.vmcmd(cmd)
                if ret != 0:
                    return "Error: info not implemented in launcher"
                return out
            else:
                ret, out, err = self.vmcmd(cmd)
                self.log.debug("%s returned out=[%s], err=[%s], ret=[%d]"%(cmd, out, err, ret))
                return ret
        except OSError, e:
            if e.errno == 8:
                self.log.error("%s execution error (Exec format error)"%name)
                return 1
            else:
                self.svc.save_exc()
                return 1
        except:
           self.svc.save_exc()
           return 1

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
