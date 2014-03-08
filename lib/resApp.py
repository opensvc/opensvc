#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>
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
import sys
import pwd
import time
import stat

from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import resources as Res
import rcStatus
import rcExceptions as ex

try:
    if rcEnv.sysname == "Windows":
        raise
    from multiprocessing import Process, Queue
    mp = True
except:
    mp = False

class StatusWARN(Exception):
    pass

class StatusNA(Exception):
    pass

class RsetApps(Res.ResourceSet):
    def __init__(self,
                 type=None,
                 resources=[],
                 parallel=False,
                 optional=False,
                 disabled=False,
                 tags=set([])):
        Res.ResourceSet.__init__(self,
                                 type=type,
                                 resources=resources,
                                 optional=optional,
                                 disabled=disabled,
                                 parallel=parallel,
                                 tags=tags)
        #
        # this bug should have be fixed, but it is not the case with python
        # 2.6.2 we ship for el5.
        # it manifests as apache failing to spawn workers because they can't
        # acquire stdin, closed upon thread startup.
        #
        sys.stdin = open(os.devnull)

    def action(self, action=None, tags=set([]), xtags=set([])):
        if action == 'start' and self.type == "app":
            self.containerize()
        try:
            Res.ResourceSet.action(self, action=action, tags=tags, xtags=xtags)
        except:
            if action == "stop":
                self.log.info("there were errors during app stop. please check the quality of the scripts. continuing anyway.")
                return
            raise

    def sort_resources(self, resources, action):
        attr = action + '_seq'
        resources.sort(lambda x, y: cmp(getattr(x, attr), getattr(y, attr)))
        return resources

    def containerize(self):
        if not self.svc.containerize:
            return
        try:
            container = __import__('rcContainer'+rcEnv.sysname)
        except ImportError:
            self.log.info("containerization not supported")
            return
        except Exception as e:
            print(e)
            raise
        container.containerize(self)

class App(Res.Resource):
    def __init__(self, rid=None,
                 script=None,
                 start=None,
                 stop=None,
                 check=None,
                 info=None,
                 run_as=None,
                 timeout=None,
                 optional=False,
                 disabled=False,
                 subset=None,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0):

        if script is None:
            raise ex.excInitError("script parameter must be defined in resource %s"%rid)

        Res.Resource.__init__(self, rid, "app", optional=optional,
                              subset=subset,
                              disabled=disabled, tags=tags,
                              monitor=monitor, restart=restart)
        self.rset_class = RsetApps
        self.script = script
        self.start_seq = start
        self.stop_seq = stop
        self.check_seq = check
        self.info_seq = info
        self.run_as = run_as
        self.timeout = timeout
        self.label = os.path.basename(script)
        self.always_on = always_on

        self.script_exec = True

    def on_add(self):
        self.validate_script_path()
        self.validate_script_exec()
        self.validate_user()

    def validate_user(self):
        if self.run_as is None:
            self.uid = 0
            self.gid = 0
            return
        try:
            ui = pwd.getpwnam(self.run_as)
        except:
            self.status_log("run_as %s issue: the user does not exist" % self.run_as)
            self.uid = None
            self.gid = None
            return
        self.uid = ui.pw_uid
        self.gid = ui.pw_gid

    def validate_script_exec(self):
        if which(self.script) is None:
            self.status_log("script %s is not executable" % self.script)
            self.script_exec = False

    def validate_script_path(self):
        if os.path.exists(self.script):
            return
        if self.script != os.path.basename(self.script):
            self.status_log("script %s does not exist" % self.script)
            self.script = None
            return
        self.script = os.path.join(self.svc.initd, self.script)
        if os.path.exists(self.script):
            return
        self.status_log("script %s does not exist" % self.script)

    def is_up(self):
        if self.script is None:
            raise StatusNA()
        if self.uid is None:
            raise StatusWARN()
        if self.gid is None:
            raise StatusWARN()
        if self.check_seq is None:
            self.status_log("check disabled")
            raise StatusNA()
        r = self.run('status', dedicated_log=False)
        return r

    def info(self):
        if self.info_seq is None:
            return []
        l = []
        s = self.run('info', dedicated_log=False, return_out=True)
        name = os.path.basename(os.path.realpath(self.script))
        if type(s) != str or len(s) == 0:
            l.append([self.svc.svcname, rcEnv.nodename, self.svc.clustertype, name, "Error", "info not implemented in launcher"])
            return l
        for line in s.split('\n'):
            if len(line) == 0:
                continue
            v = line.split(":")
            if len(v) < 2:
                l.append([self.svc.svcname, rcEnv.nodename, self.svc.clustertype, name, "Error", "parsing: %s"%line])
                continue
            l.append([self.svc.svcname, rcEnv.nodename, self.svc.clustertype, name, v[0].strip(), ":".join(v[1:]).strip()])
        return l

    def start(self):
        if self.start_seq is None:
            return
        if self.script is None:
            raise ex.excError("script %s does not exist"%self.script)

        try:
            status = self.is_up()
        except:
            status = 1

        if status == 0:
            self.log.info("%s is already started" % self.label)
            return

        r = self.run('start')
        if r != 0:
            raise ex.excError()
        self.can_rollback = True

    def stop(self):
        if self.stop_seq is None:
            return
        if self.script is None:
            raise ex.excError("script %s does not exist"%self.script)
        r = self.run('stop')
        if r != 0:
            raise ex.excError()

    def _status(self, verbose=False):
        status = self.svc.group_status(excluded_groups=set(["sync", "app", "disk.scsireserv", "hb"]))
        if str(status["overall"]) != "up" and len(self.svc.nodes) > 1:
            self.log.debug("abort resApp status because ip+fs status is %s"%status["overall"])
            if verbose: self.status_log("ip+fs status is %s, skip check"%status["overall"])
            return rcStatus.NA

        try:
            r = self.is_up()
        except StatusWARN:
            return rcStatus.WARN
        except StatusNA:
            return rcStatus.NA

        if r == 0:
            return rcStatus.UP
        elif r == 1:
            return rcStatus.DOWN

        self.status_log("check reports errors (%d)"%r)
        return rcStatus.WARN

    def set_executable(self):
        if self.script_exec: 
            return
        self.vcall(['chmod', '+x', self.script])

    def set_perms(self):
        if self.uid is None or self.gid is None:
            return
        s = os.stat(self.script)
        if s.st_uid != self.uid or s.st_gid != self.gid:
            self.log.info("set %s ownership to uid %d gid %d"%(self.script, self.uid, self.gid))
            os.chown(self.script, self.uid, self.gid)
        if self.run_as is not None and not s.st_mode & stat.S_ISUID:
            self.vcall(['chmod', '+s', self.script])
        elif self.run_as is None and s.st_mode & stat.S_ISUID:
            self.vcall(['chmod', '-s', self.script])

    def run(self, action, dedicated_log=True, return_out=False):
        if self.script is None:
            return 1

        self.set_perms()
        self.set_executable()

        cmd = [self.script, action]
        try:
            if dedicated_log:
                self.log.info('spawn: %s' % ' '.join(cmd))
                outf = os.path.join(rcEnv.pathtmp, 'svc_'+self.svc.svcname+'_'+os.path.basename(self.script)+'.log')
                f = open(outf, 'w')
                t = datetime.now()
                p = Popen(cmd, stdin=None, stdout=f.fileno(), stderr=f.fileno())
                try:
                    if self.timeout is None:
                        p.communicate()
                    else:
                        for i in range(self.timeout+1):
                            p.poll()
                            if p.returncode is not None:
                                break
                            time.sleep(1)
                        if p.returncode is None:
                            self.log.error("execution timeout (%d seconds)"%self.timeout)
                            p.terminate()
                            return 1
                        p.communicate()
                except (KeyboardInterrupt, ex.excSignal):
                    _len = datetime.now() - t
                    self.log.error('%s interrupted after %s - ret %d - logs in %s' % (action, _len, 1, outf))
                    f.close()
                    return 1
                _len = datetime.now() - t
                self.log.info('%s done in %s - ret %d - logs in %s' % (action, _len, p.returncode, outf))
                f.close()
                return p.returncode
            elif return_out:
                (out, err, ret) = justcall(cmd)
                if ret != 0:
                    return "Error: info not implemented in launcher"
                return out
            else:
                (out, err, ret) = justcall(cmd)
                self.log.debug("%s returned out=[%s], err=[%s], ret=[%d]"%(cmd, out, err, ret))
                return ret
        except OSError as e:
            if e.errno == 8:
                self.log.error("%s execution error (Exec format error)"%self.script)
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
