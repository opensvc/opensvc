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

def run_as_popen_kwargs(fpath):
    if rcEnv.sysname == "Windows":
        return {}
    try:
        st = os.stat(fpath)
    except Exception as e:
        raise ex.excError(str(e))
    cwd = rcEnv.pathtmp
    user_uid = st[stat.ST_UID]
    user_gid = st[stat.ST_GID]
    import pwd
    user_name = pwd.getpwuid(st[stat.ST_UID])[0]
    pw_record = pwd.getpwnam(user_name)
    user_name      = pw_record.pw_name
    user_home_dir  = pw_record.pw_dir
    env = os.environ.copy()
    env['HOME']  = user_home_dir
    env['LOGNAME']  = user_name
    env['PWD']  = cwd
    env['USER']  = user_name
    return {'preexec_fn': demote(user_uid, user_gid), 'cwd': cwd, 'env': env}

def demote(user_uid, user_gid):
    def result():
        os.setgid(user_gid)
        os.setuid(user_uid)
    return result

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
            import svcBuilder
            svcBuilder.add_apps_sysv(self.svc, self.svc.config)
            self.resources = self.svc.type2resSets["app"].resources

        try:
            Res.ResourceSet.action(self, action=action, tags=tags, xtags=xtags)
        except Exception as e:
            if action in ("stop", "shutdown", "rollback"):
                self.log.info("there were errors during app stop. please check the quality of the scripts. continuing anyway.")
                return
            raise

    def sort_resources(self, resources, action):
        if action in ("shutdown", "rollback"):
            action = "stop"
        attr = action + '_seq'
        l = [r for r in resources if hasattr(r, attr)]
        if len(l) != len(resources):
            attr = 'rid'
        resources.sort(key=lambda x: getattr(x, attr))
        return resources


class App(Res.Resource):
    def __init__(self, rid=None,
                 script=None,
                 start=None,
                 stop=None,
                 check=None,
                 info=None,
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
        self.timeout = timeout
        self.label = os.path.basename(script)
        self.always_on = always_on

        self.script_exec = True

    def __lt__(self, other):
        if other.start_seq is None:
            return 1
        if self.start_seq is None:
            return 0
        return self.start_seq < other.start_seq

    def validate_on_action(self):
        self.validate_script_path()
        self.validate_script_exec()

    def validate_script_exec(self):
        if self.script is None:
            self.script_exec = False
            return
        if which(self.script) is None:
            self.status_log("script %s is not executable" % self.script)
            self.script_exec = False

    def validate_script_path(self):
        if self.script is None:
            return
        if not self.script.startswith('/'):
            self.script = os.path.join(self.svc.initd, self.script)
        if os.path.exists(self.script):
            self.script = os.path.realpath(self.script)
            return
        self.script = None

    def is_up(self):
        if self.pg_frozen():
            raise StatusNA()
        if self.script is None:
            self.status_log("script does not exist", "warn")
            raise StatusNA()
        if not os.path.exists(self.script):
            self.status_log("script %s does not exist" % self.script, "warn")
            raise StatusNA()
        if self.check_seq is None:
            self.status_log("check is not set", "info")
            raise StatusNA()
        r = self.run('status', dedicated_log=False)
        return r

    def info(self):
        l = [
          ["script", self.script],
          ["start", str(self.start_seq) if self.start_seq else ""],
          ["stop", str(self.stop_seq) if self.stop_seq else ""],
          ["check", str(self.check_seq) if self.check_seq else ""],
          ["info", str(self.info_seq) if self.info_seq else ""],
          ["timeout", str(self.timeout) if self.timeout else ""],
        ]
        if self.info_seq is None:
            return self.fmt_info(l)
        self.validate_on_action()
        s = self.run('info', dedicated_log=False, return_out=True)
        if type(s) != str or len(s) == 0:
            l.append(["Error", "info not implemented in launcher"])
            return l
        for line in s.split('\n'):
            if len(line) == 0:
                continue
            v = line.split(":")
            if len(v) < 2:
                l.append(["Error", "parsing: %s"%line])
                continue
            l.append([v[0].strip(), ":".join(v[1:]).strip()])
        return self.fmt_info(l)

    def start(self):
        self.create_pg()
        self.validate_on_action()

        if self.start_seq is None:
            return
        if self.script is None:
            raise ex.excError("script does not exist")

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
        self.validate_on_action()

        if self.stop_seq is None:
            return
        if self.script is None:
            return
        if self.status() == rcStatus.DOWN:
            self.log.info("%s is already stopped" % self.label)
            return
        r = self.run('stop')
        if r != 0:
            raise ex.excError()

    def _status(self, verbose=False):
        self.validate_on_action()

        n_ref_res = len(self.svc.get_resources(['fs', 'ip', 'container', 'share', 'disk']))
        status = self.svc.group_status(excluded_groups=set(["sync", "app", "disk.scsireserv", "disk.drbd", "hb"]))
        if n_ref_res > 0 and str(status["overall"]) != "up":
            self.log.debug("abort resApp status because ip+fs status is %s"%status["overall"])
            if verbose:
                self.status_log("ip+fs status is %s, skip check"%status["overall"], "info")
            self.status_log("not evaluated (instance not up)", "info")
            return rcStatus.NA

        try:
            r = self.is_up()
        except StatusWARN:
            return rcStatus.WARN
        except StatusNA:
            return rcStatus.NA

        if r == 0:
            return self.status_stdby(rcStatus.UP)
        elif r == 1:
            return self.status_stdby(rcStatus.DOWN)

        self.status_log("check reports errors (%d)"%r)
        return rcStatus.WARN

    def set_executable(self):
        if self.script_exec:
            return
        if not os.path.exists(self.script):
            return
        self.vcall(['chmod', '+x', self.script])

    def run(self, action, dedicated_log=True, return_out=False):
        if self.script is None:
            return 1

        if not os.path.exists(self.script):
            if action == "start":
                self.log.error("script %s does not exist. can't run %s action" % (self.script, action))
                return 1
            elif action == "stop":
                self.log.info("script %s does not exist. hosting fs might already be down" % self.script)
                return 0
            elif return_out:
                return 0
            else:
                self.status_log("script %s does not exist" % self.script)
                raise StatusWARN()

        self.set_executable()

        cmd = [self.script, action]
        try:
            if dedicated_log:
                outf = os.path.join(rcEnv.pathtmp, 'svc_'+self.svc.svcname+'_'+os.path.basename(self.script)+'.log')
                f = open(outf, 'w')
                kwargs = {
                  'stdin': None,
                  'stdout': f.fileno(),
                  'stderr': f.fileno(),
                }
                kwargs.update(run_as_popen_kwargs(self.script))
                user = kwargs.get("env").get("LOGNAME")
                self.log.info('exec %s as user %s' % (' '.join(cmd), user))
                t = datetime.now()
                p = Popen(cmd, **kwargs)
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
                msg = '%s done in %s - ret %d - logs in %s' % (action, _len, p.returncode, outf)
                if p.returncode == 0:
                    self.log.info(msg)
                else:
                    self.log.error(msg)
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
                if not return_out and not dedicated_log:
                    self.status_log("exec format error")
                    raise StatusWARN()
                else:
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
