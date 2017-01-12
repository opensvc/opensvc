"""
The module defining the App resource and RsetApps resourceset objects.
"""
from subprocess import Popen
from datetime import datetime
import os
import pwd
import time
import stat

from rcUtilities import justcall, which, lazy, is_string
from rcGlobalEnv import rcEnv
from resources import Resource
from resourceset import ResourceSet
import rcStatus
import rcExceptions as ex
import lock

def run_as_popen_kwargs(fpath):
    """
    Setup the Popen keyword args to execute <fpath> with the
    privileges demoted to those of the owner of <fpath>.
    """
    if rcEnv.sysname == "Windows":
        return {}
    try:
        fstat = os.stat(fpath)
    except Exception as exc:
        raise ex.excError(str(exc))
    cwd = rcEnv.pathtmp
    user_uid = fstat[stat.ST_UID]
    user_gid = fstat[stat.ST_GID]
    try:
        user_name = pwd.getpwuid(user_uid)[0]
    except KeyError:
        user_name = "unknown"
    try:
        pw_record = pwd.getpwnam(user_name)
        user_name = pw_record.pw_name
        user_home_dir = pw_record.pw_dir
    except KeyError:
        user_home_dir = rcEnv.pathtmp
    env = os.environ.copy()
    env['HOME'] = user_home_dir
    env['LOGNAME'] = user_name
    env['PWD'] = cwd
    env['USER'] = user_name
    return {'preexec_fn': demote(user_uid, user_gid), 'cwd': cwd, 'env': env}

def demote(user_uid, user_gid):
    """
    Return a privilege demotion function to plug as Popen() prefex_fn keyword
    argument, customized for <user_uid> and <user_gid>.
    """
    def result():
        """
        A privilege demotion function to plug as Popen() prefex_fn keyword
        argument.
        """
        os.setgid(user_gid)
        os.setuid(user_uid)
    return result

class StatusWARN(Exception):
    """
    A class to raise to signal status() to return a "warn" state.
    """
    pass

class StatusNA(Exception):
    """
    A class to raise to signal status() to return a "n/a" state.
    """
    pass

class RsetApps(ResourceSet):
    """
    The app resource specific resourceset class.
    Mainly defines a specific resource sort method honoring the start,
    stop, check and info sequencing numbers.
    """
    def __init__(self,
                 type=None,
                 resources=[],
                 parallel=False,
                 optional=False,
                 disabled=False,
                 tags=set([])):
        ResourceSet.__init__(self,
                             type=type,
                             resources=resources,
                             optional=optional,
                             disabled=disabled,
                             parallel=parallel,
                             tags=tags)

    def action(self, action, **kwargs):
        """
        Wrap the standard resourceset action method to ignore launcher errors
        on stop.
        """
        try:
            ResourceSet.action(self, action, **kwargs)
        except ex.excError:
            if action in ("stop", "shutdown", "rollback", "delete", "unprovision"):
                self.log.info("there were errors during app stop. please check "
                              "the quality of the scripts. continuing anyway.")
                return
            raise

    def sort_resources(self, resources, action):
        """
        A resource sort method honoring the start, stop, check and info
        sequencing numbers.
        """
        if action in ("shutdown", "rollback", "unprovision", "delete"):
            action = "stop"
        attr = action + '_seq'
        retained_resources = [res for res in resources if hasattr(res, attr)]
        if len(retained_resources) != len(resources):
            attr = 'rid'
        resources.sort(key=lambda x: getattr(x, attr))
        return resources


class App(Resource):
    """
    The App resource driver class.
    """
    def __init__(self, rid=None,
                 script=None,
                 start=None,
                 stop=None,
                 check=None,
                 info=None,
                 timeout=None,
                 **kwargs):

        if script is None:
            raise ex.excInitError("script parameter must be defined in resource %s"%rid)

        Resource.__init__(self, rid, "app", **kwargs)
        self.rset_class = RsetApps
        self.script = script
        self.start_seq = start
        self.stop_seq = stop
        self.check_seq = check
        self.info_seq = info
        self.timeout = timeout
        self.label = os.path.basename(script)
        self.lockfd = None

        self.script_exec = True

    @lazy
    def lockfile(self):
        """
        Lazy init for the resource lock file path property.
        """
        lockfile = os.path.join(rcEnv.pathlock, self.svc.svcname)
        lockfile = ".".join((lockfile, self.rid))
        return lockfile

    def __lt__(self, other):
        if other.start_seq is None:
            return 1
        if self.start_seq is None:
            return 0
        return self.start_seq < other.start_seq

    def validate_on_action(self):
        """
        Do sanity checks on the resource parameters before running an action.
        """
        self.validate_script_path()
        self.validate_script_exec()

    def validate_script_exec(self):
        """
        Invalidate the script if the file is not executable or not found.
        """
        if self.script is None:
            self.script_exec = False
            return
        if which(self.script) is None:
            self.status_log("script %s is not executable" % self.script)
            self.script_exec = False

    def validate_script_path(self):
        """
        Converts the script path to a realpath.
        Invalidate the script if not found.
        If the script is specified as a basename, consider it is to be found
        in the <pathetc>/<svcname>.d directory.
        """
        if self.script is None:
            return
        if not self.script.startswith('/'):
            self.script = os.path.join(self.svc.initd, self.script)
        if os.path.exists(self.script):
            self.script = os.path.realpath(self.script)
            return
        self.script = None

    def is_up(self):
        """
        Return 0 if the app resource is up.
        """
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
        ret = self.run('status', dedicated_log=False)
        return ret

    def info(self):
        """
        Contribute app resource standard and script-provided key/val pairs
        to the service's resinfo.
        """
        keyvals = [
            ["script", self.script],
            ["start", str(self.start_seq) if self.start_seq else ""],
            ["stop", str(self.stop_seq) if self.stop_seq else ""],
            ["check", str(self.check_seq) if self.check_seq else ""],
            ["info", str(self.info_seq) if self.info_seq else ""],
            ["timeout", str(self.timeout) if self.timeout else ""],
        ]
        if self.info_seq is None:
            return self.fmt_info(keyvals)
        self.validate_on_action()
        buff = self.run('info', dedicated_log=False, return_out=True)
        if is_string(buff) != str or len(buff) == 0:
            keyvals.append(["Error", "info not implemented in launcher"])
            return keyvals
        for line in buff.splitlines():
            if len(line) == 0:
                continue
            elements = line.split(":")
            if len(elements) < 2:
                keyvals.append(["Error", "parsing: %s" % line])
                continue
            keyvals.append([elements[0].strip(), ":".join(elements[1:]).strip()])
        return self.fmt_info(keyvals)

    def start(self):
        """
        Start the resource.
        """
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
            self.log.info("%s is already started", self.label)
            return

        ret = self.run('start')
        if ret != 0:
            raise ex.excError()
        self.can_rollback = True

    def stop(self):
        """
        Stop the resource.
        """
        self.validate_on_action()

        if self.stop_seq is None:
            return
        if self.script is None:
            return
        if self.status() == rcStatus.DOWN:
            self.log.info("%s is already stopped", self.label)
            return
        ret = self.run('stop')
        if ret != 0:
            raise ex.excError()

    def unlock(self):
        """
        Release the app action lock.
        """
        self.log.debug("release app lock")
        lock.unlock(self.lockfd)
        try:
            os.unlink(self.lockfile)
        except OSError:
            pass
        self.lockfd = None

    def lock(self, action=None, timeout=0, delay=1):
        """
        Acquire the app action lock.
        """
        if self.lockfd is not None:
            return

        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, self.lockfile)
        self.log.debug("acquire app lock %s", details)
        try:
            lockfd = lock.lock(
                timeout=timeout,
                delay=delay,
                lockfile=self.lockfile,
                intent=action
            )
        except lock.lockTimeout as exc:
            raise ex.excError("timed out waiting for lock %s: %s" % (details, str(exc)))
        except lock.lockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.lockCreateError:
            raise ex.excError("can not create lock file %s" % details)
        except lock.lockAcquire as exc:
            raise ex.excError("another action is currently running %s: %s" % (details, str(exc)))
        except ex.excSignal:
            raise ex.excError("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.excError("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def _status(self, verbose=False):
        """
        Return the resource status.
        """
        self.validate_on_action()

        n_ref_res = len(self.svc.get_resources(['fs', 'ip', 'container', 'share', 'disk']))
        status = self.svc.group_status(excluded_groups=set([
            "sync",
            "app",
            "disk.scsireserv",
            "disk.drbd",
            "hb"
        ]))
        if n_ref_res > 0 and str(status["overall"]) != "up":
            self.log.debug("abort resApp status because ip+fs status is %s", status["overall"])
            if verbose:
                self.status_log("ip+fs status is %s, skip check"%status["overall"], "info")
            self.status_log("not evaluated (instance not up)", "info")
            return rcStatus.NA

        try:
            ret = self.is_up()
        except StatusWARN:
            return rcStatus.WARN
        except StatusNA:
            return rcStatus.NA
        except ex.excError as exc:
            msg = str(exc)
            if "intent '" in msg:
                action = msg.split("intent '")[-1].split("'")[0]
                self.status_log("%s in progress" % action, "info")
            self.log.debug("resource status forced to n/a: an action is running")
            return rcStatus.NA

        if ret == 0:
            return self.status_stdby(rcStatus.UP)
        elif ret == 1:
            return self.status_stdby(rcStatus.DOWN)

        self.status_log("check reports errors (%d)" % ret)
        return rcStatus.WARN

    def set_executable(self):
        """
        Switch the script file execution bit to on.
        """
        if self.script_exec:
            return
        if not os.path.exists(self.script):
            return
        self.vcall(['chmod', '+x', self.script])

    def run(self, action, dedicated_log=True, return_out=False):
        """
        Acquire the app resource lock, run the action and release for info, start
        and stop actions.
        Or acquire-release the app resource lock and run status.
        """
        self.lock(action)
        if action == "status":
            self.unlock()
        try:
            return self._run(action, dedicated_log=dedicated_log, return_out=return_out)
        finally:
            self.unlock()

    def _run(self, action, dedicated_log=True, return_out=False):
        """
        Do script validations, run the command associated with the action and
        catch errors.
        """
        if self.script is None:
            return 1

        if not os.path.exists(self.script):
            if action == "start":
                self.log.error("script %s does not exist. can't run %s "
                               "action", self.script, action)
                return 1
            elif action == "stop":
                self.log.info("script %s does not exist. hosting fs might "
                              "already be down", self.script)
                return 0
            elif return_out:
                return 0
            else:
                self.status_log("script %s does not exist" % self.script)
                raise StatusWARN()

        self.set_executable()

        try:
            return self._run_cmd(action, dedicated_log=dedicated_log, return_out=return_out)
        except OSError as exc:
            if exc.errno == 8:
                if not return_out and not dedicated_log:
                    self.status_log("exec format error")
                    raise StatusWARN()
                else:
                    self.log.error("%s execution error (Exec format error)", self.script)
                return 1
            else:
                self.svc.save_exc()
                return 1
        except:
            self.svc.save_exc()
            return 1

    def _run_cmd(self, action, dedicated_log=True, return_out=False):
        """
        Switch between buffered outputs or polled execution.
        Return stdout if <return_out>, else return the returncode.
        """
        cmd = [self.script, action]
        if dedicated_log:
            return self._run_cmd_dedicated_log(action, cmd)
        elif return_out:
            out, err, ret = justcall(cmd)
            if ret != 0:
                return "Error: info not implemented in launcher"
            return out
        else:
            out, err, ret = justcall(cmd)
            self.log.debug("%s returned out=[%s], err=[%s], ret=[%d]", cmd, out, err, ret)
            return ret

    def _run_cmd_dedicated_log(self, action, cmd):
        """
        Poll stdout and stderr to log as soon as new lines are available.
        """
        outf = os.path.join(
            rcEnv.pathtmp,
            'svc_'+self.svc.svcname+'_'+os.path.basename(self.script)+'.log'
        )
        ofile = open(outf, 'w')
        kwargs = {
            'stdin': None,
            'stdout': ofile.fileno(),
            'stderr': ofile.fileno(),
        }
        kwargs.update(run_as_popen_kwargs(self.script))
        user = kwargs.get("env").get("LOGNAME")
        self.log.info('exec %s as user %s', ' '.join(cmd), user)
        now = datetime.now()
        proc = Popen(cmd, **kwargs)
        try:
            if self.timeout is None:
                proc.communicate()
            else:
                for _ in range(self.timeout+1):
                    proc.poll()
                    if proc.returncode is not None:
                        break
                    time.sleep(1)
                if proc.returncode is None:
                    self.log.error("execution timeout (%d seconds)", self.timeout)
                    proc.terminate()
                    return 1
                proc.communicate()
        except (KeyboardInterrupt, ex.excSignal):
            _len = datetime.now() - now
            self.log.error('%s interrupted after %s - ret %d - logs in %s',
                           action, _len, 1, outf)
            ofile.close()
            return 1
        _len = datetime.now() - now
        msg = '%s done in %s - ret %d - logs in %s' % (action, _len, proc.returncode, outf)
        if proc.returncode == 0:
            self.log.info(msg)
        else:
            self.log.error(msg)
        ofile.close()
        return proc.returncode

