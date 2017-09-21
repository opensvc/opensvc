"""
The module defining the App resource and RsetApps resourceset objects.
"""
from subprocess import Popen
from datetime import datetime
import os
import pwd
import time
import stat
import shlex

from rcUtilities import justcall, which, lazy, is_string, lcall
from converters import convert_boolean
from rcGlobalEnv import rcEnv
from resources import Resource
from resourceset import ResourceSet
import rcStatus
import rcExceptions as ex
import lock

def run_as_popen_kwargs(fpath, limits):
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
    cwd = rcEnv.paths.pathtmp
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
        user_home_dir = rcEnv.paths.pathtmp
    env = os.environ.copy()
    env['HOME'] = user_home_dir
    env['LOGNAME'] = user_name
    env['PWD'] = cwd
    env['USER'] = user_name
    return {'preexec_fn': preexec(user_uid, user_gid, limits), 'cwd': cwd, 'env': env}

def preexec(user_uid, user_gid, limits):
    set_rlimits(limits)
    demote(user_uid, user_gid)

def set_rlimits(limits):
    """
    Set the resource limits for the executed command.
    """
    try:
        import resource
    except ImportError:
        return
    for res, val in limits.items():
        rlim = getattr(resource, "RLIMIT_"+res.upper())
        _vs, _vg = resource.getrlimit(rlim)
        resource.setrlimit(rlim, (val, val))

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

        Resource.__init__(self, rid, "app", **kwargs)
        self.script = script
        self.start_seq = start
        self.stop_seq = stop
        self.check_seq = check
        self.info_seq = info
        self.timeout = timeout
        if script:
            self.label = os.path.basename(script)
        self.lockfd = None
        try:
            # compat
            self.sort_key = "app#%d" % int(self.start_seq)
        except (TypeError, ValueError):
            pass

    @lazy
    def lockfile(self):
        """
        Lazy init for the resource lock file path property.
        """
        lockfile = os.path.join(rcEnv.paths.pathlock, self.svc.svcname)
        lockfile = ".".join((lockfile, self.rid))
        return lockfile

    def validate_on_action(self, cmd):
        """
        Do sanity checks on the resource parameters before running an action.
        """
        cmd = self.validate_script_path(cmd)
        self.validate_script_exec(cmd)
        return cmd

    def validate_script_exec(self, cmd):
        """
        Invalidate the script if the file is not executable or not found.
        """
        if cmd is None:
            return
        if which(cmd[0]) is None:
            self.status_log("%s is not executable" % cmd[0])
            self.set_executable(cmd)

    def validate_script_path(self, cmd):
        """
        Converts the script path to a realpath.
        Invalidate the script if not found.
        If the script is specified as a basename, consider it is to be found
        in the <pathetc>/<svcname>.d directory.
        """
        if cmd is None:
            return
        if not cmd[0].startswith('/'):
            cmd[0] = os.path.join(self.svc.paths.initd, cmd[0])
        if os.path.exists(cmd[0]):
            cmd[0] = os.path.realpath(cmd[0])
            return cmd
        raise ex.excError("%s does not exist" % cmd[0])

    def is_up(self):
        """
        Return 0 if the app resource is up.
        """
        if self.pg_frozen():
            raise StatusNA()
        if self.check_seq is False:
            self.status_log("check is not set", "info")
            raise StatusNA()
        try:
            cmd = self.get_cmd("check", "status")
        except ex.excAbortAction:
            raise StatusNA()
        except ex.excError as exc:
            self.status_log(str(exc), "warn")
            raise StatusNA()
        ret = self.run("status", cmd, dedicated_log=False)
        return ret

    def info(self):
        """
        Contribute app resource standard and script-provided key/val pairs
        to the service's resinfo.
        """
        keyvals = [
            ["script", self.script if self.script else ""],
            ["start", str(self.start_seq) if self.start_seq else ""],
            ["stop", str(self.stop_seq) if self.stop_seq else ""],
            ["check", str(self.check_seq) if self.check_seq else ""],
            ["info", str(self.info_seq) if self.info_seq else ""],
            ["timeout", str(self.timeout) if self.timeout else ""],
        ]
        try:
            cmd = self.get_cmd("info")
        except ex.excAbortAction:
            return self.fmt_info(keyvals)

        buff = self.run('info', cmd, dedicated_log=False, return_out=True)
        if not is_string(buff) or len(buff) == 0:
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

    def get_cmd(self, action, script_arg=None):
        key = action + "_seq"
        val = getattr(self, key)
        if val is False:
            raise ex.excAbortAction()
        try:
            int(val)
            cmd = [self.script, script_arg if script_arg else action]
        except (TypeError, ValueError):
            try:
                val = convert_boolean(val)
                if val is False:
                    raise ex.excAbortAction()
                cmd = [self.script, script_arg if script_arg else action]
            except:
                cmd = shlex.split(val)
        cmd = self.validate_on_action(cmd)
        return cmd

    def start(self):
        """
        Start the resource.
        """
        self.create_pg()

        try:
            cmd = self.get_cmd("start")
        except ex.excAbortAction:
            return

        try:
            status = self.is_up()
        except:
            status = 1

        if status == 0:
            self.log.info("%s is already started", self.label)
            return

        ret = self.run("start", cmd)
        if ret != 0:
            raise ex.excError()
        self.can_rollback = True

    def stop(self):
        """
        Stop the resource.
        """
        try:
            cmd = self.get_cmd("stop")
        except ex.excAbortAction:
            return

        if self.status() == rcStatus.DOWN:
            self.log.info("%s is already stopped", self.label)
            return
        try:
            self.run("stop", cmd)
        except Exception as exc:
            self.log.warning(exc)

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
        n_ref_res = len(self.svc.get_resources(['fs', 'ip', 'container', 'share', 'disk']))
        status = self.svc.group_status(excluded_groups=set([
            "sync",
            "app",
            "disk.scsireserv",
            "disk.drbd",
            "hb"
        ]))
        if n_ref_res > 0 and str(status["avail"]) not in ("up", "n/a"):
            self.log.debug("abort resApp status because needed resources avail status is %s", status["avail"])
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
            return rcStatus.UP
        elif ret == 1:
            return rcStatus.DOWN

        self.status_log("check reports errors (%d)" % ret)
        return rcStatus.WARN

    def set_executable(self, cmd):
        """
        Switch the script file execution bit to on.
        """
        if not os.path.exists(cmd[0]):
            return
        self.vcall(['chmod', '+x', cmd[0]])

    def run(self, action, cmd, dedicated_log=True, return_out=False):
        """
        Acquire the app resource lock, run the action and release for info, start
        and stop actions.
        Or acquire-release the app resource lock and run status.
        """
        self.lock(action)
        if action == "status":
            self.unlock()
        try:
            return self._run(action, cmd, dedicated_log=dedicated_log, return_out=return_out)
        finally:
            self.unlock()

    def _run(self, action, cmd, dedicated_log=True, return_out=False):
        """
        Do script validations, run the command associated with the action and
        catch errors.
        """
        try:
            return self._run_cmd(action, cmd, dedicated_log=dedicated_log, return_out=return_out)
        except OSError as exc:
            if exc.errno == 8:
                if not return_out and not dedicated_log:
                    self.status_log("exec format error")
                    raise StatusWARN()
                else:
                    self.log.error("execution error (Exec format error)")
            elif exc.errno == 13:
                if not return_out and not dedicated_log:
                    self.status_log("permission denied")
                    raise StatusWARN()
                else:
                    self.log.error("execution error (Permission Denied)")
            else:
                self.svc.save_exc()
            return 1
        except:
            self.svc.save_exc()
            return 1

    def _run_cmd(self, action, cmd, dedicated_log=True, return_out=False):
        """
        Switch between buffered outputs or polled execution.
        Return stdout if <return_out>, else return the returncode.
        """
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

    @lazy
    def limits(self):
        data = {}
        try:
            import resource
        except ImportError:
            return data
        for key in ("as", "cpu", "core", "data", "fsize", "memlock", "nofile", "nproc", "rss", "stack", "vmem"):
            try:
                data[key] = self.conf_get("limit_"+key)
            except ex.OptNotFound:
                continue
            rlim = getattr(resource, "RLIMIT_"+key.upper())
            _vs, _vg = resource.getrlimit(rlim)
            if data[key] > _vs:
                if data[key] > _vg:
                    _vg = data[key]
                resource.setrlimit(rlim, (data[key], _vg))
                _vs, _vg = resource.getrlimit(rlim)
            self.log.info("limit %s = %s", key, data[key])
        return data

    def _run_cmd_dedicated_log(self, action, cmd):
        """
        Poll stdout and stderr to log as soon as new lines are available.
        """
        kwargs = {
            'stdin': None,
            'timeout': self.timeout,
            'logger': self.log,
        }
        try:
            kwargs.update(run_as_popen_kwargs(cmd[0], self.limits))
        except ValueError as exc:
            self.log.error("%s", exc)
            return 1
        user = kwargs.get("env").get("LOGNAME")
        self.log.info('exec %s as user %s', ' '.join(cmd), user)
        now = datetime.now()
        try:
            ret = lcall(cmd, **kwargs)
        except (KeyboardInterrupt, ex.excSignal):
            _len = datetime.now() - now
            self.log.error('%s interrupted after %s - ret %d',
                           action, _len, 1)
            return 1
        _len = datetime.now() - now
        msg = '%s done in %s - ret %d' % (action, _len, ret)
        if ret == 0:
            self.log.info(msg)
        else:
            self.log.error(msg)
        return ret

