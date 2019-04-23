import os
import pwd
import sys

import resources as Res
import lock
import rcStatus
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import lcall
from six.moves import input

def run_as_popen_kwargs(user):
    if rcEnv.sysname == "Windows":
        return {}
    if user is None:
        return {}
    cwd = rcEnv.paths.pathtmp
    import pwd
    try:
        pw_record = pwd.getpwnam(user)
    except Exception as exc:
        raise ex.excError("user lookup failure: %s" % str(exc))
    user_name      = pw_record.pw_name
    user_home_dir  = pw_record.pw_dir
    user_uid  = pw_record.pw_uid
    user_gid  = pw_record.pw_gid
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

class Task(Res.Resource):
    default_optional = True
    def __init__(self,
                 rid=None,
                 type="task",
                 command=None,
                 user=None,
                 on_error=None,
                 timeout=0,
                 snooze=0,
                 confirmation=False,
                 log=True,
                 **kwargs):
        Res.Resource.__init__(self, rid, type=type, **kwargs)
        self.command = command
        self.on_error = on_error
        self.user = user
        self.snooze = snooze
        self.timeout = timeout
        self.confirmation = confirmation
        self.log_outputs = log

    def __str__(self):
        return "%s command=%s user=%s" % (Res.Resource.__str__(self), self.command, str(self.user))

    def _info(self):
        data = [
          ["command", self.command],
        ]
        if self.on_error:
            data.append(["on_error", self.on_error])
        if self.user:
            data.append(["user", self.user])
        return data

    def has_it(self):
        return False

    def is_up(self):
        return False

    def stop(self):
        pass

    def start(self):
        pass

    @staticmethod
    def alarm_handler(signum, frame):
        raise ex.excSignal

    def lcall(self, *args, **kwargs):
        """
        Wrap lcall, setting the resource logger
        """
        if self.log_outputs:
            kwargs["logger"] = self.log
        else:
            kwargs["logger"] = None
        return lcall(*args, **kwargs)

    def confirm(self):
        """
        Ask for an interactive confirmation. Raise if the user aborts or
        if no input is given before timeout.
        """
        if not self.confirmation:
            return
        import signal
        signal.signal(signal.SIGALRM, self.alarm_handler)
        signal.alarm(30)

        print("This task run requires confirmation.\nPlease make sure you fully "
              "understand its role and effects before confirming the run.")
        try:
            buff = input("Do you really want to run %s (yes/no) > " % self.rid)
        except ex.excSignal:
            raise ex.excError("timeout waiting for confirmation")

        if buff == "yes":
            signal.alarm(0)
            self.log.info("run confirmed")
        else:
            raise ex.excError("run aborted")

    def run(self):
        try:
            with lock.cmlock(lockfile=os.path.join(self.var_d, "run.lock"), timeout=0):
                self._run()
        except lock.LOCK_EXCEPTIONS:
            raise ex.excError("task is already running (maybe too long for the schedule)")
        finally:
            self.svc.notify_done("run", rids=[self.rid])

    def _run(self):
        self.create_pg()
        if self.snooze:
            try:
                data = self.svc._snooze(self.snooze)
                self.log.info(data.get("info", ""))
            except Exception as exc:
                self.log.warning(exc)
        self._run_call()
        if self.snooze:
            try:
                data = self.svc._unsnooze()
                self.log.info(data.get("info", ""))
            except Exception as exc:
                self.log.warning(exc)


    def _run_call(self):
        kwargs = {
            'timeout': self.timeout,
            'blocking': True,
        }
        kwargs.update(run_as_popen_kwargs(self.user))

        try:
            self.action_triggers("", "command", **kwargs)
        except ex.excError:
            if self.on_error:
                kwargs["blocking"] = False
                self.action_triggers("", "on_error", **kwargs)
            raise

    def _status(self, verbose=False):
        return rcStatus.NA

    def is_provisioned(self, refresh=False):
        return True

