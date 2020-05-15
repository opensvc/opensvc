import os
import pwd
import sys

import resources as Res
import lock
import rcStatus
import rcExceptions as ex
from rcScheduler import SchedOpts
from rcGlobalEnv import rcEnv
from rcUtilities import lcall, lazy
from six.moves import input

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
                 environment=None,
                 configs_environment=None,
                 secrets_environment=None,
                 check=None,
                 **kwargs):
        Res.Resource.__init__(self, rid, type=type, **kwargs)
        self.command = command
        self.on_error = on_error
        self.user = user
        self.snooze = snooze
        self.timeout = timeout
        self.confirmation = confirmation
        self.log_outputs = log
        self.environment = environment
        self.configs_environment = configs_environment
        self.secrets_environment = secrets_environment
        self.checker = check

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
        self.remove_last_run_retcode()

    def start(self):
        self.remove_last_run_retcode()

    @lazy
    def last_run_retcode_f(self):
        return os.path.join(self.var_d, "last_run_retcode")

    def write_last_run_retcode(self, value):
        with open(self.last_run_retcode_f, "w") as f:
            f.write(str(value))

    def read_last_run_retcode(self):
        try:
            with open(self.last_run_retcode_f, "r") as f:
                return int(f.read())
        except Exception:
            return

    def remove_last_run_retcode(self):
        try:
            os.unlink(self.last_run_retcode_f)
        except Exception:
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
            with lock.cmlock(lockfile=os.path.join(self.var_d, "run.lock"), timeout=0, intent="run"):
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
        pass

    def _status(self, verbose=False):
        if not self.checker:
            return rcStatus.NA
        elif self.checker == "last_run":
            try:
                self.check_requires("run")
            except (ex.excError, ex.excContinueAction):
                return rcStatus.NA
            ret = self.read_last_run_retcode()
            if ret is None:
                return rcStatus.NA
            if ret:
                self.status_log("last run failed", "error")
                return rcStatus.DOWN
            return rcStatus.UP

    def is_provisioned(self, refresh=False):
        return True

    def schedule_options(self):
        return {
            "run": SchedOpts(
                self.rid,
                fname="last_"+self.rid,
                schedule_option="no_schedule"
            )
        }
