from __future__ import print_function

import os
import time
import sys

import core.status
import core.exceptions as ex
import utilities.lock
from core.scheduler import SchedOpts
from utilities.lazy import lazy
from core.resource import Resource
from foreign.six.moves import input
from utilities.proc import lcall

KEYWORDS = [
    {
        "keyword": "timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the task run action a failure. If no timeout is set, the agent waits indefinitely for the task command to exit.",
        "example": "5m"
    },
    {
        "keyword": "snooze",
        "at": True,
        "default": 0,
        "convert": "duration",
        "text": "Snooze the service before running the task, so if the command is known to cause a service status degradation the user can decide to snooze alarms for the duration set as value.",
        "example": "10m"
    },
    {
        "keyword": "log",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "Log the task outputs in the service log.",
    },
    {
        "keyword": "confirmation",
        "at": True,
        "default": False,
        "convert": "boolean",
        "candidates": (True, False),
        "text": "If set to True, ask for an interactive confirmation to run the task. This flag can be used for dangerous tasks like data-restore.",
    },
    {
        "keyword": "on_error",
        "at": True,
        "text": "A command to execute on :c-action:`run` action if :kw:`command` returned an error.",
        "example": "/srv/{name}/data/scripts/task_on_error.sh"
    },
    {
        "keyword": "check",
        "candidates": [None, "last_run"],
        "at": True,
        "text": "If set to 'last_run', the last run retcode is used to report a task resource status. If not set (default), the status of a task is always n/a.",
        "example": "last_run"
    },
    {
        "keyword": "user",
        "at": True,
        "text": "The user to impersonate when running the task command. The default user is root.",
        "example": "admin"
    },
    {
        "keyword": "schedule",
        "default_keyword": "run_schedule",
        "at": True,
        "text": "Set the this task run schedule. See ``/usr/share/doc/opensvc/schedule`` for the schedule syntax reference.",
        "example": '["00:00-01:00@61 mon", "02:00-03:00@61 tue-sun"]'
    },
    {
        "keyword": "pre_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`run` action. Errors do not interrupt the action."
    },
    {
        "keyword": "post_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`run` action. Errors do not interrupt the action."
    },
    {
        "keyword": "blocking_pre_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`run` action. Errors interrupt the action."
    },
    {
        "keyword": "blocking_post_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`run` action. Errors interrupt the action."
    },
    {
        "prefixes": ["run"],
        "keyword": "_requires",
        "generic": True,
        "at": True,
        "example": "ip#0 fs#0(down,stdby down)",
        "default": "",
        "text": "A whitespace-separated list of conditions to meet to accept running a '{prefix}' action. A condition is expressed as ``<rid>(<state>,...)``. If states are omitted, ``up,stdby up`` is used as the default expected states."
    },
    {
        "keyword": "group",
        "at": True,
        "text": "If the binary is owned by the root user, run it as the specified group instead of root."
    },
    {
        "keyword": "cwd",
        "at": True,
        "text": "Change the working directory to the specified location instead of the default ``<pathtmp>``."
    },
    {
        "keyword": "umask",
        "at": True,
        "text": "The umask to set for the application process.",
        "example": "022"
    },
]


class BaseTask(Resource):
    default_optional = True
    def __init__(self,
                 type="task",
                 command=None,
                 user=None,
                 group=None,
                 cwd=None,
                 umask=None,
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
        super(BaseTask, self).__init__(type=type, **kwargs)
        self.command = command
        self.on_error = on_error
        self.user = user
        self.group = group
        self.umask = umask
        self.cwd = cwd
        self.snooze = snooze
        self.timeout = timeout
        self.confirmation = confirmation
        self.log_outputs = log
        self.environment = environment
        self.configs_environment = configs_environment
        self.secrets_environment = secrets_environment
        self.checker = check

    def __str__(self):
        return "%s command=%s user=%s" % (
            super(BaseTask, self).__str__(),
            self.command,
            self.user
        )

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
        raise ex.Signal

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
        if self.svc.options.confirm:
            self.log.info("confirmed by command line option")
            return

        print("This task run requires confirmation.\nPlease make sure you fully "
              "understand its role and effects before confirming the run.")
        print("Do you really want to run %s (yes/no) > " % self.rid, end="", flush=True)
 
        import select
        inputs, outputs, errors = select.select([sys.stdin], [], [], 30)
        if inputs:
            buff = sys.stdin.readline().strip()
        else:
            raise ex.Error("timeout waiting for confirmation")

        if buff == "yes":
            self.log.info("run confirmed")
        else:
            raise ex.Error("run aborted")

    def fast_scheduled(self):
        now = time.time()
        s = self.svc.sched.get_schedule(self.rid, "schedule")
        n, i = s.get_next(now, now)
        return i and i < 10

    def run(self):
        try:
            with utilities.lock.cmlock(lockfile=os.path.join(self.var_d, "run.lock"), timeout=0, intent="run"):
                self._run()
        except utilities.lock.LOCK_EXCEPTIONS:
            raise ex.Error("task is already running (maybe too long for the schedule)")
        finally:
            self.svc.notify_done("run", rids=[self.rid])

    def _run(self):
        """
        Update status.json when starting to run a task.
    
        So the running resource list is updated asap in the daemon status
        data, and requesters can see the task they submitted is running.
        """
        if not self.svc.options.cron or not self.fast_scheduled():
            self.svc.print_status_data_eval()
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
            return core.status.NA
        elif self.checker == "last_run":
            try:
                self.check_requires("run")
            except (ex.Error, ex.ContinueAction):
                return core.status.NA
            ret = self.read_last_run_retcode()
            if ret is None:
                return core.status.NA
            if ret:
                self.status_log("last run failed", "error")
                return core.status.DOWN
            return core.status.UP

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
