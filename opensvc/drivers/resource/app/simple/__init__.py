"""
The module defining the app.simple resource class.
"""

import os
import subprocess
import time

import core.status
from .. import App as BaseApp, KEYWORDS as BASE_KEYWORDS, StatusNA
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "app"
DRIVER_BASENAME = "simple"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "kill",
        "candidates": ["parent", "tree"],
        "default": "parent",
        "at": True,
        "text": "Select a process kill strategy to use on resource stop. ``parent`` kill only the parent process forked by the agent. ``tree`` also kill its children."
    },
    {
        "keyword": "script",
        "at": True,
        "required": False,
        "text": "Full path to the app launcher script. Or its basename if the file is hosted in the ``<pathetc>/<namespace>/<kind>/<name>.d/`` path. This script must accept as arg0 the activated actions word: ``start`` for start, ``stop`` for stop, ``status`` for check, ``info`` for info."
    },
    {
        "keyword": "status_log",
        "at": True,
        "required": False,
        "default": False,
        "convert": "boolean",
        "text": "Redirect the checker script stdout to the resource status info-log, and stderr to warn-log. The default is ``false``, for it is common the checker scripts outputs are not tuned for opensvc."
    },
    {
        "keyword": "check_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the app launcher check action a failure. Takes precedence over :kw:`timeout`. If neither :kw:`timeout` nor :kw:`check_timeout` is set, the agent waits indefinitely for the app launcher to return. A timeout can be coupled with :kw:`optional=true` to not abort a service instance check when an app launcher did not return.",
        "example": "180"
    },
    {
        "keyword": "info_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the app launcher info action a failure. Takes precedence over :kw:`timeout`. If neither :kw:`timeout` nor :kw:`info_timeout` is set, the agent waits indefinitely for the app launcher to return. A timeout can be coupled with :kw:`optional=true` to not abort a service instance info when an app launcher did not return.",
        "example": "180"
    },
    {
        "keyword": "start",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> start` on start action. ``false`` do nothing on start action. ``<shlex expression>`` execute the command on start.",
    },
    {
        "keyword": "stop",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> stop` on stop action. ``false`` do nothing on stop action. ``<shlex expression>`` execute the command on stop action.",
    },
    {
        "keyword": "check",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> status` on status evaluation. ``false`` do nothing on status evaluation. ``<shlex expression>`` execute the command on status evaluation.",
    },
    {
        "keyword": "info",
        "at": True,
        "default": False,
        "text": "``true`` execute :cmd:`<script> info` on info action. ``false`` do nothing on info action. ``<shlex expression>`` execute the command on info action.",
    },
    {
        "keyword": "limit_as",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_cpu",
        "convert": "duration",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_core",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_data",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_fsize",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_memlock",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nofile",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nproc",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_rss",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_stack",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_vmem",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "user",
        "at": True,
        "text": "If the binary is owned by the root user, run it as the specified user instead of root."
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
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    data = []
    if Env.sysname == "Windows":
        return data
    data.append("app.simple")
    return data


class AppSimple(BaseApp):
    """
    The simple App resource driver class.
    """

    def __init__(self, kill="parent", **kwargs):
        self.kill = kill
        super(AppSimple, self).__init__(type="app.simple", **kwargs)

    def _check_simple(self):
        match = self.get_running()
        count = len(match)
        if count == 0:
            return core.status.DOWN
        elif count > 1:
            self.status_log("more than one process runs (pid %s)" % ", ".join(match))
            return core.status.UP
        else:
            return core.status.UP

    def ps_pids_e(self, pids):
        """
        When a lot of services and/or a lot of app.simple resources run the
        same program, caching effective.
        """
        cmd = [Env.syspaths.ps, "-p", pids, "e"]
        out, _, _ = justcall(cmd)
        return out

    def get_matching(self, cmd):
        cmd = cmd.replace("(", "\\(").replace(")", "\\)")
        out, err, ret = justcall(["pgrep", "-f", cmd])
        if ret != 0:
            return []
        pids = ",".join(out.split())
        if not pids:
            return []
        return self.ps_pids_e(pids).splitlines()

    def get_running(self, with_children=False):
        cmd = self.get_cmd("start", validate=False)
        if isinstance(cmd, list):
            cmd_s = " ".join(cmd)
        else:
            cmd_s = cmd
        lines = self.get_matching(cmd_s)
        match = []
        for line in lines:
            words = line.split()
            if not words or words[0] == "PID":
                continue
            if "OPENSVC_SVC_ID="+self.svc.id not in words:
                continue
            if "OPENSVC_RID="+self.rid not in words:
                continue
            match.append(words[0])
        if with_children or not match:
            return match
        # exclude child processes
        cmd = ["pgrep", "-P", ",".join(match)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return match
        return list(set(match) - set(out.split()))

    def _check(self):
        try:
            ret = super(AppSimple, self)._check(verbose=False)
            return ret
        except StatusNA:
            return self._check_simple()

    def stop(self):
        super(AppSimple, self).stop()
        match = self.get_running(with_children=self.kill=="tree")
        if not match:
            self.log.info("already stopped")
            return
        cmd = ["kill"] + [str(pid) for pid in match]
        ret, _, _ = self.vcall(cmd)
        self.wait_for_fn(lambda: not self.get_running(), 5, 1, errmsg="Waited too long for process %s to disappear"%(",".join([str(pid) for pid in match])))
        return ret

    def _run_cmd_dedicated_log(self, action, cmd):
        """
        Poll stdout and stderr to log as soon as new lines are available.
        """
        for lim, val in self.limits.items():
            self.log.info("set limit %s = %s", lim, val)

        if hasattr(os, "devnull"):
            devnull = os.open(os.devnull, os.O_RDWR)
        else:
            devnull = os.open("/dev/null", os.O_RDWR)

        kwargs = {
            'stdout': devnull,
            'stderr': devnull,
            'close_fds': os.name != "nt",
        }
        try:
            kwargs.update(self.common_popen_kwargs(cmd))
        except ValueError as exc:
            self.log.error("%s", exc)
            return 1
        user = kwargs.get("env").get("LOGNAME")
        if isinstance(cmd, list):
            cmd_s = ' '.join(cmd)
        else:
            cmd_s = cmd
        self.log.info("exec '%s' as user %s", cmd_s, user)
        try:
            proc = subprocess.Popen(cmd, **kwargs)
            if proc.returncode is not None:
                return proc.returncode
            time.sleep(0.2)
            proc.poll()
            if proc.returncode is not None:
                return proc.returncode
            return 0
        except Exception as exc:
            self.log.error("%s", exc)
            return 1

