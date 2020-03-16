"""
The module defining the app.simple resource class.
"""

import os
import subprocess
import hashlib
import time
from datetime import datetime

import resApp
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs


def adder(svc, s, drv=None):
    drv = drv or App
    kwargs = init_kwargs(svc, s)
    kwargs["script"] = svc.oget(s, "script")
    kwargs["start"] = svc.oget(s, "start")
    kwargs["stop"] = svc.oget(s, "stop")
    kwargs["check"] = svc.oget(s, "check")
    kwargs["info"] = svc.oget(s, "info")
    kwargs["status_log"] = svc.oget(s, "status_log")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["start_timeout"] = svc.oget(s, "start_timeout")
    kwargs["stop_timeout"] = svc.oget(s, "stop_timeout")
    kwargs["check_timeout"] = svc.oget(s, "check_timeout")
    kwargs["info_timeout"] = svc.oget(s, "info_timeout")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["group"] = svc.oget(s, "group")
    kwargs["cwd"] = svc.oget(s, "cwd")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["kill"] = svc.oget(s, "kill")
    r = drv(**kwargs)
    svc += r


class App(resApp.App):
    """
    The simple App resource driver class.
    """

    def __init__(self, rid, kill="parent", **kwargs):
        self.kill = kill
        resApp.App.__init__(self, rid, type="app.simple", **kwargs)

    def _check_simple(self):
        match = self.get_running()
        count = len(match)
        if count == 0:
            return rcStatus.DOWN
        elif count > 1:
            self.status_log("more than one process runs (pid %s)" % ", ".join(match))
            return rcStatus.UP
        else:
            return rcStatus.UP

    def ps_pids_e(self, pids):
        """
        When a lot of services and/or a lot of app.simple resources run the
        same program, caching effective.
        """
        cmd = [rcEnv.syspaths.ps, "-p", pids, "e"]
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
            ret = resApp.App._check(self, verbose=False)
            return ret
        except resApp.StatusNA:
            return self._check_simple()

    def stop(self):
        resApp.App.stop(self)
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

