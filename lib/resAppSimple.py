"""
The module defining the app.simple resource class.
"""

import os
import subprocess
from datetime import datetime

import resApp
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

class App(resApp.App):
    """
    The simple App resource driver class.
    """

    def __init__(self, rid, **kwargs):
        resApp.App.__init__(self, rid, type="app.simple", **kwargs)

    def _check_simple(self):
        match = self.get_running()
        count = len(match)
        if count == 0:
            return rcStatus.DOWN
        elif count > 1:
            self.status_log("more than of process runs")
            return rcStatus.UP
        else:
            return rcStatus.UP

    def get_running(self):
        cmd = ["pgrep", "-f", " ".join(self.get_cmd("start"))]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []
        match = []
        for pid in out.split():
            cmd = [rcEnv.syspaths.ps, "-p", pid, "e"]
            out, _, _ = justcall(cmd)
            words = out.split()
            if "OPENSVC_SVC_ID="+self.svc.id not in words:
                continue
            if "OPENSVC_RID="+self.rid not in words:
                continue
            match.append(pid)
        return match

    def _check(self):
        try:
            ret = resApp.App._check(self, verbose=False)
            return ret
        except resApp.StatusNA:
            return self._check_simple()

    def stop(self):
        ret = resApp.App.stop(self)
        if ret is not None:
            return ret
        match = self.get_running()
        if not match:
            self.log.info("process not found")
            return
        cmd = ["kill"] + [str(pid) for pid in match]
        ret, _, _ = self.vcall(cmd)
        self.wait_for_fn(lambda: not self.get_running(), 5, 1)
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
        self.log.info("exec '%s' as user %s", ' '.join(cmd), user)
        try:
            subprocess.Popen(cmd, **kwargs)
            return 0
        except Exception as exc:
            self.log.error("%s", exc)
            return 1

