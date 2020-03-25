"""
The module defining the app.simple resource class.
"""

import os
import subprocess
from datetime import datetime

import rcStatus
import rcExceptions as ex
from . import AppSimple as ParentAppSimple, adder as parent_adder
from rcGlobalEnv import rcEnv
from utilities.proc import justcall


def adder(svc, s):
    parent_adder(svc, s, drv=AppSimple)


class AppSimple(ParentAppSimple):
    """
    The simple App resource driver class.
    """
    def get_running(self, with_children=False):
        cmd = ["pgrep", "-f", " ".join(self.get_cmd("start", validate=False))]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []
        match = []
        for pid in out.split():
            cmd = ["pargs", "-e", pid]
            out, _, _ = justcall(cmd)
            words = out.split()
            if "OPENSVC_SVC_ID="+self.svc.id not in words:
                continue
            if "OPENSVC_RID="+self.rid not in words:
                continue
            match.append(pid)
        if with_children:
            return match
        # exclude child processes
        cmd += ["-P", ",".join(match)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return match
        return list(set(match) - set(out.split()))

