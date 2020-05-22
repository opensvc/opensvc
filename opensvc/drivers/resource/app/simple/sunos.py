"""
The module defining the app.simple resource class.
"""

from . import AppSimple as ParentAppSimple
from utilities.proc import justcall

DRIVER_GROUP = "app"
DRIVER_BASENAME = "simple"

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

