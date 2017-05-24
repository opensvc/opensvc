Res = __import__("resIpLinux")

import rcStatus
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

class Ip(Res.Ip):
    def start(self):
        return 0

    def stop(self):
        return 0

    def _status(self, verbose=False):
        cmd = [rcEnv.syspaths.ip, 'addr', 'ls']
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.status_log("%s exec failed"%' '.join(cmd))
            return rcStatus.WARN
        if " "+self.addr+"/" in out:
            return rcStatus.UP
        return rcStatus.DOWN

