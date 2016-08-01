import resHb
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall, which

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 restart=0,
                 subset=subset,
                 tags=set([])):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.linuxha",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          always_on=always_on,
                          tags=tags)
        self.status_cmd = 'cl_status'
        self.name = name

    def process_running(self):
        cmd = [self.status_cmd, 'hbstatus']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return False
        if not 'is running' in out:
            return False
        return True

    def __status(self, verbose=False):
        if not which(self.status_cmd):
            self.status_log("heartbeat is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("heartbeat daemons are not running")
            return rcStatus.WARN
        return rcStatus.NA

