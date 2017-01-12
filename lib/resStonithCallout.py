import resStonith
import rcStatus
from rcUtilities import cmdline2list
from rcGlobalEnv import rcEnv

class Stonith(resStonith.Stonith):
    def __init__(self, rid=None, cmd="/bin/false", **kwargs):
        resStonith.Stonith.__init__(self, rid, type="stonith.callout", **kwargs)
        self.cmd = cmd

    def _start(self):
        _cmd = cmdline2list(self.cmd)
        self.vcall(_cmd)

