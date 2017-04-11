import resStonith
import rcStatus
from rcGlobalEnv import rcEnv

class Stonith(resStonith.Stonith):
    def __init__(self, rid=None, name=None, **kwargs):
        resStonith.Stonith.__init__(self, rid=rid, type="stonith.ilo", **kwargs)
        self.name = name
        self.username, self.password, self.key = self.creds()

    def _start(self):
        cmd = rcEnv.rsh.split() + ['-l', self.username, '-i', self.key, self.name, 'power', 'reset']
        self.vcall(cmd)
