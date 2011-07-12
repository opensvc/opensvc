import resStonith
import rcStatus
from rcGlobalEnv import rcEnv

class Stonith(resStonith.Stonith):
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        resStonith.Stonith.__init__(self, rid, "stonith.ilo",
                                    optional=optional, disabled=disabled, tags=tags)
        self.name = name

    def _start(self):
        username, password, key = self.creds()
        cmd = rcEnv.rsh.split() + ['-l', username, '-i', key, self.name, 'power', 'cycle']
        self.vcall(cmd)
