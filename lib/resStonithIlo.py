import resStonith
import rcStatus
from rcGlobalEnv import rcEnv

class Stonith(resStonith.Stonith):
    def __init__(self,
                 rid=None,
                 name=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None):
        resStonith.Stonith.__init__(self,
                                    rid,
                                    "stonith.ilo",
                                    optional=optional,
                                    disabled=disabled,
                                    always_on=always_on,
                                    tags=tags,
                                    subset=subset)
        self.name = name
        self.username, self.password, self.key = self.creds()

    def _start(self):
        cmd = rcEnv.rsh.split() + ['-l', self.username, '-i', self.key, self.name, 'power', 'reset']
        self.vcall(cmd)
