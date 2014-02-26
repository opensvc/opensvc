import resStonith
import rcStatus
from rcGlobalEnv import rcEnv

class Stonith(resStonith.Stonith):
    def __init__(self,
                 rid=None,
                 cmd="/bin/false",
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None):
        resStonith.Stonith.__init__(self,
                                    rid,
                                    "stonith.callout",
                                    optional=optional,
                                    disabled=disabled,
                                    always_on=always_on,
                                    tags=tags,
                                    subset=subset)
        self.cmd = cmd

    def _start(self):
        cmd = self.cmd.split(' ')
        self.vcall(cmd)

