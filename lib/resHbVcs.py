import resHb
import rcStatus
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Hb(resHb.Hb):
    def __init__(self,
                 rid=None,
                 name=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 restart=0,
                 subset=None,
                 disabled=False,
                 tags=set([])):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.vcs",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          tags=tags,
                          always_on=always_on)
        self.label = name

    def _status(self, verbose=False):
        try:
            s = self.svc.get_grp_val('State').strip('|')
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

        if s == "ONLINE":
            return rcStatus.UP
        elif s == "OFFLINE":
            return rcStatus.DOWN
        else:
            self.status_log(s)
            return rcStatus.WARN

