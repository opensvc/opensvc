import resHb
import rcStatus
from rcGlobalEnv import rcEnv

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
                 subset=None,
                 tags=set([])):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.sg",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          tags=tags,
                          always_on=always_on)
        self.label = name

    def __status(self, verbose=False):
        if 'node' in self.svc.cmviewcl and \
           rcEnv.nodename in self.svc.cmviewcl['node'] and \
           'status' in self.svc.cmviewcl['node'][rcEnv.nodename] and \
           self.svc.cmviewcl['node'][rcEnv.nodename]['status'] == "up":
            return rcStatus.UP
        return rcStatus.DOWN
