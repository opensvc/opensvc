import resHb
import rcStatus
from rcGlobalEnv import rcEnv

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self, rid=None, name=None, **kwargs):
        resHb.Hb.__init__(self, rid, type="hb.sg", **kwargs)
        self.label = name

    def __status(self, verbose=False):
        if 'node' in self.svc.cmviewcl and \
           rcEnv.nodename in self.svc.cmviewcl['node'] and \
           'status' in self.svc.cmviewcl['node'][rcEnv.nodename] and \
           self.svc.cmviewcl['node'][rcEnv.nodename]['status'] == "up":
            return rcStatus.UP
        return rcStatus.DOWN
