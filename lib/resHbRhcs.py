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
                          rid, "hb.rhcs",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          tags=tags,
                          always_on=always_on)
        self.label = name

    def _status(self, verbose=False):
        marker = 'service:'+self.svc.pkg_name
        for line in self.svc.clustat:
            l = line.split()
            if len(l) < 3:
                continue
            if marker != l[0].strip():
                continue

            # package found
            if rcEnv.nodename != self.svc.member_to_nodename(l[1].strip()):
                return rcStatus.DOWN
            elif l[-1].strip() != "started":
                return rcStatus.DOWN
            return rcStatus.UP

        # package not found
        return rcStatus.DOWN
