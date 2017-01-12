from rcGlobalEnv import rcEnv
Res = __import__("resFsHP-UX")

class Mount(Res.Mount):
    def __init__(self, **kwargs):
        self.sgname = kwargs.get("device", None)
        Res.Mount.__init__(self, **kwargs)

    def is_up(self):
        if 'resource' in self.svc.cmviewcl and \
           self.mon_name in self.svc.cmviewcl['resource']:
            state = self.svc.cmviewcl['resource'][self.mon_name][('status', rcEnv.nodename)]
            if state == "up":
                return True
            else:
                return False
        else:
            return Res.Mount.is_up(self)

    def start(self):
        pass

    def stop(self):
        pass

