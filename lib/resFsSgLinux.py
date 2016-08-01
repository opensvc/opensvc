from rcGlobalEnv import rcEnv
Res = __import__("resFsLinux")

class Mount(Res.Mount):
    def __init__(self,
                 rid,
                 mountPoint,
                 device,
                 fsType,
                 mntOpt,
                 always_on=set([]),
                 snap_size=None,
                 disabled=False,
                 tags=set([]),
                 optional=False,
                 monitor=False,
                 restart=0,
                 subset=None):
        self.sgname = device
        Res.Mount.__init__(self,
                           rid,
                           mountPoint,
                           device,
                           fsType,
                           mntOpt,
                           always_on=always_on,
                           snap_size=snap_size,
                           disabled=disabled,
                           tags=tags,
                           optional=optional,
                           monitor=monitor,
                           restart=restart,
                           subset=subset)

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

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

