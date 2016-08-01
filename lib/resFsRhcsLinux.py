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

    def start(self):
        pass

    def stop(self):
        pass

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

