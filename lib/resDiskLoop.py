import resources as Res
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs


def adder(svc, s, drv=None):
    drv = drv or Disk
    kwargs = init_kwargs(svc, s)
    kwargs["loopFile"] = svc.oget(s, "file")
    r = drv(**kwargs)
    svc += r


class Disk(Res.Resource):
    """
    Base loopback device resource
    """

    def __init__(self, rid=None, loopFile=None, **kwargs):
        Res.Resource.__init__(self, rid, "disk.loop", **kwargs)
        self.loopFile = loopFile
        self.label = "loop "+loopFile

    def _info(self):
        return [["file", self.loopFile]]

    def __str__(self):
        return "%s loopfile=%s" % (Res.Resource.__str__(self),\
                                 self.loopFile)

if __name__ == "__main__":
    help(Disk)
