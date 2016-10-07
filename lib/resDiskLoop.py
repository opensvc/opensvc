import resources as Res
from rcGlobalEnv import rcEnv

class Disk(Res.Resource):
    """ basic loopback device resource
    """
    def __init__(self,
                 rid=None,
                 loopFile=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Resource.__init__(self,
                              rid,
                              "disk.loop",
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              monitor=monitor,
                              restart=restart,
                              always_on=always_on,
                              subset=subset)
        self.loopFile = loopFile
        self.label = "loop "+loopFile

    def info(self):
        return self.fmt_info([["file", self.loopFile]])

    def __str__(self):
        return "%s loopfile=%s" % (Res.Resource.__str__(self),\
                                 self.loopFile)

if __name__ == "__main__":
    help(Disk)
