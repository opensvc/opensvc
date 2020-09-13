from drivers.resource.fs.flag.linux import FsFlag

DRIVER_GROUP = "sitegrp"
DRIVER_BASENAME = ""


class Sitegrp(FsFlag):
    def __init__(self, **kwargs):
        super(Sitegrp, self).__init__(type="sitegrp_base", **kwargs)
