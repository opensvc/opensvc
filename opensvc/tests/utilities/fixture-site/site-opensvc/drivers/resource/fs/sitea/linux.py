from drivers.resource.fs.flag.linux import FsFlag

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "sitea"


class FsSitea(FsFlag):
    def __init__(self, **kwargs):
        super(FsSitea, self).__init__(type="fs.sitea_linux", **kwargs)
