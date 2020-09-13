from drivers.resource.fs.flag.linux import FsFlag

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "siteb"


class FsSiteb(FsFlag):
    def __init__(self, **kwargs):
        super(FsSiteb, self).__init__(type="fs.siteb_non_os", **kwargs)
