from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "none"

class FsNone(Fs):
    def check_stat_device(self):
        return True

