import os

from . import BaseFsFlag
from utilities.lazy import lazy

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "flag"

class FsFlag(BaseFsFlag):
    @lazy
    def base_flag_d(self):
        return os.path.join(os.sep, "dev", "shm", "opensvc")
