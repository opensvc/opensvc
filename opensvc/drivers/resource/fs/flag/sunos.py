import os

from . import BaseFsFlag
from utilities.lazy import lazy

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "flag"


class FsFlag(BaseFsFlag):
    @lazy
    def base_flag_d(self):
        flag_dir = os.path.join(os.sep, "system", "volatile")
        if os.path.exists(flag_dir):
            return os.path.join(flag_dir, "opensvc")
        else:
            return os.path.join(os.sep, "var", "run", "opensvc")
