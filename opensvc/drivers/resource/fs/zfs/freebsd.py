from . import FsZfsMixin
from ..freebsd import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "zfs"

class FsZfs(FsZfsMixin, Fs):
    pass

