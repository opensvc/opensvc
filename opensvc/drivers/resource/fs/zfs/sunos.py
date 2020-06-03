from . import FsZfsMixin
from ..sunos import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "zfs"

class FsZfs(FsZfsMixin, Fs):
    pass

