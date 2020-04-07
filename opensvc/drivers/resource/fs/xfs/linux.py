from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "xfs"

class FsXfs(Fs):
    info = ['xfs_admin', '-l']
    mkfs = ['mkfs.xfs', '-f', '-q']

