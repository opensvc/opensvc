from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "vxfs"

class FsVxfs(Fs):
    mkfs = ['mkfs.vxfs', '-o', 'largefiles,bsize=8192']
    info = ['fstyp']

