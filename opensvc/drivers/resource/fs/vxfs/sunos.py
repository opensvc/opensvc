from ..sunos import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "vxfs"

class FsVxfs(Fs):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    info = ['fstyp']

