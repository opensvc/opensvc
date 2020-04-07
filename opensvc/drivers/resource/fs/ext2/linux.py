from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext2"

class FsExt2(Fs):
    mkfs = ['mkfs.ext2', '-F', '-q']
    info = ['tune2fs', '-l']

