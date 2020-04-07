from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext4"

class FsExt4(Fs):
    mkfs = ['mkfs.ext4', '-F', '-q']
    info = ['tune2fs', '-l']

