from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext3"

class FsExt3(Fs):
    mkfs = ['mkfs.ext3', '-F', '-q']
    info = ['tune2fs', '-l']

