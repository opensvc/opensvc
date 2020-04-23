from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext2"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mkfs.ext2"):
        return ["fs.ext2"]
    return []

class FsExt2(Fs):
    mkfs = ['mkfs.ext2', '-F', '-q']
    queryfs = ['tune2fs', '-l']

