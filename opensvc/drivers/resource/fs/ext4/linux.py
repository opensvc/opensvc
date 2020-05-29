from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext4"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mkfs.ext4"):
        return ["fs.ext4"]
    return []

class FsExt4(Fs):
    mkfs = ['mkfs.ext4', '-F', '-q']
    queryfs = ['tune2fs', '-l']

