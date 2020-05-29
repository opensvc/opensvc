from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "ext3"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mkfs.ext3"):
        return ["fs.ext3"]
    return []

class FsExt3(Fs):
    mkfs = ['mkfs.ext3', '-F', '-q']
    queryfs = ['tune2fs', '-l']

