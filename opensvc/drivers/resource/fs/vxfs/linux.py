from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "vxfs"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mkfs.vxfs"):
        return ["fs.vxfs"]
    return []

class FsVxfs(Fs):
    mkfs = ['mkfs.vxfs', '-o', 'largefiles,bsize=8192']
    queryfs = ['fstyp']

