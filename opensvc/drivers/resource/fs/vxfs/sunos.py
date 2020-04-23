from ..sunos import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "vxfs"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("newfs"):
        return ["fs.vxfs"]
    return []

class FsVxfs(Fs):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    queryfs = ['fstyp']

