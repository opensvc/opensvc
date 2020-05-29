from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "xfs"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mkfs.xfs"):
        return ["fs.xfs"]
    return []

class FsXfs(Fs):
    queryfs = ['xfs_admin', '-l']
    mkfs = ['mkfs.xfs', '-f', '-q']

