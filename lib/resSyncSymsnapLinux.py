import resSyncSymcloneLinux
import resSyncSymsnap

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symsnap"
KEYWORDS = resSyncSymsnap.KEYWORDS


def adder(svc, s):
    resSyncSymcloneLinux.adder(svc, s, drv=SyncSymsnap, t="sync.symsnap")


class SyncSymsnap(resSyncSymcloneLinux.SyncSymclone):
    pass
