import resSyncSymcloneLinux
import resSyncSymsnap

from svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symsnap"
KEYWORDS = resSyncSymsnap.KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    resSyncSymcloneLinux.adder(svc, s, drv=SyncSymsnap, t="sync.symsnap")


class SyncSymsnap(resSyncSymcloneLinux.SyncSymclone):
    pass
