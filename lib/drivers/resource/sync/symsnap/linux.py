from ..symclone.linux import SyncSymclone, adder as base_adder
from . import KEYWORDS, DRIVER_GROUP, DRIVER_BASENAME
from core.objects.svcdict import KEYS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    base_adder(svc, s, drv=SyncSymsnap, t="sync.symsnap")


class SyncSymsnap(SyncSymclone):
    pass
