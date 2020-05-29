from ..symclone.linux import SyncSymclone
from . import KEYWORDS, DRIVER_GROUP, DRIVER_BASENAME, driver_capabilities
from core.objects.svcdict import KEYS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class SyncSymsnap(SyncSymclone):
    pass
