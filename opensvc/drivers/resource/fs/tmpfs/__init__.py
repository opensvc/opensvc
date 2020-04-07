from drivers.resource.fs import KWS_VIRTUAL as KEYWORDS
from core.objects.svcdict import KEYS

KEYS.register_driver(
    "fs",
    "tmpfs",
    name=__name__,
    keywords=KEYWORDS,
)

