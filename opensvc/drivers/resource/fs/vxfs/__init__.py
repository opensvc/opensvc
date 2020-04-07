from drivers.resource.fs import KEYWORDS
from core.objects.svcdict import KEYS

KEYS.register_driver(
    "fs",
    "vxfs",
    name=__name__,
    keywords=KEYWORDS,
)

