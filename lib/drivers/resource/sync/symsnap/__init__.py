from ..symclone import SyncSymclone, adder as base_adder
from core.objects.svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symsnap"
KEYWORDS = [
    {
        "keyword": "recreate_timeout",
        "at": True,
        "default": 300,
        "convert": "duration",
        "text": "Maximum wait time for the clone to reach the created state."
    },
    {
        "keyword": "restore_timeout",
        "at": True,
        "default": 300,
        "convert": "duration",
        "text": "Maximum wait time for the clone to reach the restored state."
    },
    {
        "keyword": "symid",
        "required": True,
        "text": "Identifier of the symmetrix array hosting the source and target devices pairs pointed by :kw:`pairs`."
    },
    {
        "keyword": "pairs",
        "convert": "list",
        "required": True,
        "at": True,
        "text": "Whitespace-separated list of devices ``<src>:<dst>`` devid pairs to drive with this resource.",
        "example": "00B60:00B61 00B62:00B63",
    },
    {
        "keyword": "consistent",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "Use :opt:`-consistent` in symclone commands.",
    },
]

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
