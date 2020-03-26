import os

from . import BaseFsFlag
from rcUtilities import lazy
from svcBuilder import init_kwargs
from core.objects.svcdict import KEYS

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "flag"
KEYWORDS = []

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    r = FsFlag(**kwargs)
    svc += r

class FsFlag(BaseFsFlag):
    @lazy
    def base_flag_d(self):
        flag_dir = os.path.join(os.sep, "system", "volatile")
        if os.path.exists(flag_dir):
            return os.path.join(flag_dir, "opensvc")
        else:
            return os.path.join(os.sep, "var", "run", "opensvc")
