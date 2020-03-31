from . import \
    BaseDiskRaw, \
    adder as base_raw_adder, \
    BASE_RAW_KEYWORDS
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "raw"
KEYWORDS = BASE_RAW_KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    base_raw_adder(svc, s, drv=DiskRaw)


class DiskRaw(BaseDiskRaw):
    def __init__(self,
                 devs=None,
                 user=None,
                 group=None,
                 perm=None,
                 create_char_devices=False,
                 **kwargs):

        super(DiskRaw, self).__init__(**kwargs)
        devs = devs or set()

    def verify_dev(self, dev):
        cmd = ["diskinfo", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True
