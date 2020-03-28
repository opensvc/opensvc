from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from core.resource import Resource
from core.objects.builder import init_kwargs
from core.objects.svcdict import KEYS

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "loop"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "size",
        "at": True,
        "required": True,
        "default": "100m",
        "convert": "size",
        "text": "The size of the loop file to provision.",
        "provisioning": True
    },
    {
        "keyword": "file",
        "at": True,
        "required": True,
        "text": "The loopback device backing file full path."
    },
]
DEPRECATED_SECTIONS = {
    "loop": ["disk", "loop"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)

def adder(svc, s, drv=None):
    drv = drv or BaseDiskLoop
    kwargs = init_kwargs(svc, s)
    kwargs["loopFile"] = svc.oget(s, "file")
    r = drv(**kwargs)
    svc += r

class BaseDiskLoop(Resource):
    """
    Base loopback device resource
    """

    def __init__(self, loopFile=None, **kwargs):
        super(BaseDiskLoop, self).__init__(type="disk.loop", **kwargs)
        self.loopFile = loopFile
        self.label = "loop %s" % loopFile

    def _info(self):
        return [["file", self.loopFile]]

    def __str__(self):
        return "%s loopfile=%s" % (super(BaseDiskLoop, self).__str__(), self.loopFile)
