from .. import BaseDisk, BASE_KEYWORDS
from resources import Resource
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs
from svcdict import KEYS

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

    def __init__(self, rid=None, loopFile=None, **kwargs):
        super().__init__(rid, "disk.loop", **kwargs)
        self.loopFile = loopFile
        self.label = "loop "+loopFile

    def _info(self):
        return [["file", self.loopFile]]

    def __str__(self):
        return "%s loopfile=%s" % (super().__str__(), self.loopFile)
