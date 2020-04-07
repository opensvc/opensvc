from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from core.resource import Resource
from core.objects.svcdict import KEYS

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
        "protoname": "loopfile",
        "at": True,
        "required": True,
        "text": "The loopback device backing file full path."
    },
]
DEPRECATED_SECTIONS = {
    "loop": ["disk", "loop"],
}

KEYS.register_driver(
    "disk",
    "loop",
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)


class BaseDiskLoop(Resource):
    """
    Base loopback device resource
    """

    def __init__(self, loopfile=None, size=None, **kwargs):
        super(BaseDiskLoop, self).__init__(type="disk.loop", **kwargs)
        self.loopfile = loopfile
        self.size = size
        self.label = "loop %s" % loopfile

    def _info(self):
        return [["file", self.loopfile]]

    def __str__(self):
        return "%s loopfile=%s" % (super(BaseDiskLoop, self).__str__(), self.loopfile)
