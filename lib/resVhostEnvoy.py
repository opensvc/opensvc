from resData import Data
from svcdict import KEYS

DRIVER_GROUP = "vhost"
DRIVER_BASENAME = "envoy"
KEYWORDS = [
    {
        "keyword": "domains",
        "convert": "list",
        "at": True,
        "text": "The list of http domains in this expose.",
        "default": "{name}",
        "example": "{name}"
    },
    {
        "keyword": "routes",
        "convert": "list",
        "at": True,
        "default": [],
        "text": "The list of route resource identifiers for this vhost."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    r = Vhost(**kwargs)
    svc += r

class Vhost(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="vhost.envoy", **kwargs)
        if self.options.domains:
            self.label = "envoy vhost %s" % ", ".join(self.options.domains)
        else:
            self.label = "envoy vhost"

