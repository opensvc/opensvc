from core.resource import DataResource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "route"
DRIVER_BASENAME = "static"
KEYWORDS = [
    {
        "keyword": "spec",
        "at": True,
        "required": True,
        "convert": "shlex",
        "text": "The route specification, passed to the ip route commands, with the appropriate network namespace set.",
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class RouteStatic(DataResource):
    def __init__(self, **kwargs):
        super(RouteStatic, self).__init__(type="route.static", **kwargs)
        self.label = "static route"
