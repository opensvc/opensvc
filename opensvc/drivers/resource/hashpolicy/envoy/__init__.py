from core.resource import DataResource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "hashpolicy"
DRIVER_BASENAME = "envoy"
KEYWORDS = [
    {
        "keyword": "cookie_name",
        "text": "The name of the cookie that will be used to obtain the hash key. If the cookie is not present and ttl below is not set, no hash will be produced.",
    },
    {
        "keyword": "cookie_path",
        "text": "The name of the path for the cookie. If no path is specified here, no path will be set for the cookie.",
    },
    {
        "keyword": "cookie_ttl",
        "convert": "duration",
        "text": "If specified, a cookie with the TTL will be generated if the cookie is not present. If the TTL is present and zero, the generated cookie will be a session cookie.",
    },
    {
        "keyword": "header_header_name",
        "text": "The name of the request header that will be used to obtain the hash key. If the request header is not present, no hash will be produced.",
    },
    {
        "keyword": "connection_source_ip",
        "text": "Hash on source IP address.",
    },
    {
        "keyword": "terminal",
        "convert": "boolean",
        "text": "Shortcircuits the hash computing. This field provides a fallback style of configuration: if a terminal policy doesn't work, fallback to rest of the policy list. It saves time when the terminal policy works.",
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class HashpolicyEnvoy(DataResource):
    def __init__(self, **kwargs):
        super(HashpolicyEnvoy, self).__init__(type="hashpolicy.envoy", **kwargs)
        self.label = "envoy hash policy"

