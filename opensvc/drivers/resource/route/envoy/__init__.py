from core.resource import DataResource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "route"
DRIVER_BASENAME = "envoy"
KEYWORDS = [
    {
        "keyword": "match_path",
        "at": True,
        "text": "If specified, the route is an exact path rule meaning that the path must exactly match the :path header once the query string is removed. Precisely one of prefix, path, regex must be set.",
    },
    {
        "keyword": "match_regex",
        "at": True,
        "text": " If specified, the route is a regular expression rule meaning that the regex must match the :path header once the query string is removed. The entire path (without the query string) must match the regex. The rule will not match if only a subsequence of the :path header matches the regex.",
        "example": "/b[io]t",
    },
    {
        "keyword": "match_prefix",
        "at": True,
        "text": " If specified, the route is a prefix rule meaning that the prefix must match the beginning of the :path header. Precisely one of prefix, path, regex must be set.",
    },
    {
        "keyword": "match_case_sensitive",
        "at": True,
        "text": "Indicates that prefix/path matching should be case sensitive. The default is ``true``.",
    },
    {
        "keyword": "route_prefix_rewrite",
        "at": True,
        "text": "The string replacing the url path prefix if matching.",
    },
    {
        "keyword": "route_host_rewrite",
        "at": True,
        "text": "Indicates that during forwarding, the host header will be swapped with this value.",
    },
    {
        "keyword": "route_cluster_header",
        "at": True,
        "text": "If the route is not a redirect (host_redirect and/or path_redirect is not specified), one of cluster, cluster_header, or weighted_clusters must be specified. When cluster_header is specified, Envoy will determine the cluster to route to by reading the value of the HTTP header named by cluster_header from the request headers. If the header is not found or the referenced cluster does not exist, Envoy will return a 404 response.",
    },
    {
        "keyword": "route_timeout",
        "at": True,
        "text": "Specifies the timeout for the route. If not specified, the default is 15s. Note that this timeout includes all retries.",
    },
    {
        "keyword": "redirect_host_redirect",
        "at": True,
        "text": "The host portion of the URL will be swapped with this value.",
    },
    {
        "keyword": "redirect_prefix_rewrite",
        "at": True,
        "text": "Indicates that during redirection, the matched prefix (or path) should be swapped with this value. This option allows redirect URLs be dynamically created based on the request.",
    },
    {
        "keyword": "redirect_path_redirect",
        "at": True,
        "text": "Indicates that the route is a redirect rule. If there is a match, a 301 redirect response will be sent which swaps the path portion of the URL with this value. host_redirect can also be specified along with this option.",
    },
    {
        "keyword": "redirect_response_code",
        "at": True,
        "text": "The HTTP status code to use in the redirect response. The default response code is MOVED_PERMANENTLY (301).",
    },
    {
        "keyword": "redirect_https_redirect",
        "convert": "boolean",
        "default": False,
        "at": True,
        "text": "The scheme portion of the URL will be swapped with 'https'.",
    },
    {
        "keyword": "redirect_strip_query",
        "convert": "boolean",
        "default": False,
        "at": True,
        "text": "Indicates that during redirection, the query portion of the URL will be removed. Default value is ``false``.",
    },
    {
        "keyword": "hash_policies",
        "convert": "list",
        "at": True,
        "default": [],
        "default_text": "",
        "text": "The list of hash policy resource ids for the route. Honored if lb_policy is set to ring_hash or maglev.",
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class RouteEnvoy(DataResource):
    def __init__(self, **kwargs):
        super(RouteEnvoy, self).__init__(type="route.envoy", **kwargs)
        self.label = "envoy route"
