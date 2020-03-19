from resData import Data

DRIVER_GROUP = "expose"
DRIVER_BASENAME = "envoy"
KEYWORDS = [
    {
        "keyword": "cluster_data",
        "convert": "json",
        "at": True,
        "text": "The envoy protocol compliant data in json format used to bootstrap the Cluster config messages. Parts of this structure, like endpoints, are amended to reflect the actual cluster state."
    },
    {
        "keyword": "filter_config_data",
        "convert": "json",
        "at": True,
        "text": "The envoy protocol compliant data in json format used to bootstrap the Listener filter config messages. Parts of this structure, like routes, are amended by more specific keywords."
    },
    {
        "keyword": "port",
        "convert": "integer",
        "at": True,
        "required": True,
        "text": "The port number of the endpoint."
    },
    {
        "keyword": "protocol",
        "candidates": ["tcp", "udp"],
        "default": "tcp",
        "at": True,
        "text": "The protocol of the endpoint."
    },
    {
        "keyword": "listener_addr",
        "default_text": "The main proxy ip address.",
        "at": True,
        "text": "The public ip address to expose from."
    },
    {
        "keyword": "listener_port",
        "convert": "integer",
        "default_text": "The expose <port>.",
        "at": True,
        "text": "The public port number to expose from. The special value 0 is interpreted as a request for auto-allocation."
    },
    {
        "keyword": "sni",
        "at": True,
        "convert": "list",
        "text": "The SNI server names to match on the proxy to select this service endpoints. The socket server must support TLS."
    },
    {
        "keyword": "lb_policy",
        "default": "round robin",
        "candidates": ["round robin", "least_request", "ring_hash", "random", "original_dst_lb", "maglev"],
        "at": True,
        "text": "The name of the envoy cluster load balancing policy."
    },
    {
        "keyword": "gateway",
        "at": True,
        "text": "The name of the ingress gateway that should handle this expose."
    },
    {
        "keyword": "vhosts",
        "convert": "list",
        "at": True,
        "text": "The list of vhost resource identifiers for this expose."
    },
    {
        "keyword": "listener_certificates",
        "convert": "list",
        "at": True,
        "text": "The TLS certificates used by the listener."
    },
    {
        "keyword": "cluster_certificates",
        "convert": "list",
        "at": True,
        "text": "The TLS certificates used to communicate with cluster endpoints."
    },
    {
        "keyword": "cluster_private_key_filename",
        "at": True,
        "text": "Local filesystem data source of the TLS private key used to communicate with cluster endpoints."
    },
]

def adder(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    r = Expose(**kwargs)
    svc += r

class Expose(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="expose.envoy", **kwargs)
        self.label = "envoy expose %s/%s via %s:%d" % (
            self.options.port,
            self.options.protocol,
            self.options.listener_addr if self.options.listener_addr else "0.0.0.0",
            self.options.listener_port
        ) 

