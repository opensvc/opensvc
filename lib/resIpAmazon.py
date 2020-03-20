import resIp
import os
import rcStatus
from rcGlobalEnv import rcEnv
from resIp import COMMON_KEYWORDS, KW_IPNAME, KW_IPDEV, KW_NETMASK, KW_GATEWAY
import rcExceptions as ex
from rcAmazon import AmazonMixin
from rcUtilities import getaddr
from svcBuilder import init_kwargs
from svcdict import KEYS

rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "amazon"
KEYWORDS = [
    KW_IPNAME,
    KW_IPDEV,
    KW_NETMASK,
    KW_GATEWAY,
    {
        "keyword": "eip",
        "at": True,
        "text": "The public elastic ip to associate to :kw:`ipname`. The special ``allocate`` value tells the provisioner to assign a new public address.",
        "example": "52.27.90.63"
    },
    {
        "keyword": "cascade_allocation",
        "convert": "list",
        "default": [],
        "provisioning": True,
        "at": True,
        "text": "Set new allocated ip as value to other ip resources :kw:`ipname` parameter. The syntax is a whitespace separated list of ``<rid>.ipname[@<scope>]``.",
        "example": "ip#1.ipname ip#1.ipname@nodes"
    },
    {
        "keyword": "docker_daemon_ip",
        "provisioning": True,
        "at": False,
        "candidates": [True, False],
        "text": "Set new allocated ip as value as a :opt:`--ip <addr>` argument in the :kw:`DEFAULT.docker_daemon_args` parameter.",
        "example": "True"
    },

] + COMMON_KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    """
    Add a resource instance to the object, parsing parameters
    from a configuration section dictionnary.
    """
    kwargs = init_kwargs(svc, s)
    kwargs["ipname"] = svc.oget(s, "ipname")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["eip"] = svc.oget(s, "eip")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    r = Ip(**kwargs)
    svc += r


class Ip(resIp.Ip, AmazonMixin):
    def __init__(self,
                 rid=None,
                 ipname=None,
                 ipdev=None,
                 eip=None,
                 **kwargs):
        resIp.Ip.__init__(self,
                          rid=rid,
                          ipname=ipname,
                          ipdev=ipdev,
                          **kwargs)
        self.label = "ec2 ip %s@%s" % (ipname, ipdev)
        if eip:
            self.label += ", eip %s" % eip

        self.eip = eip

    def get_eip(self):
        ip = getaddr(self.eip, True)
        data = self.aws(["ec2", "describe-addresses", "--public-ips", self.eip], verbose=False)
        try:
            addr = data["Addresses"][0]
        except:
            addr = None
        return addr

    def get_instance_private_addresses(self):
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        ips = []
        for eni in instance_data["NetworkInterfaces"]:
            ips += [ pa["PrivateIpAddress"] for pa in eni["PrivateIpAddresses"] ]
        return ips

    def get_network_interface(self):
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipdev)
        ips = set(intf.ipaddr + intf.ip6addr)
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        for eni in instance_data["NetworkInterfaces"]:
            _ips = set([ pa["PrivateIpAddress"] for pa in eni["PrivateIpAddresses"] ])
            if len(ips & _ips) > 0:
                return eni["NetworkInterfaceId"]

    def is_up(self):
        """Returns True if ip is associated with this node
        """
        self.getaddr()
        ips = self.get_instance_private_addresses()
        if self.addr not in ips:
            return False
        return True

    def _status(self, verbose=False):
        try:
            s = self.is_up()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if s:
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def check_ping(self, count=1, timeout=5):
        pass

    def start_assign(self):
        if self.is_up():
            self.log.info("ec2 ip %s is already assigned to this node" % self.addr)
            return
        eni = self.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipdev)
        data = self.aws([
         "ec2", "assign-private-ip-addresses",
         "--network-interface-id", eni,
         "--private-ip-address", self.addr,
         "--allow-reassignment"
        ])
        self.can_rollback = True

    def start_associate(self):
        if self.eip is None:
            return

        eip = self.get_eip()
        if eip is None:
            raise ex.excError("eip %s is not allocated" % self.eip)
        if "PrivateIpAddress" in eip and eip["PrivateIpAddress"] == self.addr:
            self.log.info("eip %s is already associated to private ip %s" % (eip["PublicIp"], self.addr))
            return
        data = self.aws([
          "ec2", "associate-address",
          "--allocation-id", eip["AllocationId"],
          "--private-ip-address", self.addr,
          "--instance-id", self.get_instance_id()
        ])


    def start(self):
        self.start_assign()
        self.start_associate()

    def stop(self):
        if not self.is_up():
            self.log.info("ec2 ip %s is already unassigned from this node" % self.addr)
            return
        eni = self.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipdev)
        data = self.aws([
         "ec2", "unassign-private-ip-addresses",
         "--network-interface-id", eni,
         "--private-ip-address", self.addr
        ])

    def shutdown(self):
        pass

