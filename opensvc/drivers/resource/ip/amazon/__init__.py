import core.exceptions as ex
import core.status
import utilities.ifconfig

from .. import Ip, COMMON_KEYWORDS, KW_IPNAME, KW_IPDEV, KW_NETMASK, KW_GATEWAY
from core.objects.svcdict import KEYS
from utilities.net.getaddr import getaddr
from utilities.subsystems.amazon import AmazonMixin

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

def driver_capabilities(node=None):
    import os
    from utilities.proc import which
    data = []
    if not which("ec2"):
        return data
    if os.path.exists("/sys/hypervisor/uuid"):
        with open("/sys/hypervisor/uuid", "r") as f:
           uuid = f.read().lower()
        if not uuid.startswith("ec2"):
           return data
    if os.path.exists("/sys/devices/virtual/dmi/id/product_uuid"):
        with open("/sys/devices/virtual/dmi/id/product_uuid", "r") as f:
           uuid = f.read().lower()
        if not uuid.startswith("ec2"):
           return data
    data.append("ip.amazon")
    return data


class IpAmazon(Ip, AmazonMixin):
    def __init__(self, eip=None, **kwargs):
        Ip.__init__(self, type="ip.amazon", **kwargs)
        self.eip = eip
        self.label = "ec2 ip %s@%s" % (self.ipname, self.ipdev)
        if eip:
            self.label += ", eip %s" % eip

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
            raise ex.Error("can't find instance data")

        ips = []
        for eni in instance_data["NetworkInterfaces"]:
            ips += [ pa["PrivateIpAddress"] for pa in eni["PrivateIpAddresses"] ]
        return ips

    def get_network_interface(self):
        ifconfig = utilities.ifconfig.Ifconfig()
        intf = ifconfig.interface(self.ipdev)
        ips = set(intf.ipaddr + intf.ip6addr)
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.Error("can't find instance data")

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
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN
        if s:
            return core.status.UP
        else:
            return core.status.DOWN

    def check_ping(self, count=1, timeout=5):
        pass

    def start_assign(self):
        if self.is_up():
            self.log.info("ec2 ip %s is already assigned to this node" % self.addr)
            return
        eni = self.get_network_interface()
        if eni is None:
            raise ex.Error("could not find ec2 network interface for %s" % self.ipdev)
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
            raise ex.Error("eip %s is not allocated" % self.eip)
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
            raise ex.Error("could not find ec2 network interface for %s" % self.ipdev)
        data = self.aws([
         "ec2", "unassign-private-ip-addresses",
         "--network-interface-id", eni,
         "--private-ip-address", self.addr
        ])

    def shutdown(self):
        pass


    def provisioner(self):
        self.provisioner_private()
        self.provisioner_public()
        self.provisioner_docker_ip()
        self.cascade_allocation()
        self.log.info("provisioned")
        self.start()
        return True

    def cascade_allocation(self):
        cascade = self.oget("cascade_allocation")
        if not cascade:
            return
        changes = []
        for e in cascade:
            try:
                rid, param = e.split(".")
            except:
                self.log.warning("misformatted cascade entry: %s (expected <rid>.<param>[@<scope>])" % e)
                continue
            if not rid in self.svc.cd:
                self.log.warning("misformatted cascade entry: %s (rid does not exist)" % e)
                continue
            self.log.info("cascade %s to %s" % (self.ipname, e))
            changes.append("%s.%s=%s" % (rid, param, self.ipname))
            self.svc.resources_by_id[rid].ipname = self.svc.conf_get(rid, param)
            self.svc.resources_by_id[rid].addr = self.svc.resources_by_id[rid].ipname
        self.svc.set_multi(changes)

    def provisioner_docker_ip(self):
        if not self.oget("docker_daemon_ip"):
            return
        args = self.svc.oget('DEFAULT', 'docker_daemon_args')
        args += ["--ip", self.ipname]
        self.svc.set_multi(["DEFAULT.docker_daemon_args=%s" % " ".join(args)])
        for r in self.svc.get_resources("container.docker"):
            # reload docker daemon args
            r.on_add()

    def provisioner_private(self):
        if self.ipname != "<allocate>":
            self.log.info("private ip already provisioned")
            return

        eni = self.get_network_interface()
        if eni is None:
            raise ex.Error("could not find ec2 network interface for %s" % self.ipdev)

        ips1 = set(self.get_instance_private_addresses())
        data = self.aws([
          "ec2", "assign-private-ip-addresses",
          "--network-interface-id", eni,
          "--secondary-private-ip-address-count", "1"
        ])
        ips2 = set(self.get_instance_private_addresses())
        new_ip = list(ips2 - ips1)[0]

        self.svc.set_multi(["%s.ipname=%s" % (self.rid, new_ip)])
        self.ipname = new_ip

    def provisioner_public(self):
        if self.eip != "<allocate>":
            self.log.info("public ip already provisioned")
            return

        data = self.aws([
          "ec2", "allocate-address",
          "--domain", "vpc",
        ])

        self.svc.set_multi(["%s.eip=%s" % (self.rid, data["PublicIp"])])
        self.eip = data["PublicIp"]

