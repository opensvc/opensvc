import os

import core.exceptions as ex
import core.status
import utilities.ifconfig

from drivers.resource.ip.host.linux import IpHost
from drivers.resource.ip import KW_IPNAME, KW_IPDEV, KW_NETMASK, KW_GATEWAY, COMMON_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall, which
from utilities.net.converters import to_cidr

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "netns"
DRIVER_BASENAME_ALIASES = ["docker"]
KEYWORDS = [
    KW_IPNAME,
    KW_IPDEV,
    KW_NETMASK,
    KW_GATEWAY,
    {
        "keyword": "netns",
        "at": True,
        "required": True,
        "text": "The resource id of the container to plumb the ip into.",
        "example": "container#0"
    },
    {
        "keyword": "vlan_tag",
        "depends": [("mode", "ovs")],
        "at": True,
        "required": False,
        "text": "The VLAN tag the switch port will relay.",
        "example": "44 45"
    },
    {
        "keyword": "vlan_mode",
        "candidates": ["access", "native-tagged", "native-untagged"],
        "depends": [("mode", "ovs")],
        "at": True,
        "required": False,
        "default": "native-untagged",
        "text": "The VLAN port mode.",
        "example": "access"
    },
    {
        "keyword": "mode",
        "at": True,
        "required": False,
        "candidates": ["bridge", "dedicated", "macvlan", "ipvlan-l2", "ipvlan-l3", "ovs"],
        "text": "The ip link mode. If ipdev is set to a bridge interface the mode defaults to bridge, else defaults to macvlan. ipvlan requires a 4.2+ kernel.",
        "example": "container#0"
    },
    {
        "keyword": "nsdev",
        "at": True,
        "required": False,
        "text": "If specified, use this interface name in the netns. If not specified the first free ``eth<n>`` is chosen."
    },
    {
        "keyword": "macaddr",
        "at": True,
        "required": False,
        "text": "If specified, use this mac address in the netns."
    },
    {
        "keyword": "del_net_route",
        "at": True,
        "default": False,
        "example": "true",
        "text": "Some docker ip configuration requires dropping the network route autoconfigured when installing the ip address. In this case set this parameter to true, and also set the network parameter."
    },
] + COMMON_KEYWORDS
DEPRECATED_KEYWORDS = {
    "ip.docker.container_rid": "netns",
    "ip.netns.container_rid": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "ip.docker.netns": "container_rid",
    "ip.netns.netns": "container_rid",
}
DEPRECATED_SECTIONS = {
    "ip.docker": ["ip", "netns"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if Env.sysname == "Linux" and which("ip"):
        return ["ip.netns"]
    return []

class IpNetns(IpHost):
    def __init__(self,
                 mode=None,
                 network=None,
                 del_net_route=False,
                 netns=None,
                 nsdev=None,
                 macaddr=None,
                 vlan_tag=None,
                 vlan_mode=None,
                 **kwargs):
        super(IpNetns, self).__init__(type="ip.netns", **kwargs)
        self.mode = mode if mode else "bridge"
        self.network = network
        self.nsdev = nsdev
        self.macaddr = macaddr
        self.del_net_route = del_net_route
        self.container_rid = str(netns)
        self.vlan_tag = vlan_tag
        self.vlan_mode = vlan_mode
        self.tags = self.tags | set(["docker"])
        self.tags.add(self.container_rid)

    def on_add(self):
        self.svc.register_dependency("start", self.rid, self.container_rid)
        self.svc.register_dependency("start", self.container_rid, self.rid)
        self.svc.register_dependency("stop", self.container_rid, self.rid)
        self.set_label()

    def set_macaddr(self):
        """
        Set the intf mac addr
        """
        if not self.macaddr:
            return
        try:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", self.final_guest_dev, "address", self.macaddr]
            ret, out, err = self.vcall(cmd)
        except ex.Error:
             pass
        if ret != 0:
            return ret, out, err

    def set_label(self):
        """
        Set the resource label property.
        """
        try:
             self.get_mask()
        except ex.Error:
             pass
        self.label = "netns %s %s/%s %s@%s" % (self.mode, self.ipname, to_cidr(self.netmask), self.ipdev, self.container_rid)

    @lazy
    def guest_dev(self):
        """
        Find a free eth netdev.

        Execute a ip link command in the container net namespace to parse
        used eth netdevs.
        """
        if self.netns is None:
            raise ex.Error("could not determine netns")
        with open("/proc/net/dev", "r") as filep:
            local_devs = [line.split(":", 1)[0] for line in filep.readlines() if ":" in line]

        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip" , "link"]
        out, err, ret = justcall(cmd)
        used = []
        for line in out.splitlines():
            if ": eth" not in line:
                continue
            idx = line.split()[1].replace(":", "").replace("eth", "")
            if "@" in idx:
                # strip "@if<n>" suffix
                idx = idx[:idx.index("@")]
            try:
                used.append(int(idx))
            except ValueError:
                # user named interface. ex: eth-metier
                continue
        idx = 0
        nspid = self.get_nspid()
        while True:
            guest_dev = "eth%d" % idx
            local_dev = "v%spl%s" % (guest_dev, nspid)
            if idx not in used and local_dev not in local_devs:
                return guest_dev
            idx += 1

    @lazy
    def final_guest_dev(self):
        if self.nsdev:
            return self.nsdev
        else:
            return self.guest_dev

    @lazy
    def container(self):
        if self.container_rid not in self.svc.resources_by_id:
            raise ex.Error("rid %s not found" % self.container_rid)
        return self.svc.resources_by_id[self.container_rid]

    def container_id(self, refresh=False):
        if self.container.type in ("container.lxd", "container.lxc"):
            return self.container.name
        elif self.container.type in ("container.docker", "container.podman"):
            return self.container.lib.get_container_id(self.container, refresh=refresh)
        else:
            raise ex.Error("unsupported container %s type: %s" % (
                self.container.rid,
                self.container.type,
            ))

    def arp_announce(self):
        """ disable the generic arping. We do that in the guest namespace.
        """
        pass

    def get_docker_ifconfig(self):
        try:
            nspid = self.get_nspid()
        except ex.Error as e:
            return
        if nspid is None:
            return
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ifconfig = utilities.ifconfig.Ifconfig(ip_out=out)
        return ifconfig

    def abort_start(self):
        return super(IpNetns, self).abort_start()

    def is_up(self):
        if not self.container.is_up():
            return False
        ifconfig = self.get_docker_ifconfig()
        if ifconfig is None:
            return False
        return super(IpNetns, self)._is_up(ifconfig)

    def get_docker_interface(self):
        ifconfig = self.get_docker_ifconfig()
        if ifconfig is None:
            return
        for intf in ifconfig.intf:
            if self.addr in intf.ipaddr+intf.ip6addr:
                name = intf.name
                if "@" in name:
                    name = name[:name.index("@")]
                return name
        return

    def container_running_elsewhere(self):
        return False

    def _status(self, verbose=False):
        self.unset_lazy("netns")
        if self.container_running_elsewhere():
            self.status_log("%s is hosted by another host" % self.container_rid, "info")
            return core.status.NA
        ret = super(IpNetns, self)._status()
        return ret

    def startip_cmd(self):
        self.unset_lazy("netns")
        if "dedicated" in self.tags or self.mode == "dedicated":
            self.log.info("dedicated mode")
            return self.startip_cmd_dedicated()
        else:
            return self.startip_cmd_shared()

    def startip_cmd_shared(self):
        if self.mode is None:
            if os.path.exists("/sys/class/net/%s/bridge" % self.ipdev):
                self.log.info("bridge mode")
                return self.startip_cmd_shared_bridge()
            else:
                self.log.info("macvlan mode")
                return self.startip_cmd_shared_macvlan()
        elif self.mode == "bridge":
            self.log.info("bridge mode")
            return self.startip_cmd_shared_bridge()
        elif self.mode == "macvlan":
            self.log.info("macvlan mode")
            return self.startip_cmd_shared_macvlan()
        elif self.mode == "ipvlan-l2":
            self.log.info("ipvlan-l2 mode")
            return self.startip_cmd_shared_ipvlan("l2")
        elif self.mode == "ipvlan-l3":
            self.log.info("ipvlan-l3 mode")
            return self.startip_cmd_shared_ipvlan("l3")
        elif self.mode == "ovs":
            self.log.info("ovs mode")
            return self.startip_cmd_shared_ovs()

    def startip_cmd_dedicated(self):
        # assign interface to the nspid
        nspid = self.get_nspid()
        if nspid is None:
            raise ex.Error("could not determine nspid")
        cmd = [Env.syspaths.ip, "link", "set", self.ipdev, "netns", nspid, "name", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # plumb the ip
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "add", "%s/%s" % (self.addr, to_cidr(self.netmask)), "dev", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # activate
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", self.final_guest_dev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # add default route
        if self.gateway:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "add", "default", "via", self.gateway, "dev", self.final_guest_dev]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                return ret, out, err

        # announce
        cmd = [Env.syspaths.nsenter, "--net="+self.netns] + Env.python_cmd + [os.path.join(Env.paths.pathlib, "utilities", "arp.py"), self.final_guest_dev, self.addr]
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd)

        return 0, "", ""

    def stopip_cmd_shared_ovs(self):
        nspid = self.get_nspid()
        tmp_local_dev = "v%spl%s" % (self.guest_dev, nspid)

        cmd = ["ovs-vsctl", "del-port", self.ipdev, tmp_local_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err
        return ret, out, err
 
    def startip_cmd_shared_ovs(self):
        nspid = self.get_nspid()
        tmp_guest_dev = "v%spg%s" % (self.guest_dev, nspid)
        tmp_local_dev = "v%spl%s" % (self.guest_dev, nspid)
        mtu = self.ip_get_mtu()

        if not which("ovs-vsctl"):
            raise Exception("ovs-vsctl must be installed")

        # create peer devs
        cmd = [Env.syspaths.ip, "link", "add", "name", tmp_local_dev, "mtu", mtu, "type", "veth", "peer", "name", tmp_guest_dev, "mtu", mtu]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        cmd = ["ovs-vsctl", "--may-exist", "add-port", self.ipdev, tmp_local_dev, "vlan_mode=%s" % self.vlan_mode]
        if self.vlan_tag is not None:
            cmd += ["tag=%s" % self.vlan_tag]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        cmd = [Env.syspaths.ip, "link", "set", tmp_local_dev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # assign the interface to the container namespace
        cmd = [Env.syspaths.ip, "link", "set", tmp_guest_dev, "netns", nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # rename the tmp guest dev
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", tmp_guest_dev, "name", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # plumb ip
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "add", self.addr+"/"+to_cidr(self.netmask), "dev", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # setup default route
        self.ip_setup_route()

        return 0, "", ""

    def startip_cmd_shared_bridge(self):
        nspid = self.get_nspid()
        tmp_guest_dev = "v%spg%s" % (self.guest_dev, nspid)
        tmp_local_dev = "v%spl%s" % (self.guest_dev, nspid)
        mtu = self.ip_get_mtu()

        # create peer devs
        cmd = [Env.syspaths.ip, "link", "add", "name", tmp_local_dev, "mtu", mtu, "type", "veth", "peer", "name", tmp_guest_dev, "mtu", mtu]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # activate the parent dev
        cmd = [Env.syspaths.ip, "link", "set", tmp_local_dev, "master", self.ipdev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err
        cmd = [Env.syspaths.ip, "link", "set", tmp_local_dev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # assign the macvlan interface to the container namespace
        cmd = [Env.syspaths.ip, "link", "set", tmp_guest_dev, "netns", nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # rename the tmp guest dev
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", tmp_guest_dev, "name", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # set the mac addr
        self.set_macaddr()

        # plumb ip
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "add", self.addr+"/"+to_cidr(self.netmask), "dev", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # setup default route
        self.ip_setup_route()

        return 0, "", ""

    def startip_cmd_shared_ipvlan(self, mode):
        nspid = self.get_nspid()

        tmp_guest_dev = "ph%s%s" % (nspid, self.guest_dev)
        mtu = self.ip_get_mtu()

        # create a macvlan interface
        cmd = [Env.syspaths.ip, "link", "add", "link", self.ipdev, "dev", tmp_guest_dev, "mtu", mtu, "type", "ipvlan", "mode", mode]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # activate the parent dev
        cmd = [Env.syspaths.ip, "link", "set", self.ipdev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # assign the macvlan interface to the container namespace
        cmd = [Env.syspaths.ip, "link", "set", tmp_guest_dev, "netns", nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # rename the tmp guest dev
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", tmp_guest_dev, "name", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # plumb the ip
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "add", "%s/%s" % (self.addr, to_cidr(self.netmask)), "dev", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # setup default route
        self.ip_setup_route()

        return 0, "", ""

    def startip_cmd_shared_macvlan(self):
        nspid = self.get_nspid()

        tmp_guest_dev = "ph%s%s" % (nspid, self.guest_dev)
        mtu = self.ip_get_mtu()

        # create a macvlan interface
        cmd = [Env.syspaths.ip, "link", "add", "link", self.ipdev, "dev", tmp_guest_dev, "mtu", mtu, "type", "macvlan", "mode", "bridge"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # activate the parent dev
        cmd = [Env.syspaths.ip, "link", "set", self.ipdev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # assign the macvlan interface to the container namespace
        cmd = [Env.syspaths.ip, "link", "set", tmp_guest_dev, "netns", nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # rename the tmp guest dev
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", tmp_guest_dev, "name", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # set the mac addr
        self.set_macaddr()

        # plumb the ip
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "add", "%s/%s" % (self.addr, to_cidr(self.netmask)), "dev", self.final_guest_dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # setup default route
        self.ip_setup_route()

        return 0, "", ""

    def ip_get_mtu(self):
        # get mtu
        cmd = [Env.syspaths.ip, "link", "show", self.ipdev]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error("failed to get %s mtu: %s" % (self.ipdev, err))
        mtu = out.split()[4]
        return mtu

    def ip_setup_route(self):
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "set", self.final_guest_dev, "up"]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "list", "default"]
        ret, out, err = self.call(cmd, errlog=False)
        if out.startswith("default via"):
            pass
        elif out.startswith("default dev") and not self.gateway:
            pass
        elif self.gateway:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "replace", "default", "via", self.gateway]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                return ret, out, err
        else:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "replace", "default", "dev", self.final_guest_dev]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                return ret, out, err

        if self.del_net_route and self.network:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "del", self.network+"/"+to_cidr(self.netmask), "dev", self.final_guest_dev]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                return ret, out, err

        # announce
        cmd = [Env.syspaths.nsenter, "--net="+self.netns] + Env.python_cmd + [os.path.join(Env.paths.pathlib, "utilities", "arp.py"), self.final_guest_dev, self.addr]
        self.log.info(" ".join(cmd))
        out, err, ret = justcall(cmd)

    def get_nspid(self):
        if self.container.type in ("container.docker", "container.podman"):
            return self.get_nspid_docker()
        elif self.container.type in ("container.lxd", "container.lxc"):
            return self.get_nspid_lxc()

    def get_nspid_lxc(self):
        return str(self.container.get_pid())

    def get_nspid_docker(self):
        nspid = self.container.container_pid()
        if nspid is None:
            raise ex.Error("failed to get nspid")
        nspid = str(nspid).strip()
        if "'" in nspid:
            nspid = nspid.replace("'","")
        if nspid == "0":
            raise ex.Error("nspid is 0")
        return nspid

    @lazy
    def netns(self):
        if self.container.type in ("container.docker", "container.podman"):
            path = self.sandboxkey()
            if os.path.exists(path):
                return path
            # compat with older netns location
            path = path.replace("/services/", "/svc/")
            if os.path.exists(path):
                return path
            return
        elif self.container.type in ("container.lxd", "container.lxc"):
            return self.container.cni_netns()
        raise ex.Error("unsupported container type: %s" % self.container.type)

    def sandboxkey(self):
        sandboxkey = self.container.container_sandboxkey()
        if sandboxkey is None:
            raise ex.Error("failed to get sandboxkey")
        sandboxkey = str(sandboxkey).strip()
        if "'" in sandboxkey:
            sandboxkey = sandboxkey.replace("'","")
        if sandboxkey == "":
            raise ex.Error("sandboxkey is empty")
        return sandboxkey

    def stopip_cmd(self):
        intf = self.get_docker_interface()
        if intf is None:
            raise ex.ContinueAction("can't find on which interface %s is plumbed in container %s" % (self.addr, self.container_id()))
        if self.netmask is None:
            raise ex.ContinueAction("netmask is not set")
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "addr", "del", self.addr+"/"+to_cidr(self.netmask), "dev", intf]
        ret, out, err = self.vcall(cmd)
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "link", "del", "dev", intf]
        ret, out, err = self.vcall(cmd)

        if self.mode == "ovs":
            self.log.info("ovs mode")
            ret, out, err = self.stopip_cmd_shared_ovs()

        self.unset_lazy("netns")
        return ret, out, err

