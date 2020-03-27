"""
A CNI driver following the specs available at
https://github.com/containernetworking/cni/blob/master/SPEC.md
"""
import os
import json

from subprocess import Popen, PIPE

import core.exceptions as ex
import core.status
import utilities.ifconfig

from .. import \
    KW_WAIT_DNS, \
    KW_DNS_NAME_SUFFIX, \
    KW_PROVISIONER, \
    KW_DNS_UPDATE, \
    KW_CHECK_CARRIER, \
    KW_ALIAS, \
    KW_EXPOSE
from ..linux import Ip
from rcGlobalEnv import rcEnv
from core.objects.builder import init_kwargs
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall, which
from utilities.net.converters import to_cidr
from utilities.render.color import format_str_flat_json
from utilities.string import bencode, bdecode

CNI_VERSION = "0.3.0"
PORTMAP_CONF = {
    "cniVersion": CNI_VERSION,
    "name": "osvc-portmap",
    "type": "portmap",
    "snat": True,
    "capabilities": {
        "portMappings": True
    },
#    "externalSetMarkChain": "OSVC-MARK-MASQ"
}

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "cni"
KEYWORDS = [
    {
        "keyword": "ipname",
        "required": False,
        "at": True,
        "text": "Not used by the cni driver."
    },
    {
        "keyword": "network",
        "at": True,
        "required": False,
        "default": "default",
        "text": "The name of the CNI network to plug into. The default network is created using the host-local bridge plugin if no existing configuration already exists.",
        "example": "my-weave-net",
    },
    {
        "keyword": "netns",
        "at": True,
        "required": False,
        "text": "The resource id of the container to plug into the CNI network.",
        "example": "container#0"
    },
    {
        "keyword": "ipdev",
        "default": "eth12",
        "at": True,
        "required": False,
        "text": "The interface name in the container namespace."
    },
    KW_WAIT_DNS,
    KW_DNS_NAME_SUFFIX,
    KW_PROVISIONER,
    KW_DNS_UPDATE,
    KW_CHECK_CARRIER,
    KW_ALIAS,
    KW_EXPOSE,
]
DEPRECATED_KEYWORDS = {
    "ip.cni.container_rid": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "ip.cni.netns": "container_rid",
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
)

def adder(svc, s):
    """
    Add a resource instance to the object, parsing parameters
    from a configuration section dictionnary.
    """
    kwargs = init_kwargs(svc, s)
    kwargs["expose"] = svc.oget(s, "expose")
    kwargs["check_carrier"] = svc.oget(s, "check_carrier")
    kwargs["alias"] = svc.oget(s, "alias")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    kwargs["network"] = svc.oget(s, "network")
    kwargs["netns"] = svc.oget(s, "netns")
    r = IpCni(**kwargs)
    svc += r


class IpCni(Ip):
    def __init__(self,
                 network=None,
                 netns=None,
                 **kwargs):
        super(IpCni, self).__init__(type="ip.cni", **kwargs)
        self.network = network
        self.container_rid = netns
        if self.container_rid:
            self.tags = self.tags | set(["docker"])
            self.tags.add(self.container_rid)

    def set_label(self):
        pass

    @lazy
    def label(self): # pylint: disable=method-hidden
        intf = self.get_ipdev()
        label = "cni "
        label += self.network if self.network else ""
        if intf and len(intf.ipaddr) > 0:
            label += " %s/%s" % (intf.ipaddr[0], to_cidr(intf.mask[0]))
        elif intf and len(intf.ip6addr) > 0:
            label += " %s/%s" % (intf.ip6addr[0], intf.ip6mask[0])
        if self.ipdev:
            label += " %s" % self.ipdev
        if self.expose:
            label += " %s" % " ".join(self.expose)
        return label


    def _status_info(self):
        data = super(IpCni, self)._status_info()
        intf = self.get_ipdev()
        if intf and len(intf.ipaddr) > 0:
            data["ipaddr"] = intf.ipaddr[0]
        elif intf and len(intf.ip6addr) > 0:
            data["ipaddr"] = intf.ip6addr[0]
        if self.container:
            if self.container.vm_hostname != self.container.name:
                data["hostname"] = self.container.vm_hostname
            else:
                data["hostname"] = self.container.name
            if self.dns_name_suffix:
                data["hostname"] += self.dns_name_suffix
        return data

    def arp_announce(self):
        """ disable the generic arping. We do that in the guest namespace.
        """
        pass

    def get_ifconfig(self):
        if self.container_rid:
            return self.container_get_ifconfig()
        else:
            return self._get_ifconfig()

    def container_get_ifconfig(self):
        if self.netns is None:
            return

        cmd = [rcEnv.syspaths.nsenter, "--net="+self.netns, "ip", "addr"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ifconfig = utilities.ifconfig.Ifconfig(ip_out=out)
        return ifconfig

    def _get_ifconfig(self):
        try:
            nspid = self.nspid
        except ex.Error as e:
            return
        if nspid is None:
            return

        cmd = [rcEnv.syspaths.ip, "netns", "exec", self.nspid, "ip", "addr"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ifconfig = utilities.ifconfig.Ifconfig(ip_out=out)
        return ifconfig

    def get_ipdev(self):
        ifconfig = self.get_ifconfig()
        if ifconfig is None:
            return
        return ifconfig.interface(self.ipdev)

    def has_ipdev(self):
        ifconfig = self.get_ifconfig()
        if ifconfig is None:
            return False
        if ifconfig.has_interface(self.ipdev) == 0:
            return False
        return True

    def has_ip(self):
        ifconfig = self.get_ifconfig()
        if ifconfig is None:
            return False
        iface = ifconfig.interface(self.ipdev)
        if iface is None:
            return False
        if len(iface.ipaddr) == 0:
            return False
        return True

    def abort_start(self):
        return False

    @lazy
    def cni_data(self):
        self.svc.node.network_create_config(self.network)
        try:
            with open(self.cni_conf, "r") as ofile:
                return json.load(ofile)
        except ValueError:
            raise ex.Error("invalid json in cni configuration file %s" % self.cni_conf)

    @lazy
    def cni_plugins(self):
        path = self.svc.node.oget("cni", "plugins")
        if os.path.exists(os.path.join(path, "bridge")):
            return path
        altpath = os.path.join(os.sep, "usr", "libexec", "cni")
        if os.path.exists(os.path.join(altpath, "bridge")):
            return altpath
        return path

    @lazy
    def cni_config(self):
        return self.svc.node.oget("cni", "config")

    @lazy
    def cni_portmap_conf(self):
        return os.path.join(self.cni_config, "osvc-portmap.conf")

    @lazy
    def cni_conf(self):
        return os.path.join(self.cni_config, "%s.conf" % self.network)

    def cni_bin(self, data):
        return os.path.join(self.cni_plugins, data["type"])

    @lazy
    def nspid(self):
        return self.svc.id

    @lazy
    def nspidfile(self):
        return "/var/run/netns/%s" % self.nspid

    @lazy
    def container(self):
        return self.svc.resources_by_id.get(self.container_rid)

    @lazy
    def netns(self):
        if self.container is None:
            return
        return self.container.cni_netns()

    @lazy
    def containerid(self):
        return self.container.cni_containerid()

    def allow_start(self):
        pass

    def start_locked(self):
        self.startip_cmd()
        return False

    def cni_cmd(self, _env, data):
        cmd = [self.cni_bin(data)]
        if not which(cmd[0]):
            raise ex.Error("%s not found" % cmd[0])
        self.log_cmd(_env, data, cmd)
        env = {}
        env.update(rcEnv.initial_env)
        env.update(_env)
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE, env=env)
        out, err = proc.communicate(input=bencode(json.dumps(data)))
        out = bdecode(out)
        try:
            data = json.loads(out)
        except ValueError:
            if proc.returncode == 0:
                # for example a del portmap outs nothing
                return
            raise ex.Error(err)
        if "code" in data:
            raise ex.Error(data.get("msg", ""))
        for line in format_str_flat_json(data).splitlines():
            self.log.info(line)
        return data

    def log_cmd(self, _env, data, cmd):
        envs = " ".join(map(lambda x: "%s=%s" % (x[0], x[1]), _env.items()))
        text = "echo '%s' | %s %s" % (json.dumps(data), envs, " ".join(cmd))
        self.log.info(text)

    def has_netns(self):
        if self.container_rid:
            return True
        return os.path.exists(self.nspidfile)

    def add_netns(self):
        if self.container_rid:
            # the container is expected to already have a netns
            return
        if self.has_netns():
            self.log.info("netns %s already added" % self.nspid)
            return
        cmd = [rcEnv.syspaths.ip, "netns", "add", self.nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error(err)

    def del_netns(self):
        if self.container_rid:
            # the container is expected janitor its netns himself
            return
        if not self.has_netns():
            self.log.info("netns %s already deleted" % self.nspid)
            return
        cmd = [rcEnv.syspaths.ip, "netns", "del", self.nspid]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def get_plugins(self):
        if "type" in self.cni_data:
            return [self.cni_data]
        elif "plugins" in self.cni_data:
            return [pdata for pdata in self.cni_data["plugins"]
                    if pdata.get("type") != "portmap"]
        raise ex.Error("no type nor plugins in cni configuration %s" % self.cni_conf)

    def runtime_config(self):
        data = []
        expose_data = self.cni_expose_data()
        for expose in expose_data:
            if "hostPort" not in expose:
                continue
            data.append(expose)
        if len(data) > 0:
            return {"portMappings": data}
        return {}

    def cni_expose_data(self):
        """
        Translate opensvc expose data in the format expected by cni.
        """
        data = self.expose_data()
        _data = []
        for expose in data:
            if "port" not in expose or not expose["port"]:
                continue
            if "protocol" not in expose or not expose["port"]:
                continue
            if "host_port" not in expose or not expose["host_port"]:
                expose["host_port"] = expose["port"]
            exdata = {
                "containerPort": expose["port"],
                "protocol": expose["protocol"],
                "hostPort": expose["host_port"],
            }
            _data.append(exdata)
        return _data

    def add_cni(self):
        if not self.has_netns():
            raise ex.Error("netns %s not found" % self.nspid)
        if self.has_ipdev():
            self.log.info("cni %s already added" % self.ipdev)
            return
        _env = {
            "CNI_COMMAND": "ADD",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": self.cni_plugins,
        }
        if self.container_rid:
            if self.containerid is None:
                raise ex.Error("container %s is down" % self.container_rid)
            _env["CNI_CONTAINERID"] = str(self.containerid)
            _env["CNI_NETNS"] = self.netns
        else:
            _env["CNI_CONTAINERID"] = self.svc.id
            _env["CNI_NETNS"] = self.nspidfile

        ret = 0
        result = None
        for data in self.get_plugins():
            data["cniVersion"] = self.cni_data["cniVersion"]
            data["name"] = self.cni_data["name"]
            if result is not None:
                data["prevResult"] = result
            result = self.cni_cmd(_env, data)

        if self.expose and result:
            data = {}
            data.update(PORTMAP_CONF)
            data["prevResult"] = result
            data["runtimeConfig"] = self.runtime_config()
            if data["runtimeConfig"]:
                result = self.cni_cmd(_env, data)

    def del_cni(self):
        if not self.has_netns():
            self.log.info("already no ip dev %s" % self.ipdev)
            return
        if not self.has_ip():
            self.log.info("already no ip on dev %s" % self.ipdev)
            return
        intf = self.get_ipdev()
        if intf and len(intf.ipaddr) > 0:
            ipaddr = intf.ipaddr[0]
        else:
            ipaddr = None
        _env = {
            "CNI_COMMAND": "DEL",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": self.cni_plugins,
        }
        if self.container_rid:
            if self.containerid is None:
                self.log.info("container %s is already down" % self.container_rid)
                return
            _env["CNI_CONTAINERID"] = str(self.containerid)
            _env["CNI_NETNS"] = self.netns
        else:
            _env["CNI_CONTAINERID"] = str(self.nspid)
            _env["CNI_NETNS"] = self.nspidfile

        if self.expose:
            data = {}
            data.update(PORTMAP_CONF)
            data["runtimeConfig"] = self.runtime_config()
            if data["runtimeConfig"]:
                result = self.cni_cmd(_env, data)

        for data in reversed(self.get_plugins()):
            data["cniVersion"] = self.cni_data["cniVersion"]
            data["name"] = self.cni_data["name"]
            result = self.cni_cmd(_env, data)

        var_f = "/var/lib/cni/networks/%s/%s" % (self.network, ipaddr)
        if os.path.exists(var_f):
            self.log.info("rm %s", var_f)
            os.unlink(var_f)

    def start(self):
        self.unset_lazy("containerid")
        self.unset_lazy("netns")
        self.add_netns()
        self.add_cni()
        self.wait_dns_records()

    def stop(self):
        self.del_cni()
        self.del_netns()
        self.unset_lazy("containerid")
        self.unset_lazy("netns")

    def _status(self, verbose=False):
        try:
            self.cni_data
        except ex.Error as exc:
            self.status_log(str(exc))
        _has_netns = self.has_netns()
        _has_ipdev = self.has_ipdev()
        if self.container_rid and not _has_ipdev:
            return core.status.DOWN
        elif not _has_netns and not _has_ipdev:
            return core.status.DOWN
        elif _has_netns and _has_ipdev:
            return core.status.UP
        else:
            if not _has_netns:
                self.status_log("netns %s not found" % self.nspid)
            if not _has_ipdev:
                self.status_log("cni %s not found" % self.ipdev)
            return core.status.DOWN

    def is_up(self):
        if not self.has_netns():
            return False
        if not self.has_ipdev():
            return False
        return True

    def provisioned(self):
        return True

