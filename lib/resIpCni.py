"""
A CNI driver following the specs available at
https://github.com/containernetworking/cni/blob/master/SPEC.md
"""
import os
import hashlib
import json
from subprocess import Popen, PIPE

import resIpLinux as Res
import rcExceptions as ex
import rcIfconfigLinux as rcIfconfig
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which, justcall, to_cidr, lazy, bencode, bdecode

CNI_VERSION = "0.2.0"
PORTMAP_CONF = {
    "cniVersion": CNI_VERSION,
    "name": "osvc-portmap",
    "type": "portmap",
    "snat": True,
    "capabilities": {
        "portMappings": True
    },
    "externalSetMarkChain": "OSVC-MARK-MASQ"
}

class Ip(Res.Ip):
    def __init__(self,
                 rid=None,
                 ipname=None,
                 ipdev=None,
                 network=None,
                 container_rid=None,
                 expose=None,
                 **kwargs):
        Res.Ip.__init__(self,
                        rid,
                        ipname=ipname,
                        ipdev=ipdev,
                        type="ip.cni",
                        expose=expose,
                        **kwargs)
        self.network = network
        self.container_rid = container_rid
        if container_rid:
            self.tags = self.tags | set(["docker"])
            self.tags.add(container_rid)

    def set_label(self):
        pass

    @lazy
    def label(self):
        intf = self.get_ipdev()
        label = self.network if self.network else ""
        if intf and len(intf.ipaddr) > 0:
            label += " %s/%s" % (intf.ipaddr[0], intf.mask[0])
        elif intf and len(intf.ip6addr) > 0:
            label += " %s/%s" % (intf.ip6addr[0], intf.ip6mask[0])
        if self.ipdev:
            label += "@%s" % self.ipdev
        if self.expose:
            label += " %s" % " ".join(self.expose)
        return label

    def status_info(self):
        data = Res.Ip.status_info(self)
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
        sandboxkey = self.container_sandboxkey
        if sandboxkey is None:
            return

        cmd = [rcEnv.syspaths.nsenter, "--net="+self.container_sandboxkey, "ip", "addr"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ifconfig = rcIfconfig.ifconfig(ip_out=out)
        return ifconfig

    def _get_ifconfig(self):
        try:
            nspid = self.nspid
        except ex.excError as e:
            return
        if nspid is None:
            return

        cmd = [rcEnv.syspaths.ip, "netns", "exec", self.nspid, "ip", "addr"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        ifconfig = rcIfconfig.ifconfig(ip_out=out)
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
        if not os.path.exists(self.cni_conf):
            raise ex.excError("cni configuration %s does not exist" % self.cni_conf)
        try:
            with open(self.cni_conf, "r") as ofile:
                return json.load(ofile)
        except ValueError:
            raise ex.excError("invalid json in cni configuration file %s" % self.cni_conf)

    @lazy
    def cni_plugins(self):
        try:
            return self.svc.node.conf_get("cni", "plugins")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def cni_config(self):
        try:
            return self.svc.node.conf_get("cni", "config")
        except ex.OptNotFound as exc:
            return exc.default

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
        nspid = hashlib.sha224(self.svc.svcname).hexdigest()
        return nspid

    @lazy
    def nspidfile(self):
        return "/var/run/netns/%s" % self.nspid

    @lazy
    def container(self):
        return self.svc.resources_by_id.get(self.container_rid)

    @lazy
    def container_sandboxkey(self):
        try:
            data = self.svc.dockerlib.docker_inspect(self.container.container_id)
            return data["NetworkSettings"]["SandboxKey"]
        except (IndexError, KeyError):
            return

    @lazy
    def container_pid(self):
        try:
            data = self.svc.dockerlib.docker_inspect(self.container.container_id)
            return data["State"]["Pid"]
        except (IndexError, KeyError):
            return

    def allow_start(self):
        pass

    def start_locked(self):
        self.startip_cmd()
        return False

    def cni_cmd(self, _env, data):
        cmd = [self.cni_bin(data)]
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
            raise ex.excError(err)
        if "code" in data:
            raise ex.excError(data.get("msg", ""))
        for line in out.splitlines():
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
            raise ex.excError(err)

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
            raise ex.excError()

    def get_plugins(self):
        if "type" in self.cni_data:
            return [self.cni_data]
        elif "plugins" in self.cni_data:
            return [pdata for pdata in self.cni_data["plugins"]
                    if pdata.get("type") != "portmap"]
        raise ex.excError("no type nor plugins in cni configuration %s" % self.cni_conf)

    def runtime_config(self):
        data = []
        expose_data = self.cni_expose_data()
        for expose in expose_data:
            if "host_port" not in expose:
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
            if "host_port" not in expose:
                continue
            exdata = {
                "containerPort": expose["port"],
                "protocol": expose["protocol"],
                "hostPort": expose["host_port"],
            }
            _data.append(exdata)
        return data

    def add_cni(self):
        if not self.has_netns():
            raise ex.excError("netns %s not found" % self.nspid)
        if self.has_ipdev():
            self.log.info("cni %s already added" % self.ipdev)
            return
        _env = {
            "CNI_COMMAND": "ADD",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": self.cni_plugins,
        }
        if self.container_rid:
            if self.container_pid is None:
                raise ex.excError("container %s is down" % self.container_rid)
            _env["CNI_CONTAINERID"] = str(self.container_pid)
            _env["CNI_NETNS"] = self.container_sandboxkey
        else:
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
            data["runtimeConfig"] = self.runtime_config()
            data["prevResult"] = result
            result = self.cni_cmd(_env, data)

    def del_cni(self):
        if not self.has_netns():
            self.log.info("already no ip dev %s" % self.ipdev)
            return
        if not self.has_ip():
            self.log.info("already no ip on dev %s" % self.ipdev)
            return
        _env = {
            "CNI_COMMAND": "DEL",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": self.cni_plugins,
        }
        if self.container_rid:
            if self.container_pid is None:
                self.log.info("container %s is already down" % self.container_rid)
                return
            _env["CNI_CONTAINERID"] = str(self.container_pid)
            _env["CNI_NETNS"] = self.container_sandboxkey
        else:
            _env["CNI_NETNS"] = self.nspidfile

        if self.expose:
            data = {}
            data.update(PORTMAP_CONF)
            data["runtimeConfig"] = self.runtime_config()
            result = self.cni_cmd(_env, data)
        for data in reversed(self.get_plugins()):
            data["cniVersion"] = self.cni_data["cniVersion"]
            data["name"] = self.cni_data["name"]
            result = self.cni_cmd(_env, data)

    def start(self):
        self.unset_lazy("container_pid")
        self.unset_lazy("container_sandboxkey")
        self.add_netns()
        self.add_cni()

    def stop(self):
        self.del_cni()
        self.del_netns()
        self.unset_lazy("container_pid")
        self.unset_lazy("container_sandboxkey")

    def _status(self, verbose=False):
        try:
            self.cni_data
        except ex.excError as exc:
            self.status_log(str(exc))
        _has_netns = self.has_netns()
        _has_ipdev = self.has_ipdev()
        if self.container_rid and not _has_ipdev:
            return rcStatus.DOWN
        elif not _has_netns and not _has_ipdev:
            return rcStatus.DOWN
        elif _has_netns and _has_ipdev:
            return rcStatus.UP
        else:
            if not _has_netns:
                self.status_log("netns %s not found" % self.nspid)
            if not _has_ipdev:
                self.status_log("cni %s not found" % self.ipdev)
            return rcStatus.DOWN

    def is_up(self):
        if not self.has_netns():
            return False
        if not self.has_ipdev():
            return False
        return True

