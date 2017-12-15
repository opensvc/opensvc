import os
import hashlib
import json
from subprocess import Popen, PIPE

import resIpLinux as Res
import rcExceptions as ex
import rcIfconfigLinux as rcIfconfig
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which, justcall, to_cidr, lazy

class Ip(Res.Ip):
    def __init__(self,
                 rid=None,
                 ipname=None,
                 ipdev=None,
                 network=None,
                 container_rid=None,
                 **kwargs):
        Res.Ip.__init__(self,
                        rid,
                        ipname=ipname,
                        ipdev=ipdev,
                        type="ip.cni",
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
        return label

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

    def abort_start(self):
        return False

    @lazy
    def cni_data(self):
        with open(self.cni_conf, "r") as ofile:
            return json.load(ofile)

    @lazy
    def cni_conf(self):
        return "/opt/cni/net.d/%s.conf" % self.network

    @lazy
    def cni_bin(self):
        cni_driver = self.cni_data["type"]
        return "/opt/cni/bin/%s" % cni_driver

    @lazy
    def cni_ipam_bin(self):
        cni_driver = self.cni_data["ipam"]["type"]
        return "/opt/cni/bin/%s" % cni_driver

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

    def cni_cmd(self, _env, cmd):
        self.log_cmd(_env, cmd)
        env = {}
        env.update(rcEnv.initial_env)
        env.update(_env)
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE, env=env)
        with open(self.cni_conf, "r") as ofile:
            buff = ofile.read()
        out, err = proc.communicate(input=buff)
        try:
            data = json.loads(out)
        except ValueError:
            if proc.returncode == 0:
                return 0, out, err
            else:
                raise ex.excError(err)
        if "code" in data:
            self.log.error(data.get("msg", ""))
            return 1, "", data.get("msg", "")
        self.log.info(out)
        return 0, out, err

    def log_cmd(self, _env, cmd):
        text = " ".join(map(lambda x: "%s=%s" % (x[0], x[1]), _env.items()))
        text += " %s <%s" % (" ".join(cmd), self.cni_conf)
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

    def add_cni(self):
        if not self.has_netns():
            raise ex.excError("netns %s not found" % self.nspid)
        if self.has_ipdev():
            self.log.info("cni %s already added" % self.ipdev)
            return
        _env = {
            "CNI_COMMAND": "ADD",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": "/opt/cni/bin",
        }
        if self.container_rid:
            if self.container_pid is None:
                raise ex.excError("container %s is down" % self.container_rid)
            _env["CNI_CONTAINERID"] = str(self.container_pid)
            _env["CNI_NETNS"] = self.container_sandboxkey
        else:
            _env["CNI_NETNS"] = self.nspidfile

        cmd = [self.cni_bin]
        return self.cni_cmd(_env, cmd)

    def del_cni(self):
        if not self.has_netns():
            return
        _env = {
            "CNI_COMMAND": "DEL",
            "CNI_IFNAME": self.ipdev,
            "CNI_PATH": "/opt/cni/bin",
        }
        if self.container_rid:
            if self.container_pid is None:
                self.log.info("container %s is already down" % self.container_rid)
                return
            _env["CNI_CONTAINERID"] = str(self.container_pid)
            _env["CNI_NETNS"] = self.container_sandboxkey
        else:
            _env["CNI_NETNS"] = self.nspidfile

        cmd = [self.cni_bin]
        return self.cni_cmd(_env, cmd)

    def start(self):
        self.add_netns()
        self.add_cni()

    def stop(self):
        self.del_cni()
        self.del_netns()

    def _status(self, verbose=False):
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

