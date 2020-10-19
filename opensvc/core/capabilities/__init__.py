from __future__ import print_function

import importlib
import json
import sys

from env import Env
from utilities.proc import which
from utilities.lazy import lazy
from utilities.subsystems.docker import has_docker

DRIVER_CAP_FN = "driver_capabilities"
DRIVER_CAP_PREFIX = "drivers.resource."

class BaseCapabilities(object):
    def __contains__(self, cap):
        return cap in self.data

    def scan_generic(self):
        data = []
        if which("stat"):
            data.append("node.x.stat")
        if has_docker(["docker"]):
            data.append("node.x.docker")
        if has_docker(["docker.io"]):
            data.append("node.x.docker.io")
        if has_docker(["dockerd"]):
            data.append("node.x.dockerd")
        if which("exportfs"):
            data.append("node.x.exportfs")
        if which("findfs"):
            data.append("node.x.findfs")
        if which("/usr/bin/podman"):
            data.append("node.x.podman")
        if which("/sbin/ip"):
            data.append("node.x.ip")
        if which("netstat"):
            data.append("node.x.netstat")
        if which("ifconfig"):
            data.append("node.x.ifconfig")
        if which("powermt"):
            data.append("node.x.powermt")
        if which("vxdmpadm"):
            data.append("node.x.vxdmpadm")
        if which("udevadm"):
            data.append("node.x.udevadm")
        if which("drbdadm"):
            data.append("node.x.drbdadm")
        if which("vmware-cmd"):
            data.append("node.x.vmware-cmd")
        if which("zfs"):
            data.append("node.x.zfs")
        if which("zpool"):
            data.append("node.x.zpool")
        if which("git"):
            data.append("node.x.git")
        if which("share"):
            data.append("node.x.share")
        if which("srp"):
            data.append("node.x.srp")
        if which("srp_su"):
            data.append("node.x.srp_su")
        if which("/opt/hpvm/bin/hpvmstatus"):
            data.append("node.x.hpvmstatus")
        if which("/opt/hpvm/bin/hpvmstart"):
            data.append("node.x.hpvmstart")
        if which("/opt/hpvm/bin/hpvmstop"):
            data.append("node.x.hpvmstop")
        if which(Env.syspaths.blkid):
            data.append("node.x.blkid")
        if which(Env.syspaths.losetup):
            data.append("node.x.losetup")
        if which(Env.syspaths.multipath):
            data.append("node.x.multipath")
        if which(Env.syspaths.dmsetup):
            data.append("node.x.dmsetup")
        return data
    
    def need_refresh(self):
        return False

    def scan_os(self):
        return []

    def scan(self, node=None):
        from core.objects.svcdict import SECTIONS
        from utilities.drivers import iter_drivers
        if node is None:
            from core.node import Node
            node = Node()
        data = self.scan_generic()
        data += self.scan_os()
        for mod in iter_drivers(SECTIONS):
            if not hasattr(mod, DRIVER_CAP_FN):
                if hasattr(mod, "DRIVER_GROUP") and hasattr(mod, "DRIVER_BASENAME"):
                    # consider the driver active by default
                    data += ["%s%s.%s" % (DRIVER_CAP_PREFIX, mod.DRIVER_GROUP, mod.DRIVER_BASENAME)]
                continue
            try:
                data += [DRIVER_CAP_PREFIX + cap for cap in getattr(mod, DRIVER_CAP_FN)(node=node)]
            except Exception as exc:
                print(mod, exc, file=sys.stderr)
        data = sorted([cap for cap in set(data)])
        with open(Env.paths.capabilities, "w") as f:
            json.dump(data, f)
        return data
    
    @lazy
    def data(self):
        if self.need_refresh():
            return self.scan()
        try:
            with open(Env.paths.capabilities, "r") as f:
                return json.load(f)
        except Exception:
            return self.scan()
    
    def has(self, cap):
        return cap in self.data


try:
    _package = __package__ or __spec__.name # pylint: disable=undefined-variable
    _os = importlib.import_module("." + Env.module_sysname, package=_package)
    capabilities = _os.Capabilities()
except (ImportError, AttributeError):
    capabilities = BaseCapabilities()


