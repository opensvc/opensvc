from __future__ import print_function

import importlib
import json
import sys

from env import Env
from utilities.proc import which
from utilities.lazy import lazy

DRIVER_CAP_FN = "driver_capabilities"
DRIVER_CAP_PREFIX = "drivers.resource."

class BaseCapabilities(object):
    def __contains__(self, cap):
        return cap in self.data

    def scan_generic(self):
        data = []
        if which("stat"):
            data.append("node.x.stat")
        if which("docker"):
            data.append("node.x.docker")
        if which("docker.io"):
            data.append("node.x.docker.io")
        if which("dockerd"):
            data.append("node.x.dockerd")
        if which("/sbin/podman"):
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
        if which("zpool"):
            data.append("node.x.zpool")
        if which("git"):
            data.append("node.x.git")
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

    def scan(self):
        from core.objects.svcdict import SECTIONS
        from utilities.drivers import iter_drivers
        data = self.scan_generic()
        data += self.scan_os()
        for mod in iter_drivers(SECTIONS):
            if not hasattr(mod, DRIVER_CAP_FN):
                continue
            try:
                data += [DRIVER_CAP_PREFIX + cap for cap in getattr(mod, DRIVER_CAP_FN)()]
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


