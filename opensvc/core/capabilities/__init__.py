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
        return cap in self.data["tags"]

    def scan_generic(self):
        tags = [
            "node.x.cache.name",
            "node.x.cache.ttl",
        ]
        labels = {}

        for tag, bp in (
            ("node.x.blkid", Env.syspaths.blkid),
            ("node.x.dmsetup", Env.syspaths.dmsetup),
            ("node.x.drbdadm", "drbdadm"),
            ("node.x.exportfs", "exportfs"),
            ("node.x.findfs", "findfs"),
            ("node.x.git", "git"),
            ("node.x.hpvmstart", "/opt/hpvm/bin/hpvmstart"),
            ("node.x.hpvmstatus", "/opt/hpvm/bin/hpvmstatus"),
            ("node.x.hpvmstop", "/opt/hpvm/bin/hpvmstop"),
            ("node.x.ifconfig", "ifconfig"),
            ("node.x.ip", "/sbin/ip"),
            ("node.x.losetup", Env.syspaths.losetup),
            ("node.x.lvs", "/sbin/lvs"),
            ("node.x.multipath", Env.syspaths.multipath),
            ("node.x.netstat", "netstat"),
            ("node.x.podman", "/usr/bin/podman"),
            ("node.x.powermt", "powermt"),
            ("node.x.scsi_id", ("scsi_id", "/lib/udev/scsi_id", "/usr/lib/udev/scsi_id")),
            ("node.x.share", "share"),
            ("node.x.srp", "srp"),
            ("node.x.srp_su", "srp_su"),
            ("node.x.stat", "stat"),
            ("node.x.udevadm", "udevadm"),
            ("node.x.vmware-cmd", "vmware-cmd"),
            ("node.x.vxdmpadm", "vxdmpadm"),
            ("node.x.zfs", "zfs"),
            ("node.x.zpool", "zpool"),
        ):
            if not isinstance(bp, tuple):
                bp = (bp,)
            for bpc in bp:
                p = which(bpc)
                if not p:
                    continue
                tags.append(tag)
                labels[tag+".path"] = p
                break

        if has_docker(["docker"]):
            tags.append("node.x.docker")
        if has_docker(["docker.io"]):
            tags.append("node.x.docker.io")
        if has_docker(["dockerd"]):
            tags.append("node.x.dockerd")

        if "node.x.stat" in tags:
            tags.append("drivers.resource.fs.check_readable")

        return {"tags": tags, "labels": labels}
    
    def need_refresh(self):
        return False

    def scan_os(self):
        return {"tags": [], "labels": {}}

    def scan(self, node=None):
        from core.objects.svcdict import SECTIONS
        from utilities.drivers import iter_drivers
        if node is None:
            from core.node import Node
            node = Node()
        data = self.scan_generic()
        osdata = self.scan_os()
        data["tags"] += osdata["tags"]
        data["labels"].update(osdata["labels"])
        for mod in iter_drivers(SECTIONS):
            if not hasattr(mod, DRIVER_CAP_FN):
                if hasattr(mod, "DRIVER_GROUP") and hasattr(mod, "DRIVER_BASENAME"):
                    # consider the driver active by default
                    data["tags"] += ["%s%s.%s" % (DRIVER_CAP_PREFIX, mod.DRIVER_GROUP, mod.DRIVER_BASENAME)]
                continue
            try:
                for cap in getattr(mod, DRIVER_CAP_FN)(node=node):
                    if isinstance(cap, tuple):
                        cap, val = cap
                        pcap = DRIVER_CAP_PREFIX + cap
                        data["labels"][pcap] = val
                    else:
                        pcap = DRIVER_CAP_PREFIX + cap
                        data["tags"].append(pcap)
            except Exception as exc:
                print(mod, exc, file=sys.stderr)
        self.dump(data)
        return data

    @staticmethod
    def as_list(data):
        l = [] + data["tags"]
        for k, v in data["labels"].items():
            l.append("%s=%s" % (k, v))
        return sorted(l)

    def dump(self, data):
        data = self.as_list(data)
        with open(Env.paths.capabilities, "w") as f:
            json.dump(data, f)
    
    def load(self):
        with open(Env.paths.capabilities, "r") as f:
            l = json.load(f)
        data = {"tags": [], "labels": {}}
        for s in l:
            try:
                label, val = s.split("=", 1)
                data["labels"][label] = val
            except ValueError:
                data["tags"].append(s)
        return data

    @lazy
    def data(self):
        if self.need_refresh():
            return self.scan()
        try:
            return self.load()
        except Exception as exc:
            return self.scan()
    
    def has(self, cap):
        return cap in self.data["tags"]

    def get(self, cap):
        return self.data["labels"].get(cap)

try:
    _package = __package__ or __spec__.name # pylint: disable=undefined-variable
    _os = importlib.import_module("." + Env.module_sysname, package=_package)
    capabilities = _os.Capabilities()
except (ImportError, AttributeError):
    capabilities = BaseCapabilities()


