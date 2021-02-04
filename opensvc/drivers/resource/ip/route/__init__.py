import os

import core.exceptions as ex
import core.status
import utilities.ifconfig

from env import Env
from core.objects.svcdict import KEYS
from core.resource import Resource
from utilities.lazy import lazy
from utilities.proc import justcall, which

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "route"
KEYWORDS = [
    {
        "keyword": "netns",
        "at": True,
        "required": True,
        "text": "The resource id of the container to plumb the ip into.",
        "example": "container#0"
    },
    {
        "keyword": "spec",
        "at": True,
        "required": True,
        "convert": "shlex",
        "text": "The route specification, passed to the ip route commands, with the appropriate network namespace set.",
    },
]
KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if Env.sysname == "Linux" and which("ip"):
        return ["ip.route"]
    return []

class IpRoute(Resource):
    def __init__(self,
                 netns=None,
                 spec=None,
                 **kwargs):
        super(IpRoute, self).__init__(type="ip.route", **kwargs)
        self.container_rid = str(netns)
        self.spec = spec
        self.tags = self.tags | set(["docker"])
        self.tags.add(self.container_rid)
        self.label = " ".join(spec)

    def on_add(self):
        self.svc.register_dependency("start", self.rid, self.container_rid)
        self.svc.register_dependency("start", self.container_rid, self.rid)
        self.svc.register_dependency("stop", self.container_rid, self.rid)

    def start_route(self):
        if self.netns is None:
            raise ex.Error("unable to find the network namespace")
        try:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "replace"] + self.spec
            ret, out, err = self.vcall(cmd)
        except ex.Error:
            pass
        if ret != 0:
            raise ex.Error

    def stop_route(self):
        if self.netns is None:
            self.log.info("skip: unable to find the network namespace")
            return
        try:
            cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "del"] + self.spec
            ret, out, err = self.vcall(cmd)
        except ex.Error:
            pass
        if ret != 0:
            raise ex.Error

    def is_up(self):
        try:
            self.netns
        except ex.Error:
            return False
        if self.netns is None:
            return False
        cmd = [Env.syspaths.nsenter, "--net="+self.netns, "ip", "route", "list"] + self.spec
        out, err, ret = justcall(cmd)
        return ret == 0 and out.strip() != ""

    def _status(self, verbose=False):
        self.unset_lazy("netns")
        if self.is_up():
            return core.status.UP
        return core.status.DOWN

    def start(self):
        if self.is_up():
            self.log.info("route is already up")
            return
        self.start_route()

    def stop(self):
        if not self.is_up():
            self.log.info("route is already down")
            return
        self.stop_route()

    @lazy
    def container(self):
        if self.container_rid not in self.svc.resources_by_id:
            raise ex.Error("rid %s not found" % self.container_rid)
        return self.svc.resources_by_id[self.container_rid]

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

