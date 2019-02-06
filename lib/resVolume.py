"""
Volume resource driver module.
"""

import resources as Res
import rcExceptions as ex
import rcStatus
from rcUtilities import lazy
from svc import Svc

class Volume(Res.Resource):
    """
    Volume resource class.

    Access:
    * rwo  Read Write Once
    * rwx  Read Write Many
    * roo  Read Only Once
    * rox  Read Only Many
    """

    def __init__(self, rid=None, name=None, pool=None, size=None, format=True, access="rwo", **kwargs):
        Res.Resource.__init__(self, rid, **kwargs)
        self.type = "volume"
        self.access = access
        self.name = name
        self.pool = pool
        self.size = size
        self.format = format
        self.label = name

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    @lazy
    def volsvc(self):
        return Svc(svcname=self.name, namespace=self.svc.namespace, node=self.svc.node)

    @lazy
    def mount_point(self):
        return self.volsvc.mount_point()

    @lazy
    def device(self):
        return self.volsvc.device()

    def stop(self):
        if not self.volsvc.exists():
            self.log.info("volume %s does not exist", self.name)
            return
        if self.volsvc.topology == "flex":
            return
        if self.volsvc.action("stop", options={"local": True}) != 0:
            raise ex.excError

    def start(self):
        if not self.volsvc.exists():
            raise ex.excError("volume %s does not exist" % self.name)
        if self.volsvc.action("start", options={"local": True}) != 0:
            raise ex.excError
        self.can_rollback = True
        self.unset_lazy("device")
        self.unset_lazy("mount_point")

    def _status(self, verbose=False):
        if not self.volsvc.exists():
            self.status_log("volume %s does not exist" % self.name, "info")
            return rcStatus.DOWN
        status = rcStatus.Status(self.volsvc.print_status_data()["avail"])
        return status

    def exposed_devs(self):
        return set([self.volsvc.device()])

