"""
Volume resource driver module.
"""
import os

import resources as Res
import rcExceptions as ex
import rcStatus
from rcUtilities import lazy, factory, fmt_svcpath, split_svcpath

class Volume(Res.Resource):
    """
    Volume resource class.

    Access:
    * rwo  Read Write Once
    * rwx  Read Write Many
    * roo  Read Only Once
    * rox  Read Only Many
    """

    def __init__(self, rid=None, name=None, pool=None, size=None, format=True,
                 access="rwo", secrets=None, configs=None, **kwargs):
        Res.Resource.__init__(self, rid, **kwargs)
        self.type = "volume"
        self.access = access
        self.name = name
        self.pool = pool
        self.size = size
        self.format = format
        self.secrets = secrets
        self.configs = configs
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.volname)

    def set_label(self):
        self.label = self.volname

    def on_add(self):
        self.set_label()

    @lazy
    def volname(self):
        if self.name:
            return self.name
        else:
            return "%s-vol-%s" % (self.svc.svcname, self.rid.split("#")[-1])

    @lazy
    def volsvc(self):
        return factory("vol")(svcname=self.volname, namespace=self.svc.namespace, node=self.svc.node)

    @lazy
    def mount_point(self):
        return self.volsvc.mount_point()

    @lazy
    def device(self):
        return self.volsvc.device()

    def stop(self):
        if not self.volsvc.exists():
            self.log.info("volume %s does not exist", self.volname)
            return
        if self.volsvc.topology == "flex":
            return
        if self.volsvc.action("stop", options={"local": True}) != 0:
            raise ex.excError

    def start(self):
        if not self.volsvc.exists():
            raise ex.excError("volume %s does not exist" % self.volname)
        if self.volsvc.action("start", options={"local": True}) != 0:
            raise ex.excError
        self.install_secrets()
        self.install_configs()
        self.can_rollback = True
        self.unset_lazy("device")
        self.unset_lazy("mount_point")

    def _status(self, verbose=False):
        if not self.volsvc.exists():
            self.status_log("volume %s does not exist" % self.volname, "info")
            return rcStatus.DOWN
        status = rcStatus.Status(self.volsvc.print_status_data()["avail"])
        return status

    def exposed_devs(self):
        return set([self.volsvc.device()])

    def data_data(self, kind):
        """
        Transform the secrets/configs mappings list into a list of data structures.
        """
        if not self.mount_point:
            # volume not yet provisioned
            return []
        if kind == "sec" and self.secrets:
            refs = self.secrets
        elif kind == "cfg" and self.configs:
            refs = self.configs
        else:
            refs = []
        data = []
        for ref in refs:
            try:
                datapath, path = ref.split(":", 1)
            except Exception:
                continue
            try:
                datapath, key = datapath.split("/", 1)
            except Exception:
                continue
            if path in ("", os.sep):
                path = self.mount_point.rstrip(os.sep) + os.sep
            else:
                path = os.path.join(self.mount_point.rstrip(os.sep), path.lstrip(os.sep))
            data.append({
                "obj": fmt_svcpath(datapath, namespace=self.svc.namespace, kind=kind),
                "key": key,
                "path": path,
            })
        return data

    def _install_data(self, kind):
        for data in self.data_data(kind):
            name, _, _ = split_svcpath(data["obj"])
            obj = factory(kind)(name, namespace=self.svc.namespace, volatile=True, node=self.svc.node)
            for key in obj.resolve_key(data["key"]):
                obj._install(key, data["path"])

    def has_data(self, kind, name, key=None):
        for data in self.data_data(kind):
            if data["obj"] != name:
                continue
            if key and data["key"] != key:
                continue
            return True
        return False

    def install_data(self):
        self.install_secrets()
        self.install_configs()

    def install_configs(self):
        self._install_data("cfg")

    def install_secrets(self):
        self._install_data("sec")

    def has_config(self, name, key=None):
        return self.has_data("cfg", name, key)

    def has_secret(self, name, key=None):
        return self.has_data("sec", name, key)

