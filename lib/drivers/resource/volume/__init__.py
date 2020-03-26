"""
Volume resource driver module.
"""
import os

import exceptions as ex
import rcStatus

from converters import print_size
from rcUtilities import lazy, factory, fmt_path, split_path, makedirs, factory
from resources import Resource
from svcBuilder import init_kwargs
from core.objects.svcdict import KEYS
from utilities.string import is_string, is_glob

DRIVER_GROUP = "volume"
DRIVER_BASENAME = None
KEYWORDS = [
    {
        "keyword": "name",
        "at": True,
        "required": False,
        "text": "The volume service name. A service can only reference volumes in the same namespace."
    },
    {
        "keyword": "type",
        "provisioning": True,
        "at": True,
        "required": False,
        "text": "The type of the pool to allocate from. The selected pool will be the one matching type and capabilities and with the maximum available space."
    },
    {
        "keyword": "access",
        "default": "rwo",
        "candidates": ["rwo", "roo", "rwx", "rox"],
        "provisioning": True,
        "at": True,
        "required": False,
        "text": "The access mode of the volume. ``rwo`` is Read Write Once, ``roo`` is Read Only Once, ``rwx`` is Read Write Many, ``rox`` is Read Only Many. ``rox`` and ``rwx`` modes are served by flex volume services.",
    },
    {
        "keyword": "size",
        "at": True,
        "convert": "size",
        "provisioning": True,
        "required": True,
        "text": "The size to allocate in the pool."
    },
    {
        "keyword": "pool",
        "at": True,
        "provisioning": True,
        "text": "The name of the pool to allocate from."
    },
    {
        "keyword": "format",
        "at": True,
        "provisioning": True,
        "default": True,
        "convert": "boolean",
        "text": "If true the volume translator will also produce a fs resource layered over the disk allocated in the pool."
    },
    {
        "keyword": "configs",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<config name>/<key>:<volume relative path>:<options>``.",
        "example": "conf/mycnf:/etc/mysql/my.cnf:ro,mode=0640 conf/sysctl:/etc/sysctl.d/01-db.conf"
    },
    {
        "keyword": "secrets",
        "at": True,
        "rtype": ["shm"],
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<secret name>/<key>:<volume relative path>:<options>``.",
        "example": "cert/pem:server.pem cert/key:server.key:mode=0600"
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["pool"] = svc.oget(s, "pool")
    kwargs["format"] = svc.oget(s, "format")
    kwargs["size"] = svc.oget(s, "size")
    kwargs["access"] = svc.oget(s, "access")
    kwargs["configs"] = svc.oget(s, "configs")
    try:
        kwargs["secrets"] = svc.oget(s, "secrets")
    except ValueError:
        # only supported on type=shm volumes
        pass
    r = Volume(**kwargs)
    svc += r


class Volume(Resource):
    """
    Volume resource class.

    Access:
    * rwo  Read Write Once
    * rwx  Read Write Many
    * roo  Read Only Once
    * rox  Read Only Many
    """

    def __init__(self, name=None, pool=None, size=None, format=True,
                 access="rwo", secrets=None, configs=None, **kwargs):
        super(Volume, self).__init__(type="volume", **kwargs)
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
        return "%s name=%s" % (super(Volume, self).__str__(), self.volname)

    def set_label(self):
        self.label = self.volname

    def on_add(self):
        self.set_label()

    @lazy
    def volname(self):
        if self.name:
            return self.name
        else:
            return "%s-vol-%s" % (self.svc.name, self.rid.split("#")[-1])

    @lazy
    def volsvc(self):
        return factory("vol")(name=self.volname, namespace=self.svc.namespace, node=self.svc.node)

    @lazy
    def mount_point(self):
        return self.volsvc.mount_point()

    @lazy
    def device(self):
        return self.volsvc.device()

    def stop(self):
        self.uninstall_flag()
        if not self.volsvc.exists():
            self.log.info("volume %s does not exist", self.volname)
            return
        if self.volsvc.topology == "flex":
            return
        if self.volsvc.action("stop", options={"local": True, "leader": self.svc.options.leader}) != 0:
            raise ex.Error

    def start(self):
        if not self.volsvc.exists():
            raise ex.Error("volume %s does not exist" % self.volname)
        if self.volsvc.action("start", options={"local": True, "leader": self.svc.options.leader}) != 0:
            raise ex.Error
        self.can_rollback |= any([r.can_rollback for r in self.volsvc.resources_by_id.values()])
        self.install_flag()
        self.install_secrets()
        self.install_configs()
        self.unset_lazy("device")
        self.unset_lazy("mount_point")

    @lazy
    def flag_path(self):
        return os.path.join(self.var_d, "flag")

    def flag_installed(self):
        return os.path.exists(self.flag_path)

    def uninstall_flag(self):
        try:
            os.unlink(self.flag_path)
        except Exception as exc:
            pass

    def install_flag(self):
        makedirs(os.path.dirname(self.flag_path))
        with open(self.flag_path, "w"):
            pass

    def boot(self):
        self.uninstall_flag()

    def _status(self, verbose=False):
        self.data_status()
        if not self.volsvc.exists():
            self.status_log("volume %s does not exist" % self.volname, "info")
            return rcStatus.DOWN
        status = rcStatus.Status(self.volsvc.print_status_data()["avail"])
        if not self.flag_installed():
            self.status_log("%s is %s" % (self.volsvc.path, status), "info")
            return rcStatus.DOWN
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
            datapath = datapath.lstrip("/")
            if datapath.startswith("usr/"):
                kind = "usr"
                datapath = datapath[4:]
            elif datapath.startswith("sec/"):
                datapath = datapath[4:]
            elif datapath.startswith("cfg/"):
                datapath = datapath[4:]
            try:
                datapath, key = datapath.split("/", 1)
            except Exception:
                continue
            try:
                if path in ("", os.sep):
                    path = self.mount_point.rstrip(os.sep) + os.sep
                else:
                    path = os.path.join(self.mount_point.rstrip(os.sep), path.lstrip(os.sep))
            except AttributeError:
                # self.mount_point changed to None since tested, so has no rstrip()
                continue
            data.append({
                "obj": fmt_path(datapath, namespace=self.svc.namespace, kind=kind),
                "key": key,
                "path": path,
            })
        return data

    def data_status(self):
        self._data_status("cfg")
        self._data_status("sec")

    def _data_status(self, kind):
        for data in self.data_data(kind):
            name, _, kind = split_path(data["obj"])
            obj = factory(kind)(name, namespace=self.svc.namespace, volatile=True, node=self.svc.node)
            if not obj.exists():
                self.status_log("referenced %s %s does not exist: "
                                "expected data %s can not be installed in the volume" % (kind, name, data["key"]), "warn")
                continue
            keys = obj.resolve_key(data["key"])
            if not keys and not is_glob(data["key"]):
                self.status_log("%s %s has no key %s. "
                                "expected data can not be installed in the volume" % (kind, name, data["key"]), "warn")

    def _install_data(self, kind):
        for data in self.data_data(kind):
            name, _, kind = split_path(data["obj"])
            obj = factory(kind)(name, namespace=self.svc.namespace, volatile=True, node=self.svc.node)
            if not obj.exists():
                self.log.warning("referenced %s %s does not exist: "
                                 "expected data %s can not be installed in the volume",
                                 kind, name, data["key"])
                continue
            keys = obj.resolve_key(data["key"])
            if not keys and not is_glob(data["key"]):
                self.log.warning("%s %s has no key %s. "
                                 "expected data can not be installed in the volume",
                                 kind, name, data["key"])
                continue
            self.log.debug("install ./%s/%s/%s in %s", kind, name, data["key"], data["path"])
            for key in keys:
                obj.install_key(key, data["path"])

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

    def provisioned(self):
        if not self.volsvc.exists():
            return False
        if not self.owned():
            return False
        return self.volsvc.print_status_data().get("provisioned")

    def claimed(self, volume=None):
        if not volume:
            volume = self.volsvc
        if volume.children:
            return True
        return False

    def owned(self, volume=None):
        if not volume:
            volume = self.volsvc
        if not self.claimed(volume):
            return False
        if self.svc.path not in volume.children:
            return False
        return True

    def owned_exclusive(self, volume=None):
        if not volume:
            volume = self.volsvc
        if set(volume.children) != set([self.svc.path]):
            return False
        return True

    def claim(self, volume):
        if self.shared:
            if self.owned_exclusive(volume):
                self.log.info("volume %s is already claimed exclusively by %s",
                                volume.path, self.svc.path)
                return
            if self.claimed(volume):
                raise ex.Error("shared volume %s is already claimed by %s" % (volume.name, ",".join(volume.children)))
        else:
            if self.owned(volume):
                self.log.info("volume %s is already claimed by %s",
                                volume.path, self.svc.path)
                return
        self.log.info("claim volume %s", volume.name)
        volume.set_multi(["DEFAULT.children+=%s" % self.svc.path])

    def unclaim(self):
        self.log.info("unclaim volume %s", self.volsvc.name)
        self.volsvc.set_multi(["DEFAULT.children-=%s" % self.svc.path], validation=False)

    def unprovisioner(self):
        if not self.volsvc.exists():
            return
        self.unclaim()

    def provisioner_shared_non_leader(self):
        self.provisioner()

    def provisioner(self):
        """
        Create a volume service with resources definitions deduced from the storage
        pool translation rules.
        """
        volume = self.create_volume()
        self.claim(volume)
        self.log.info("provision the %s volume service instance", self.volname)

        # will be rolled back by the volume resource. for now, the remaining
        # resources might need the volume for their provision
        ret = volume.action("provision", options={
            "disable_rollback": True,
            "local": True,
            "leader": self.svc.options.leader,
            "notify": True,
        }) 
        if ret != 0:
            raise ex.Error("volume provision returned %d" % ret)
        self.can_rollback = True
        self.unset_lazy("device")
        self.unset_lazy("mount_point")
        self.unset_lazy("volsvc")

    def create_volume(self):
        volume = factory("vol")(name=self.volname, namespace=self.svc.namespace, node=self.svc.node)
        if volume.exists():
            self.log.info("volume %s already exists", self.volname)
            data = volume.print_status_data(mon_data=True)
            if not data or "cluster" not in data:
                return volume
            if not self.svc.node.get_pool(volume.pool):
                raise ex.Error("pool %s not found on this node" % volume.pool)
            if self.svc.options.leader and volume.topology == "failover" and \
               (self.owned() or not self.claimed(volume)) and \
               data["avail"] != "up":
                cluster_avail = data["cluster"].get("avail")
                if cluster_avail is None:
                    self.log.info("no take over decision, we are leader but unknown cluster avail for volume %s",
                                  self.volname)
                elif cluster_avail == "up":
                    self.log.info("volume %s is up on, peer, we are leader: take it over", self.volname)
                    volume.action("takeover", options={"wait": True, "time": 60})
            return volume
        elif not self.svc.options.leader:
            self.log.info("volume %s does not exist, we are not leader: wait its propagation", self.volname)
            self.wait_for_fn(lambda: volume.exists(), 10, 1,
                               "non leader instance waited for too long for the "
                               "volume to appear")
            return volume
        pooltype = self.oget("type")
        self.log.info("create new volume %s (pool name: %s, pool type: %s, "
                        "access: %s, size: %s, format: %s, shared: %s)",
                        self.volname, self.pool, pooltype, self.access,
                        print_size(self.size, unit="B", compact=True),
                        self.format, self.shared)
        pool = self.svc.node.find_pool(poolname=self.pool,
                                         pooltype=pooltype,
                                         access=self.access,
                                         size=self.size,
                                         fmt=self.format,
                                         shared=self.shared)
        if pool is None:
            raise ex.Error("could not find a pool matching criteria")
        pool.log = self.log
        try:
            nodes = self.svc._get("DEFAULT.nodes")
        except ex.OptNotFound:
            nodes = None
        env = {}
        for mapping in pool.volume_env:
            try:
                src, dst = mapping.split(":", 1)
            except Exception:
                continue
            args = src.split(".", 1)
            val = self.svc.oget(*args)
            if val is None:
                raise ex.Error("missing mapped key in %s: %s" % (self.svc.path, mapping))
            if is_string(val) and ".." in val:
                raise ex.Error("the '..' substring is forbidden in volume env keys: %s=%s" % (mapping, val))
            env[dst] = val
        pool.configure_volume(volume,
                              fmt=self.format,
                              size=self.size,
                              access=self.access,
                              nodes=nodes,
                              shared=self.shared,
                              env=env)
        volume = factory("vol")(name=self.volname, namespace=self.svc.namespace, node=self.svc.node)
        return volume

