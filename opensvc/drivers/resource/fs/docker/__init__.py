import os

import core.status
import utilities.subsystems.docker as dockerlib
import core.exceptions as ex
from utilities.lazy import lazy
from core.resource import Resource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "docker"
KEYWORDS = [
    {
        "keyword": "driver",
        "default": "local",
        "at": True,
        "text": "The docker volume driver to use for the resource.",
        "example": "tmpfs"
    },
    {
        "keyword": "options",
        "at": True,
        "convert": "shlex",
        "text": "The docker volume create options to use for the resource. :opt:`--label` and :opt:`--opt`",
        "example": "--opt o=size=100m,uid=1000 --opt type=tmpfs --opt device=tmpfs"
    },
    {
        "section": "fs",
        "keyword": "populate",
        "at": True,
        "convert": "list",
        "provisioning": True,
        "text": "The list of modulesets providing files to install in the volume.",
        "example": "configmap.redis configmap.global"
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    data = []
    if dockerlib.has_docker(["docker", "docker.io"]):
        data.append("fs.docker")
    return data


class FsDocker(Resource):
    def __init__(self, driver=None, options=None, populate=None, **kwargs):
        super(FsDocker, self).__init__(type='fs.docker', **kwargs)
        self.driver = driver
        self.options = options
        self.populate = populate or []

    @lazy
    def lib(self):
        """
        Lazy allocator for the dockerlib object.
        """
        try:
            return self.svc.dockerlib
        except AttributeError:
            self.svc.dockerlib = dockerlib.DockerLib(self.svc)
            return self.svc.dockerlib

    @lazy
    def label(self): # pylint: disable=method-hidden
        return "%s volume %s" % (self.driver, self.volname)

    def _status(self, verbose=False):
        return core.status.NA

    def _info(self):
        data = [
          ["name", self.volname],
          ["driver", self.driver],
          ["options", self.options],
          ["vol_path", self.vol_path],
        ]
        return data

    @lazy
    def mount_point(self):
        mount_point = self.lib.container_data_dir
        if mount_point is None:
            mount_point = "/var/tmp"
        return mount_point

    @lazy
    def vol_path(self):
        return self.lib.docker_volume_inspect(self.volname).get("Mountpoint")

    def has_it(self):
        try:
            data = self.lib.docker_volume_inspect(self.volname)
            return True
        except (ValueError, IndexError):
            return False

    def is_up(self):
        """
        Returns True if the logical volume is present and activated
        """
        return self.has_it()

    @lazy
    def volname(self):
        if self.svc.namespace:
            return ".".join([self.svc.namespace.lower(), self.svc.name, self.rid.replace("#", ".")])
        else:
            return ".".join([self.svc.name, self.rid.replace("#", ".")])

    def create_vol(self):
        if self.has_it():
            return 0
        cmd = self.lib.docker_cmd + ["volume", "create", "--name", self.volname]
        if self.options:
            cmd += self.options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def start(self):
        pass

    def stop(self):
        pass

    def provisioned(self):
        return self.has_it()

    def provisioner(self):
        self.lib.docker_start()
        self.create_vol()
        self.populatefs()

    def populatefs(self):
        modulesets = self.populate
        if not modulesets:
            return
        try:
            os.environ["OPENSVC_VOL_PATH"] = self.vol_path
            self.svc.compliance.options.moduleset = ",".join(modulesets)
            ret = self.svc.compliance.do_run("fix")
        finally:
            del os.environ["OPENSVC_VOL_PATH"]
        if ret:
            raise ex.Error

    def unprovisioner(self):
        if not self.has_it():
            return
        cmd = self.lib.docker_cmd + ["volume", "rm", "-f", self.volname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

