import os
import sys

import rcContainer
import core.exceptions as ex
import rcStatus

from rcGlobalEnv import rcEnv
from rcUtilities import lazy
from core.resource import Resource
from svcBuilder import init_kwargs
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
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["driver"] = svc.oget(s, "driver")
    kwargs["options"] = svc.oget(s, "options")
    r = FsDocker(**kwargs)
    svc += r


class FsDocker(Resource):
    def __init__(self, driver=None, options=None, **kwargs):
        super(FsDocker, self).__init__(type='fs.docker', **kwargs)
        self.driver = driver
        self.options = options

    @lazy
    def lib(self):
        """
        Lazy allocator for the dockerlib object.
        """
        try:
            return self.svc.dockerlib
        except AttributeError:
            self.svc.dockerlib = rcContainer.DockerLib(self.svc)
            return self.svc.dockerlib

    @lazy
    def label(self): # pylint: disable=method-hidden
        return "%s volume %s" % (self.driver, self.volname)

    def _status(self, verbose=False):
        return rcStatus.NA

    def _info(self):
        data = [
          ["name", self.volname],
          ["driver", self.driver],
          ["options", self.options],
          ["vol_path", self.vol_path],
        ]
        return data

    def on_add(self):
        self.mount_point = self.lib.container_data_dir
        if self.mount_point is None:
            self.mount_point = "/var/tmp"

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
        self.populate()

    def populate(self):
        modulesets = self.oget("populate")
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

