import os
import rcExceptions as ex
import resources
from rcGlobalEnv import rcEnv
from rcUtilities import lazy
from svcBuilder import init_kwargs
import rcContainer
import rcStatus


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["driver"] = svc.oget(s, "driver")
    kwargs["options"] = svc.oget(s, "options")
    r = Fs(**kwargs)
    svc += r


class Fs(resources.Resource):
    def __init__(self,
                 rid=None,
                 driver=None,
                 options=None,
                 **kwargs):
        resources.Resource.__init__(self,
                                    rid=rid,
                                    type='fs.docker',
                                    **kwargs)
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
            raise ex.excError

    def start(self):
        pass

    def stop(self):
        pass

