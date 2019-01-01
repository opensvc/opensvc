import os
import rcExceptions as ex
import resources
from rcGlobalEnv import rcEnv
from rcUtilities import lazy
import rcStatus

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
        self.mount_point = self.svc.dockerlib.docker_data_dir
        if self.mount_point is None:
            self.mount_point = "/var/tmp"

    @lazy
    def vol_path(self):
        return self.svc.dockerlib.docker_volume_inspect(self.volname).get("Mountpoint")

    def has_it(self):
        try:
            data = self.svc.dockerlib.docker_volume_inspect(self.volname)
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
            return ".".join([self.svc.namespace.lower(), self.svc.svcname, self.rid.replace("#", ".")])
        else:
            return ".".join([self.svc.svcname, self.rid.replace("#", ".")])

    def create_vol(self):
        if self.has_it():
            return 0
        cmd = self.svc.dockerlib.docker_cmd + ["volume", "create", "--name", self.volname]
        if self.options:
            cmd += self.options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def start(self):
        pass

    def stop(self):
        pass

