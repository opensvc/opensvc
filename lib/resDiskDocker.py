import os
import rcExceptions as ex
import resDisk
from rcGlobalEnv import rcEnv
from rcUtilities import lazy

class Disk(resDisk.Disk):
    def __init__(self,
                 rid=None,
                 driver=None,
                 options=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                              rid=rid,
                              name="",
                              type='disk.docker',
                              **kwargs)
        self.driver = driver
        self.options = options

    @lazy
    def label(self):
        return "%s volume %s" % (self.driver, self.volname)

    def _info(self):
        data = [
          ["name", self.volname],
          ["driver", self.driver],
          ["options", self.options],
        ]
        return data

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
        return ".".join([self.svc.svcname, self.rid.replace("#", ".")])

    def create_vol(self):
        cmd = self.svc.dockerlib.docker_cmd + ["volume", "create", "--name", self.volname]
        if self.options:
            cmd += options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        if self.has_it():
            self.log.info("%s is already created" % self.label)
            return 0
        self.create_vol()
        self.can_rollback = True

    def do_stop(self):
        pass

