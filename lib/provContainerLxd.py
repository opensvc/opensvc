import os
import provisioning
import rcExceptions as ex
from subprocess import PIPE

class Prov(provisioning.Prov):
    def is_provisioned(self):
        return self.r.has_it()

    def start(self):
        """
        Replace the container.lxd start method, to bypass the initial
        lxc import, as it can't suceed on nodes where the container
        storage is not yet available.
        """
        pass

    def provisioner(self):
        if self.r.has_it():
            return
        image = self.r.conf_get("launch_image")
        try:
            options = self.r.conf_get("launch_options")
        except ex.OptNotFound as exc:
            options = exc.default
        cmd = ["/usr/bin/lxc", "launch", image] + options + [self.r.name]
        ret, out, err = self.r.vcall(cmd, stdin=PIPE)
        if ret != 0:
            raise ex.excError
        self.r.wait_for_fn(self.r.is_up, self.r.start_timeout, 2)
        self.r.can_rollback = True

    def unprovisioner(self):
        cmd = ["/usr/bin/lxc", "delete", self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

