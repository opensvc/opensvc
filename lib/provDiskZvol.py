import provisioning
from rcUtilities import justcall, which
from converters import convert_size
from rcUtilities import bdecode
from rcUtilitiesLinux import label_to_dev
from rcGlobalEnv import rcEnv
from subprocess import *
import os
import rcExceptions as ex
import time
import signal

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.r.has_it()

    def unprovisioner(self):
        if not self.r.has_it():
            self.r.log.info("zvol %s already destroyed", self.r.name)
            return
        cmd = [rcEnv.syspaths.zfs, "destroy", "-f", self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.svc.node.unset_lazy("devtree")

    def provisioner(self):
        if self.r.has_it():
            self.r.log.info("zvol %s already exists", self.r.name)
            return
        size = self.r.conf_get("size")
        create_options = self.r.oget("create_options")
        cmd = [rcEnv.syspaths.zfs, "create", "-V"]
        cmd += create_options
        cmd += [str(convert_size(size, _to="m"))+'M', self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.can_rollback = True

        for i in range(3, 0, -1):
            if os.path.exists(self.r.device):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.r.log.error("timed out waiting for %s to appear" % self.r.device)
            raise ex.excError

        self.r.svc.node.unset_lazy("devtree")

