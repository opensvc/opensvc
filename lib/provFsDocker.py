import sys
import os

import provisioning
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.r.has_it()

    def provisioner(self):
        self.r.svc.dockerlib.docker_start()
        self.r.create_vol()
        self.populate()

    def populate(self):
        modulesets = self.r.oget("populate")
        if not modulesets:
            return
        try:
            os.environ["OPENSVC_VOL_PATH"] = self.r.vol_path
            self.r.svc.compliance.options.moduleset = ",".join(modulesets)
            ret = self.r.svc.compliance.do_run("fix")
        finally:
            del os.environ["OPENSVC_VOL_PATH"]
        if ret:
            raise ex.excError

    def unprovisioner(self):
        if not self.r.has_it():
            return
        cmd = self.r.svc.dockerlib.docker_cmd + ["volume", "rm", "-f", self.r.volname]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

