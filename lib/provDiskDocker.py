import provisioning
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.r.has_it()

    def unprovisioner(self):
        if not self.r.has_it():
            return
        cmd = self.r.svc.dockerlib.docker_cmd + ["volume", "rm", "-f", self.r.volname]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

