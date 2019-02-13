import provisioning
import rcExceptions as ex

from rcUtilities import drop_option
from rcGlobalEnv import rcEnv

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.r.has_it()

    def unprovisioner(self):
        if not self.r.is_up():
            ret = self.r.import_pool(verbose=False)
            if ret != 0:
                self.r.log.info("already unprovisioned")
                return
        cmd = ["zpool", "destroy", "-f", self.r.name]
        ret, _, _ = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.svc.node.unset_lazy("devtree")

    def stop(self):
        # a pool must be imported for destroy
        pass

    def provisioner(self):
        if self.is_provisioned():
            self.r.log.info("already provisioned")
            return
        name = self.r.name
        vdev = self.r.oget("vdev")
        multihost = self.r.oget("multihost")
        create_options = self.r.oget("create_options")

        args = create_options
        args += [name]
        args += vdev
        args = drop_option("-m", args, drop_value=True)
        args = drop_option("-o", args, drop_value="cachefile")
        args = drop_option("-o", args, drop_value="multihost")
        args = [
            "-m", "legacy",
            "-o", "cachefile="+self.r.zpool_cache,
        ] + args
        if multihost and rcEnv.sysname == "Linux":
            args = ["-o", "multihost=on"] + args
            self.r.zgenhostid()
        cmd = ["zpool", "create"] + args
        ret, _, _ = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.can_rollback = True
        self.r.svc.node.unset_lazy("devtree")

