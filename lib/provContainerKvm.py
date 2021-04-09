import provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import which, lazy
import os
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    @lazy
    def snapof(self):
        return self.r.svc.oget(self.r.rid, "snapof")

    @lazy
    def snap(self):
        return self.r.svc.oget(self.r.rid, "snap")

    @lazy
    def virtinst(self):
        return self.r.svc.oget(self.r.rid, "virtinst")

    def check_kvm(self):
        if os.path.exists(self.r.cf):
            return True
        return False

    def setup_kvm(self):
        if self.virtinst is None:
            self.r.log.error("the 'virtinst' parameter must be set")
            raise ex.excError
        cmd = [] + self.virtinst
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def setup_ips(self):
        self.purge_known_hosts()
        for resource in self.r.svc.get_resources("ip"):
            self.purge_known_hosts(resource.addr)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.name]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out, err = self.r.vcall(cmd, err_to_info=True)

    def setup_snap(self):
        if self.snap is None and self.snapof is None:
            return
        elif self.snap and self.snapof is None:
            self.r.log.error("the 'snapof' parameter is required when 'snap' parameter present")
            raise ex.excError
        elif self.snapof and self.snap is None:
            self.r.log.error("the 'snap' parameter is required when 'snapof' parameter present")
            raise ex.excError

        if not which('btrfs'):
            self.r.log.error("'btrfs' command not found")
            raise ex.excError

        cmd = ['btrfs', 'subvolume', 'snapshot', self.snapof, self.snap]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def provisioner(self):
        self.setup_snap()
        self.setup_kvm()
        self.setup_ips()
        self.r.log.info("provisioned")
        return True
