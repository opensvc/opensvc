from provisioning import Provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import which
import os
import rcExceptions as ex

class ProvisioningKvm(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

        self.section = r.svc.config.defaults()

        if 'snapof' in self.section:
            self.snapof = self.section['snapof']
        else:
            self.snapof = None

        if 'snap' in self.section:
            self.snap = self.section['snap']
        else:
            self.snap = None

        if 'virtinst' in self.section:
            self.virtinst = self.section['virtinst']
        else:
            self.virtinst = None

    def check_kvm(self):
        if os.path.exists(self.r.cf):
            return True
        return False

    def setup_kvm(self):
        if self.virtinst is None:
            self.r.log.error("the 'virtinst' parameter must be set")
            raise ex.excError
        ret, out = self.r.vcall(self.virtinst.split())
        if ret != 0:
            raise ex.excError


    def setup_ips(self):
        self.purge_known_hosts()
        for rs in self.r.svc.get_res_sets("ip"):
            for r in rs.resources:
                self.purge_known_hosts(r.addr)

    def purge_known_hosts(self, ip=None):
        if ip is None:
            cmd = ['ssh-keygen', '-R', self.r.svc.svcname]
        else:
            cmd = ['ssh-keygen', '-R', ip]
        ret, out = self.r.vcall(cmd, err_to_info=True)

    def setup_snap(self):
        if self.snap is None:
            self.r.log.error("the 'snap' parameter must be set")
            raise ex.excError
        if self.snapof is None:
            self.r.log.error("the 'snapof' parameter must be set")
            raise ex.excError
        if not which('btrfs'):
            self.r.log.error("'btrfs' command not found")
            raise ex.excError
 
        cmd = ['btrfs', 'subvolume', 'snapshot', self.snapof, self.snap]
        ret, out = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def provisioner(self):
        self.setup_snap()
        self.setup_kvm()
        self.setup_ips()

        self.r.start()
        self.r.log.info("provisioned")
        return True
