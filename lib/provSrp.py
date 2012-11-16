from provisioning import Provisioning
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex

class ProvisioningSrp(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

        self.name = r.name
        self.rootpath = os.path.join(os.sep, 'var', 'hpsrp', self.name)
        try:
            self.prm_cores = r.svc.config.get(r.rid, 'prm_cores')
        except:
            self.prm_cores = "1"
        self.ip = r.svc.config.get(r.rid, 'ip')

    def validate(self):
        # False triggers provisioner, True skip provisioner
        if not which('srp'):
            self.r.log.error("this node is not srp capable")
            return True

        if not self.check_srp():
            self.r.log.error("container is not created")
            return True

        return False

    def check_srp(self):
        try:
            self.r.get_status()
        except:
            return False
        return True

    def add_srp(self):
        cmd = ['srp', '-batch', '-a',
               self.name,
               'ip_address='+self.ip, 'assign_ip=no',
               'autostart=no',
               'prm_group_type=PSET',
               'prm_cores='+str(self.prm_cores)]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError()

    def provisioner(self):
        self.add_srp()
        self.r.start()
        self.r.log.info("provisioned")
        return True
