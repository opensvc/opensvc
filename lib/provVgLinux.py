from provisioning import Provisioning
import os
import json
import rcExceptions as ex
from stat import *
from rcUtilities import justcall
import glob

class ProvisioningVg(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)
        self.pvs = r.svc.config.get(self.r.rid, 'pvs')
        self.pvs = self.pvs.split()
        l = []
        for pv in self.pvs:
            l += glob.glob(pv)
        self.pvs = l

    def provisioner(self):
        if self.r.has_it():
            self.r.log.info("already provisioned")
            return

        err = False
        for i, pv in enumerate(self.pvs):
            if not os.path.exists(pv):
                self.r.log.error("pv %s does not exist"%pv)
                err |= True
            mode = os.stat(pv)[ST_MODE]
            if S_ISBLK(mode):
                continue
            elif S_ISREG(mode):
                cmd = ['losetup', '-j', pv]
                out, err, ret = justcall(cmd)
                if ret != 0 or not out.startswith('/dev/loop'):
                    self.r.log.error("pv %s a regular file but not a loop"%pv)
                    err |= True
                    continue
                self.pvs[i] = out.split(':')[0]
            else:
                self.r.log.error("pv %s is not a block device nor a loop file"%pv)
                err |= True
        if err:
            raise ex.excError

        for pv in self.pvs:
            cmd = ['pvcreate', '-f', pv]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError

        cmd = ['vgcreate', self.r.name] + self.pvs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.r.log.info("provisioned")
        return True
