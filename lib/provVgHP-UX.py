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
        try:
            self.options = r.svc.config.get(self.r.rid, 'options').split()
        except:
            self.options = []
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
            else:
                self.r.log.error("pv %s is not a block device nor a loop file"%pv)
                err |= True
        if err:
            raise ex.excError

        for pv in self.pvs:
            pv = pv.replace('/disk/', '/rdisk/')
            cmd = ['pvcreate', '-f', pv]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError

        pvs = []
        for pv in self.pvs:
            pvs.append(pv.replace('/rdisk/', '/disk/'))
        cmd = ['vgcreate']
        if len(self.options) > 0:
            cmd += self.options
        cmd += [self.r.name] + pvs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.r.log.info("provisioned")
        return True
