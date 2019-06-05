import provisioning
import os
import json
import rcExceptions as ex
from stat import *
from rcUtilities import justcall, lazy
import glob

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    @lazy
    def options(self):
        return self.r.svc.oget(self.r.rid, 'options').split()

    @lazy
    def pvs(self):
        try:
            pvs = self.r.svc.oget(self.r.rid, 'pvs')
        except:
            raise ex.excError("pvs provisioning keyword is not set")
        pvs = pvs.split()
        l = []
        for pv in pvs:
            l += glob.glob(pv)
        return l

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
        self.r.svc.node.unset_lazy("devtree")
