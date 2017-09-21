import os
import json
import glob
from stat import *

from rcGlobalEnv import rcEnv
import provisioning
import rcExceptions as ex
from rcUtilities import justcall

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.r.has_it()

    def unprovisioner(self):
        cmd = ['vgremove', '-ff', self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.clear_cache("vg.lvs")
        self.r.clear_cache("vg.lvs.attr")
        self.r.clear_cache("vg.tags")

    def provisioner(self):
        try:
            self.pvs = self.r.svc.conf_get(self.r.rid, "pvs")
        except ex.OptNotFound:
            raise ex.excError("the 'pvs' parameter is mandatory for provisioning")

        if self.pvs is None:
            # lazy reference not resolvable
            raise ex.excError("%s.pvs value is not valid" % self.r.rid)

        self.pvs = self.pvs.split()
        l = []
        for pv in self.pvs:
            _l = glob.glob(pv)
            self.r.log.info("expand %s to %s" % (pv, ', '.join(_l)))
            l += _l
        self.pvs = l

        pv_err = False
        for i, pv in enumerate(self.pvs):
            pv = os.path.realpath(pv)
            if not os.path.exists(pv):
                self.r.log.error("pv %s does not exist"%pv)
                pv_err |= True
            mode = os.stat(pv)[ST_MODE]
            if S_ISBLK(mode):
                continue
            elif S_ISREG(mode):
                cmd = [rcEnv.syspaths.losetup, '-j', pv]
                out, err, ret = justcall(cmd)
                if ret != 0 or not out.startswith('/dev/loop'):
                    self.r.log.error("pv %s is a regular file but not a loop"%pv)
                    pv_err |= True
                    continue
                self.pvs[i] = out.split(':')[0]
            else:
                self.r.log.error("pv %s is not a block device nor a loop file"%pv)
                pv_err |= True
        if pv_err:
            raise ex.excError

        for pv in self.pvs:
            cmd = ['pvcreate', '-f', pv]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError

        if len(self.pvs) == 0:
            raise ex.excError("no pvs specified")

        cmd = ['vgcreate', self.r.name] + self.pvs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.r.clear_cache("vg.lvs")
        self.r.clear_cache("vg.lvs.attr")
        self.r.clear_cache("vg.tags")
        self.r.log.info("provisioned")
        self.r.start()
        return True
