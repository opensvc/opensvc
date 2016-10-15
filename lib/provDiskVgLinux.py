from provisioning import Provisioning
from svcBuilder import conf_get_string_scope
import os
import json
import rcExceptions as ex
from stat import *
from rcUtilities import justcall
import glob

class ProvisioningDisk(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def unprovisioner(self):
        if not self.r.has_it():
            self.r.log.info("already unprovisioned")
            return
        cmd = ['vgremove', '-ff', self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError



    def provisioner(self):
        if self.r.has_it():
            self.r.log.info("already provisioned")
            return

        try:
            self.pvs = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "pvs")
        except ex.OptNotFound:
            raise ex.excError("the 'pvs' parameter is mandatory for provisioning")

        self.pvs = self.pvs.split()
        l = []
        for pv in self.pvs:
            _l = glob.glob(pv)
            self.r.log.info("expand %s to %s" % (pv, ', '.join(_l)))
            l += _l
        self.pvs = l

        err = False
        for i, pv in enumerate(self.pvs):
            pv = os.path.realpath(pv)
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

        if len(self.pvs) == 0:
            raise ex.excError("no pvs specified")

        cmd = ['vgcreate', self.r.name] + self.pvs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.remove_keywords(["pvs"])
        self.r.clear_cache("vg.lvs")
        self.r.clear_cache("vg.lvs.attr")
        self.r.clear_cache("vg.tags")
        self.r.log.info("provisioned")
        return True
