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
        self.size = r.svc.config.get(self.r.rid, 'size')

    def provisioner(self):
        for image in self.r.images:
            self.provisioner_one(image)
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def provisioner_one(self, image):
        if self.r.exists(image):
            self.r.log.info("%s already provisioned"%image)
            return

        cmd = self.r.rbd_rcmd() + ['create', '--size', str(self.size), image]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError


