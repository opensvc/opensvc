from provisioning import Provisioning
from rcUtilities import justcall, which, convert_size
from rcGlobalEnv import rcEnv
from svcBuilder import conf_get_string_scope
import os
import rcExceptions as ex
import time

class ProvisioningLv(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provision_lv(self):
        if not which('vgdisplay'):
            self.r.log.error("vgdisplay command not found")
            raise ex.excError

        if not which('lvcreate'):
            self.r.log.error("lvcreate command not found")
            raise ex.excError

        try:
            self.size = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "size")
            self.size = convert_size(self.size, _to="m")
            self.vg = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vg")
        except Exception as e:
            self.r.log.info("skip lv provisioning: %s" % str(e))
            return

        cmd = ['vgdisplay', self.vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("volume group %s does not exist"%self.vg)
            raise ex.excError

        dev = os.path.basename(self.r.device)

        # create the logical volume
        cmd = ['lvcreate', '-n', dev, '-L', str(self.size)+'M', self.vg]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError



