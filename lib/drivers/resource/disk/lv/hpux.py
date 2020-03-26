import os

import exceptions as ex

from . import BaseDiskLv, adder as base_adder
from converters import convert_size
from utilities.proc import justcall, which

def adder(svc, s):
    base_adder(svc, s, drv=DiskLv)

class DiskLv(BaseDiskLv):
    def provisioner(self):
        if not which('vgdisplay'):
            self.log.error("vgdisplay command not found")
            raise ex.excError

        if not which('lvcreate'):
            self.log.error("lvcreate command not found")
            raise ex.excError

        size = self.oget("size")
        size = convert_size(size, _to="m")
        vg = self.oget("vg")

        cmd = ['vgdisplay', vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.error("volume group %s does not exist" % vg)
            raise ex.excError

        # create the logical volume
        cmd = ['lvcreate', '-n', self.name, '-L', str(size)+'M', vg]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.svc.node.unset_lazy("devtree")

