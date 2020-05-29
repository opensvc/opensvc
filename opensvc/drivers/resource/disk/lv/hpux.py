import core.exceptions as ex

from . import BaseDiskLv
from utilities.converters import convert_size
from utilities.proc import justcall, which

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "lv"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("vgdisplay") and which("lvcreate"):
        return ["disk.lv"]
    return []

class DiskLv(BaseDiskLv):
    def provisioner(self):
        if not which('vgdisplay'):
            self.log.error("vgdisplay command not found")
            raise ex.Error

        if not which('lvcreate'):
            self.log.error("lvcreate command not found")
            raise ex.Error

        if self.vg is None:
            raise ex.Error("skip lv provisioning: vg is not set")

        if self.size is None:
            raise ex.Error("skip lv provisioning: size is not set")

        size = convert_size(self.size, _to="m")
        vg = self.vg

        cmd = ['vgdisplay', vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.error("volume group %s does not exist" % vg)
            raise ex.Error

        # create the logical volume
        cmd = ['lvcreate', '-n', self.name, '-L', str(size)+'M', vg]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        self.svc.node.unset_lazy("devtree")

