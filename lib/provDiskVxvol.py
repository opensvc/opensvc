import os
import provisioning

from converters import convert_size
from rcUtilities import justcall, which
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def stop(self):
        # leave the vxvol active for wipe
        pass

    def unprovisioner(self):
        if not which('vxassist'):
            raise ex.excError("vxassist command not found")

        if not self.r.has_it():
            self.r.log.info("skip vxvol unprovision: %s already unprovisioned", self.r.fullname)
            return

        if which('wipefs') and os.path.exists(self.r.devpath) and self.r.is_up():
            self.r.vcall(["wipefs", "-a", self.r.devpath])

        cmd = ["vxassist", "-g", self.r.vg, "remove", "volume", self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.svc.node.unset_lazy("devtree")

    def provisioner(self):
        if not which('vxassist'):
            raise ex.excError("vxassist command not found")

        if self.r.has_it():
            self.r.log.info("skip vxvol provision: %s already exists" % self.r.fullname)
            return

        try:
            self.size = self.r.conf_get("size")
            self.size = str(self.size).upper()
            size_parm = [str(convert_size(self.size, _to="m"))+'M']
        except Exception as e:
            self.r.log.info("skip vxvol provisioning: %s %s" % (self.r.fullname, str(e)))
            return

        create_options = self.r.oget("create_options")

        # strip dev dir in case the alloc vxassist parameter was formatted using sub_devs
        # lazy references
        for idx, option in enumerate(create_options):
            create_options[idx] = option.replace("/dev/vx/dsk/", "")

        # create the logical volume
        cmd = ['vxassist', '-g', self.r.vg, "make", self.r.name] + size_parm + create_options
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.r.can_rollback = True
        self.r.svc.node.unset_lazy("devtree")

