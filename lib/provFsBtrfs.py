import provFs
import tempfile
import os
import time
import rcExceptions as ex
from rcUtilities import which, justcall, lazy
from svcBuilder import conf_get_string_scope

class ProvisioningFs(provFs.ProvisioningFs):
    info = ['btrfs', 'device', 'ready']

    @lazy
    def mkfs(self):
        return ['mkfs.btrfs', '-f', '-L', self.label]

    @lazy
    def raw_label(self):
        return '{svcname}.' + self.r.rid.replace("#", ".")

    @lazy
    def label(self):
        return self.r.svc.svcname + '.' + self.r.rid.replace("#", ".")

    def get_subvol(self):
        l = self.r.mntOpt.split(",")
        for e in l:
            if not e.startswith("subvol="):
                continue
            subvol = e.replace("subvol=", "")
            return subvol

    def cleanup(self, mnt):
        cmd = ["umount", mnt]
        self.r.vcall(cmd)
        os.removedirs(mnt)

    def write_label(self):
        self.r.svc.config.set(self.r.rid, "dev", "LABEL="+self.raw_label)
        self.r.svc.write_config()
        self.r.device = "LABEL="+self.label
        self.wait_label()

    def wait_label(self):
        if which("findfs") is None:
            self.r.log.info("findfs program not found, wait arbitrary 20 seconds for label to be usable")
            time.sleep(20)
        cmd = ["findfs", "LABEL="+self.label]
        for i in range(20):
            out, err, ret = justcall(cmd)
            self.r.log.debug("%s\n%s\n%s" % (" ".join(cmd), out, err))
            if ret == 0:
                return
            self.r.log.info("label is not usable yet (%s)" % err.strip())
            time.sleep(2)
        raise ex.excError("timeout waiting for label to become usable")

    def mount(self, mnt):
        cmd = ["mount", "-t", "btrfs", "-o", "subvolid=0", self.r.device, mnt]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def create_subvol(self):
        subvol = self.get_subvol()
        if subvol is None:
            return
        mnt = tempfile.mkdtemp()

        self.mount(mnt)
        self.write_label()

        # create subvol
        path = os.path.join(mnt, subvol)

        if os.path.exists(path):
            self.cleanup(mnt)
            return

        cmd = ["btrfs", "subvol", "create", path]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            self.cleanup(mnt)
            raise ex.excError

        self.cleanup(mnt)

    def provisioner(self):
        self.r.device = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "dev")
        if self.r.device.startswith("LABEL=") or self.r.device.startswith("UUID="):
            self.r.log.info("skip formatting because dev is specified by LABEL or UUID")
        else:
            provFs.ProvisioningFs.provisioner_fs(self)
        self.create_subvol()
        self.r.log.info("provisioned")
        self.r.start()



