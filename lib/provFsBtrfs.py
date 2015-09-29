from provFs import ProvisioningFs
import tempfile
import os
import time
import rcExceptions as ex
from rcUtilities import which, justcall

class ProvisioningFsBtrfs(ProvisioningFs):
    mkfs = ['mkfs.btrfs']
    info = ['btrfs', 'device', 'ready']

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

    def label(self, mnt):
        cmd = ["btrfs", "filesystem", "label", mnt]
        ret, out, err = self.r.call(cmd, errlog=False)
        if ret == 0 and len(out.strip()) > 0:
            label = out.strip()
            relabel = False
        else:
            label = self.r.svc.svcname + '.' + self.r.rid.replace("#", ".")
            relabel = True

        if relabel:
            cmd = ["btrfs", "filesystem", "label", mnt, label]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                self.cleanup(mnt)
                raise ex.excError

        self.r.svc.config.set(self.r.rid, "dev", "LABEL="+label)
        self.r.svc.write_config()
        self.r.device = "LABEL="+label
        self.wait_label(label)

    def wait_label(self, label):
        if which("findfs") is None:
            self.r.log.info("findfs program not found, wait arbitrary 20 seconds for label to be usable")
            time.sleep(20)
        cmd = ["findfs", "LABEL="+label]
        for i in range(20):
            out, err, ret = justcall(cmd)
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
        self.label(mnt)

        # create subvol
        path = os.path.join(mnt, subvol)

        if os.path.exists(path):
            self.cleanup()
            return

        cmd = ["btrfs", "subvol", "create", path]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            self.cleanup(mnt)
            raise ex.excError

        self.cleanup(mnt)

    def provisioner(self):
        if self.r.device.startswith("LABEL=") or self.r.device.startswith("UUID="):
            raise ex.excError("dev with LABEL= or UUID= are not supported for provisioning")
        ProvisioningFs.provisioner_fs(self)
        self.create_subvol()
        self.r.log.info("provisioned")
        self.r.start()



