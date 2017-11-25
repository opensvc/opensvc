"""
The zfs dataset provisioning driver.

Supports the provisioning keywords:

* size
* mkfs_opt
"""
import os
import provFs
from rcUtilities import which
from converters import convert_size
from rcGlobalEnv import rcEnv
from rcZfs import Dataset
import rcExceptions as ex

class Prov(provFs.Prov):
    """
    The zfs dataset provisioning class.
    """
    def unprovisioner(self):
        if not which(rcEnv.syspaths.zfs):
            self.r.log.error("zfs command not found")
            raise ex.excError
        dataset = Dataset(self.r.device, log=self.r.log)
        if dataset.exists():
            dataset.destroy(["-r"])
        if os.path.exists(self.r.mount_point) and os.path.isdir(self.r.mount_point):
            try:
                os.rmdir(self.r.mount_point)
                self.r.log.info("rmdir %s", self.r.mount_point)
            except OSError as exc:
                self.r.log.warning("failed to rmdir %s: %s", self.r.mount_point, exc)

    def provisioner(self):
        if not which(rcEnv.syspaths.zfs):
            self.r.log.error("zfs command not found")
            raise ex.excError
        dataset = Dataset(self.r.device, log=self.r.log)
        mkfs_opt = ["-p"]
        try:
            mkfs_opt += self.r.svc.conf_get(self.r.rid, "mkfs_opt")
        except ex.OptNotFound:
            pass

        if not any([True for e in mkfs_opt if e.startswith("mountpoint=")]):
            mkfs_opt += ['-o', 'mountpoint='+self.r.mount_point]
        if not any([True for e in mkfs_opt if e.startswith("canmount=")]):
            mkfs_opt += ['-o', 'canmount=noauto']

        if dataset.exists() is False:
            dataset.create(mkfs_opt)

        nv_list = dict()
        try:
            size = self.r.svc.conf_get(self.r.rid, "size")
        except ex.OptNotFound:
            return
        if size:
            nv_list['refquota'] = "%dM" % convert_size(size, _to="m")
        dataset.verify_prop(nv_list)

    def is_provisioned(self):
        dataset = Dataset(self.r.device, log=self.r.log)
        return dataset.exists()
