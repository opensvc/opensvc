import os
import provFs
from rcUtilities import which
from converters import convert_size
from rcGlobalEnv import rcEnv
from rcZfs import Dataset
import rcExceptions as ex

class Prov(provFs.Prov):
    def unprovisioner(self):
        if not which(rcEnv.syspaths.zfs):
            self.r.log.error("zfs command not found")
            raise ex.excError
        ds = Dataset(self.r.device, log=self.r.log)
        if ds.exists():
            ds.destroy(["-r"])
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
        ds = Dataset(self.r.device, log=self.r.log)
        mkfs_opt = ["-p"]
        try:
            mkfs_opt += self.r.svc.conf_get(self.r.rid, "mkfs_opt")
        except ex.OptNotFound as exc:
            pass

        if not any([True for e in mkfs_opt if e.startswith("mountpoint=")]):
            mkfs_opt += ['-o', 'mountpoint='+self.r.mount_point]
        if not any([True for e in mkfs_opt if e.startswith("canmount=")]):
            mkfs_opt += ['-o', 'canmount=noauto']

        if ds.exists() is False:
            ds.create(mkfs_opt)

        nv_list = dict()
        try:
            size = self.r.svc.conf_get(self.r.rid, "size")
        except ex.OptNotFound as exc:
            return
        if size:
            nv_list['refquota'] = "%dM" % convert_size(size, _to="m")
        ds.verify_prop(nv_list)

    def is_provisioned(self):
        ds = Dataset(self.r.device, log=self.r.log)
        return ds.exists()

