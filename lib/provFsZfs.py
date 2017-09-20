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
            os.rmdir(self.r.mount_point)

    def provisioner(self):
        if not which(rcEnv.syspaths.zfs):
            self.r.log.error("zfs command not found")
            raise ex.excError
        ds = Dataset(self.r.device, log=self.r.log)
        if ds.exists() is False:
            ds.create(['-p', '-o', 'mountpoint='+self.r.mount_point, '-o', 'canmount=noauto'])

        nv_list = dict()
        try:
            size = self.r.svc.conf_get(self.r.rid, "size")
        except ex.OptNotFound as exc:
            size = exc.default
        if size:
            nv_list['refquota'] = "%dM" % convert_size(size, _to="m")
        ds.verify_prop(nv_list)

    def is_provisioned(self):
        ds = Dataset(self.r.device, log=self.r.log)
        return ds.exists()

