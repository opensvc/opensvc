import os
import provFs
from rcUtilities import which, convert_size
from rcZfs import Dataset
from svcBuilder import conf_get_string_scope

class ProvisioningFs(provFs.ProvisioningFs):
    def unprovision_dev(self):
        if not which('zfs'):
            self.r.log.error("zfs command not found")
            raise ex.excError
        ds = Dataset(self.r.device, log=self.r.log)
        if ds.exists():
            ds.destroy(["-r"])
        if os.path.exists(self.r.mountPoint) and os.path.isdir(self.r.mountPoint):
            os.rmdir(self.r.mountPoint)

    def provision_dev(self):
        if not which('zfs'):
            self.r.log.error("zfs command not found")
            raise ex.excError
        ds = Dataset(self.r.device, log=self.r.log)
        if ds.exists() is False:
            ds.create(['-p', '-o', 'mountpoint='+self.r.mountPoint, '-o', 'canmount=noauto'])

        nv_list = dict()
        try:
            size = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "size")
        except:
            size = None
        if size:
            nv_list['refquota'] = "%dM" % convert_size(size, _to="m")
        ds.verify_prop(nv_list)

    def provisioner(self):
        self.provision_dev()
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def unprovisioner(self):
        self.r.stop()
        self.unprovision_dev()
        self.r.log.info("unprovisioned")
        return True
