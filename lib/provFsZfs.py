import provFs
from rcUtilities import which
from rcZfs import Dataset

class ProvisioningFs(provFs.ProvisioningFs):
    def provision_dev(self):
        if not which('zfs'):
            self.r.log.error("zfs command not found")
            raise ex.excError
        ds = Dataset(self.dev, log=self.r.log)
        if ds.exists() is False:
            ds.create(['-p'])

        nv_list = dict()
        #nv_list['mountpoint'] = self.mnt
        if 'size' in self.section:
            nv_list['refquota'] = self.section['size']
        ds.verify_prop(nv_list)

    def provisioner(self):

        self.provision_dev()

        self.r.log.info("provisioned")
        # self.r.start(),
        return True
