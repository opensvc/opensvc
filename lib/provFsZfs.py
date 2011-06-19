#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2011 Cyril Galibern <cyril.galibern@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
from provFs import ProvisioningFs
from rcUtilities import which
from rcZfs import Dataset

class ProvisioningFsZfs(ProvisioningFs):
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
