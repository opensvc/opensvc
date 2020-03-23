import shutil
import os

import provisioning
from rcUtilities import protected_dir

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def is_provisioned(self):
        return os.path.exists(self.r.path)

    def provisioner(self):
        pass

    def unprovisioner(self):
        if not os.path.exists(self.r.path):
            return
        if protected_dir(self.r.path):
            self.r.log.warning("cowardly refuse to purge %s", self.r.path)
        self.r.log.info("purge %s", self.r.path)
        shutil.rmtree(self.r.path)

