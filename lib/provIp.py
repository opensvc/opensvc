import provisioning
import rcExceptions as ex
from rcUtilities import lazy

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def is_provisioned(self):
        try:
            self.r.conf_get("ipname")
            return True
        except ex.OptNotFound:
            return False

    def start(self):
        pass

    @lazy
    def kw_provisioner(self):
        try:
            provisioner = self.r.conf_get("provisioner")
        except ex.OptNotFound as exc:
            provisioner = exc.default
        return provisioner

    def provisioner(self):
        """
        Provision the ip resource, allocate an ip collector's side, and
        start it.
        """
        if self.kw_provisioner != "collector":
            return
        self.r.allocate()

    def unprovisioner(self):
        """
        Unprovision the ip resource, meaning unplumb and release collector's
        side.
        """
        if self.kw_provisioner != "collector":
            return
        self.r.release()

