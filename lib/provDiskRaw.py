import provisioning
import rcExceptions as ex

class Prov(provisioning.Prov):
    def is_provisioned(self):
        return True

