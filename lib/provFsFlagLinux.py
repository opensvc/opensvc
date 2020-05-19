import provisioning

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def is_provisioned(self):
        flag = self.r.is_provisioned_flag()
        if flag is None:
            return False
        return flag

    def provisioner(self):
        pass

    def unprovisioner(self):
        pass

