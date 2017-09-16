import provisioning

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def provisioner(self):
        self.r.create()

    def unprovisioner(self):
        pass

