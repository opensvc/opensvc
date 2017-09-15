from provisioning import Provisioning

class Prov(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)
    
    def provisioner(self):
        self.r.create()

    def unprovisioner(self):
        pass

