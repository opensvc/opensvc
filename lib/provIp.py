import provisioning

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def provisioner(self):
        """
        Provision the ip resource, allocate an ip collector's side, and
        start it.
        """
        self.r.allocate()
        self.r.start()

    def unprovisioner(self):
        """
        Unprovision the ip resource, meaning unplumb and release collector's
        side.
        """
        self.r.stop()
        self.r.release()

