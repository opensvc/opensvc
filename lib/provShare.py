import provisioning
import rcExceptions as ex

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)
    
    def start(self):
        pass

