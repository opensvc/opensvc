class Prov(object):
    def __init__(self, r):
        self.r = r

    def start(self):
        self.r.start()

    def stop(self):
        self.r.stop()

    def is_provisioned(self):
        return

    def unprovisioner(self):
        pass

    def provisioner(self):
        pass

