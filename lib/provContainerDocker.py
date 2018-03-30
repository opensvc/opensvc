import provisioning

class Prov(provisioning.Prov):
    def is_provisioned(self):
        return False

    def start(self):
        self.r._start()
        self.r.status(resfresh=True)

    def stop(self):
        self.r._stop()
        self.r.status(resfresh=True)

