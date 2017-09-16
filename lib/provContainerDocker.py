import provisioning

class Prov(provisioning.Prov):
    def provisioner(self):
        # docker resources are naturally provisioned
        self.r._start()
        self.r.status(refresh=True)
        self.r.svc.sub_set_action("ip", "provision", tags=set([self.r.rid]))

    def unprovisioner(self):
        self.r.svc.sub_set_action("ip", "unprovision", tags=set([self.r.rid]))
        self.r._stop()
        self.r.status(refresh=True)

