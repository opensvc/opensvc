from provisioning import Provisioning
import rcExceptions as ex

class ProvisioningIp(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provisioner(self):
        self.provisioner_private()
        self.provisioner_public()
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def provisioner_private(self):
        if self.r.ipName != "<allocate>":
            self.r.log.info("private ip already provisioned")
            return

        eni = self.r.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipDev)

        ips1 = set(self.r.get_instance_private_addresses())
        data = self.r.aws([
          "ec2", "assign-private-ip-addresses",
          "--network-interface-id", eni,
          "--secondary-private-ip-address-count", "1"
        ])
        ips2 = set(self.r.get_instance_private_addresses())
        new_ip = list(ips2 - ips1)[0]

        self.r.svc.config.set(self.r.rid, "ipname", new_ip)
        self.r.svc.write_config()

    def provisioner_public(self):
        if self.r.eip != "<allocate>":
            self.r.log.info("public ip already provisioned")
            return

        data = self.r.aws([
          "ec2", "allocate-address",
          "--domain", "vpc",
        ])

        self.r.svc.config.set(self.r.rid, "eip", data["PublicIp"])
        self.r.svc.write_config()

