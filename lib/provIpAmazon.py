from provisioning import Provisioning
import rcExceptions as ex

class ProvisioningIp(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provisioner(self):
        self.provisioner_private()
        self.provisioner_public()
        self.provisioner_docker_ip()
        self.cascade_allocation()
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def cascade_allocation(self):
        if not self.r.svc.config.has_option(self.r.rid, "cascade_allocation"):
            return
        cascade = self.r.svc.config.get(self.r.rid, "cascade_allocation").split()
        need_write = False
        for e in cascade:
            try:
                rid, param = e.split(".")
            except:
                self.r.log.warning("misformatted cascade entry: %s (expected <rid>.<param>[@<scope>])" % e)
                continue
            if not self.r.svc.config.has_section(rid):
                self.r.log.warning("misformatted cascade entry: %s (rid does not exist)" % e)
                continue
            need_write = True
            self.r.log.info("cascade %s to %s" % (self.r.ipname, e))
            self.r.svc.config.set(rid, param, self.r.ipname)
            self.r.svc.resources_by_id[rid].ipname = self.r.svc.conf_get_string_scope(rid, param)
            self.r.svc.resources_by_id[rid].addr = self.r.svc.resources_by_id[rid].ipname
        if need_write:
            self.r.svc.write_config()

    def provisioner_docker_ip(self):
        if not self.r.svc.config.has_option(self.r.rid, "docker_daemon_ip"):
            return
        if not self.r.svc.config.get(self.r.rid, "docker_daemon_ip"):
            return
        try:
            args = self.r.svc.config.get("DEFAULT", "docker_daemon_args")
        except:
            args = ""
        args += " --ip "+self.r.ipname
        self.r.svc.config.set("DEFAULT", "docker_daemon_args", args)
        self.r.svc.write_config()
        for r in self.r.svc.get_resources("container.docker"):
            # reload docker dameon args
            r.on_add()

    def provisioner_private(self):
        if self.r.ipname != "<allocate>":
            self.r.log.info("private ip already provisioned")
            return

        eni = self.r.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipdev)

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
        self.r.ipname = new_ip

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
        self.r.eip = data["PublicIp"]

