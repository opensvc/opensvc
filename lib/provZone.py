from provisioning import Provisioning
from rcGlobalEnv import rcEnv
from rcUtilities import which,vcall,justcall
import os
import rcExceptions as ex
import rcZfs
import resContainerZone

SYSIDCFG="/etc/sysidcfg"

class ProvisioningZone(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

        self.section = r.svc.config.defaults()

        if 'container_origin' in self.section:
            self.container_origin = self.section['container_origin']
        else:
            self.container_origin = "skelzone"

        if 'snapof' in self.section:
            self.snapof = self.section['snapof']
        else:
            self.snapof = None

        if 'snap' in self.section:
            self.snap = self.section['snap']
        else:
            self.snap = None

        if 'virtinst' in self.section:
            self.virtinst = self.section['virtinst']
        else:
            self.virtinst = None

        if 'zonepath' in self.section:
            self.zonepath = self.section['zonepath']
        else:
            self.zonepath = None

    def get_ns(self):
        p = os.path.join(os.sep, 'etc', 'resolv.conf')
        domain = None
        nameservers = set()
        with open(p) as f:
            for line in f.readlines():
                if 'domain' in line:
                    l = line.split()
                    if len(l) > 1:
                        domain = l[1]
                if 'nameserver' in line:
                    l = line.split()
                    if len(l) > 1:
                        nameservers.add(l[1])
        if len(nameservers) > 0:
            return (domain, nameservers)
        else:
            return None

    def create_sysidcfg(self):
        try:
            zone = self.r
            self.r.log.info("creating zone sysidcfg file")
            ns = self.get_ns()
            if ns is None:
                name_service = "name_service=NONE\n"
            else:
                (domain, nameservers) = ns
                name_service = "name_service=DNS {domain_name=%s name_server=%s}\n" % (domain, ",".join(nameservers))

            sysidcfg_filename = zone.zonepath + "/root" + SYSIDCFG
            sysidcfg_file = open(sysidcfg_filename, "w" )
            contents = ""
            contents += "system_locale=C\n"
            contents += "timezone=MET\n"
            contents += "terminal=vt100\n"
            contents += "timeserver=localhost\n"
            contents += "security_policy=NONE\n"
            contents += "root_password=NP\n"
            contents += "nfs4_domain=dynamic\n"
            contents += "network_interface=NONE {hostname=%(zonename)s}\n" % {"zonename":zone.name}
            contents += name_service

            sysidcfg_file.write(contents)
            sysidcfg_file.close()
        except Exception,e:
            self.r.save_exc()
            raise(e("exception %s during create_sysidcfg file" % (e.__str__())))

    def create_zone2clone(self):
        zonename = self.container_origin
        zone2clone = resContainerZone.Zone(name=zonename)
        zone2clone.log = self.r.log
        if zone2clone.state is None:
            zone2clone.zonecfg("create; set zonepath=" + zone2clone.zonepath)
        if zone2clone.state == "configured":
            zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(Exception("zone %s is not installed" % (zonename)))

    def create_cloned_zone(self):
        zone = self.r
        if zone.state is None:
            zone.zonecfg("create; set zonepath=" + zone.zonepath)
            if zone.state != "configured":
                raise(Exception("zone %s is not configured" % (zone.name)))
        if zone.state == "configured":
            zone.zoneadm("clone", [self.container_origin])
        if zone.state != "installed":
            raise(Exception("zone %s is not installed" % (zone.name)))
        
    def provisioner(self):
        try:
            self.create_zone2clone()
            self.create_cloned_zone()
            self.create_sysidcfg()
            self.r.boot_and_wait_reboot()
            self.r.log.info("provisioned")
            return True
        except Exception, e:
            self.r.log.error("Exception raised: " + e.__str__())
            self.r.save_exc()
            raise(e)
