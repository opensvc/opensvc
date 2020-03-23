import os
import shutil

import lock
import provisioning
import rcExceptions as ex
import rcZone

from drivers.resource.container.zone import ContainerZone
from rcGlobalEnv import rcEnv
from rcUtilitiesSunOS import get_solaris_version
from rcUtilities import justcall
from rcZfs import Dataset


SYSIDCFG="/etc/sysidcfg"

class Prov(provisioning.Prov):
    def __init__(self, r):
        """
        """
        provisioning.Prov.__init__(self, r)
        self.log = self.r.log
        self.container_origin = self.r.oget("container_origin")
        self.snapof = self.r.oget("snapof")
        self.clone = self.r.oget("snap")
        self.zonepath = self.r.oget("zonepath")
        if not self.clone:
            self.clone = "rpool/zones/" + self.r.name

    def stop(self):
        self.r._stop()

    def start(self):
        pass

    def sysid_network(self):
        """
         network_interface=l226z1 {primary
          hostname=zone1-32
          ip_address=172.30.5.232
          netmask=255.255.255.0
          protocol_ipv6=no
          default_route=172.30.5.1}
        """
        cf = os.path.join(rcEnv.paths.pathetc, self.r.svc.name+'.conf')
        s = ""

        for r in self.r.svc.get_resources(["ip"]):
            # Add mandatory tags for sol11 zones
            r.tags.add("noaction")
            r.tags.add("noalias")
            r.tags.add("exclusive")
            r.tags.remove("preboot")
            r.tags.remove("postboot")

            try:
                default_route = self.r.svc.conf_get(r.rid, "gateway")
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                # Add nonrouted tag if no gateway provisioning keyword is passed
                self.r.tags.add("nonrouted")
                continue

            try:
                netmask = self.r.svc.oget(r.rid, "netmask")
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                continue

            if s == "":
                s += "network_interface=%s {primary\n"%r.ipdev
                s += " hostname=%s\n"%r.ipname
                s += " ip_address=%s\n"%r.addr
                s += " netmask=%s\n"%netmask
                s += " protocol_ipv6=no\n"
                s += " default_route=%s}\n"%default_route

            # save new service env file
        self.r.svc.set_multi(["%s.tags=%s" % (r.rid, ' '.join(r.tags))])
        return s

    def get_tz(self):
        if "TZ" not in os.environ:
            return "MET"
        tz = os.environ["TZ"]
        tzp = os.path.join(os.sep, "etc", tz)
        if os.path.exists(tzp) and self.osver >= 11:
            p = os.path.realpath(tzp)
            l = p.split('zoneinfo/')
            if len(l) != 2:
                return "MET"
            return l[-1]
        else:
            return tz

    def get_ns(self):
        "return (domain, nameservers) detected from resolv.conf"
        p = os.path.join(os.sep, 'etc', 'resolv.conf')
        domain = None
        search = []
        nameservers = []
        with open(p) as f:
            for line in f.readlines():
                if line.strip().startswith('search'):
                    l = line.split()
                    if len(l) > 1:
                        search = l[1:]
                if line.strip().startswith('domain'):
                    l = line.split()
                    if len(l) > 1:
                        domain = l[1]
                if line.strip().startswith('nameserver'):
                    l = line.split()
                    if len(l) > 1 and l[1] not in nameservers:
                        nameservers.append(l[1])
        return (domain, nameservers, search)

    def create_sysidcfg(self, zone=None):
        self.r.log.info("creating zone sysidcfg file")
        if self.osver >= 11.0:
            self._create_sysidcfg_11(zone)
        else:
            self._create_sysidcfg_10(zone)

    def _create_sysidcfg_11(self, zone=None):
        try:
            domain, nameservers, search = self.get_ns()
            if domain is None and len(search) > 0:
                domain = search[0]
            if domain is None or len(nameservers) == 0:
                name_service="name_service=none"
            else:
                name_service = "name_service=DNS {domain_name=%s name_server=%s search=%s}\n" % (
                  domain,
                  ",".join(nameservers),
                  ",".join(search)
                )

            sysidcfg_dir = os.path.join(self.r.var_d)
            sysidcfg_filename = os.path.join(sysidcfg_dir, 'sysidcfg')
            contents = ""
            contents += "keyboard=US-English\n"
            contents += "system_locale=C\n"
            contents += "timezone=%s\n"%self.get_tz()
            contents += "terminal=vt100\n"
            contents += "timeserver=localhost\n"
            contents += self.sysid_network()
            contents += "root_password=NP\n"
            contents += "security_policy=NONE\n"
            contents += name_service

            try:
                os.makedirs(sysidcfg_dir)
            except:
                pass
            with open(sysidcfg_filename, "w") as sysidcfg_file:
                sysidcfg_file.write(contents)
            os.chdir(sysidcfg_dir)
            self.zonecfg_xml = os.path.join(sysidcfg_dir, "sc_profile.xml")
            try:
                os.unlink(self.zonecfg_xml)
            except:
                pass
            cmd = ['/usr/sbin/js2ai', '-s']
            out, err, ret = justcall(cmd)
            if not os.path.exists(self.zonecfg_xml):
                raise ex.excError("js2ai conversion error")
        except Exception as e:
            self.r.svc.save_exc()
            raise ex.excError("exception from %s: %s during create_sysidcfg file" % (e.__class__.__name__, e.__str__()))

    def _create_sysidcfg_10(self, zone=None):
        try:
            name_service = "name_service=NONE\n"

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
        except Exception as exc:
            raise ex.excError("exception from %s: %s during create_sysidcfg file" % (exc.__class__.__name__, exc.__str__()))

    def test_net_interface(self, intf):
        cmd = ['dladm', 'show-link', intf]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        return False

    def zone_configure_net(self, zone=None):
        if zone is None:
            zone = self.r
        cmds = []
        for r in self.r.svc.get_resources(["ip"]):
            if not self.test_net_interface(r.ipdev):
                raise ex.excError("Missing interface: %s" % r.ipdev)
            cmds.append("add net ; set physical=%s ; end" % r.ipdev)
        for cmd in cmds:
            zone.zonecfg([cmd])

    def zone_configure(self, zone=None):
        """
            configure zone, if zone is None, configure self.r
        """
        if zone is None:
            zone = self.r

        if self.osver >= 11.0 and self.container_origin:
            cmd = "create -t " + self.container_origin
        else:
            cmd = "create"

        cmd += "; set zonepath=" + zone.zonepath

        if zone.state is None:
            zone.zonecfg([cmd])
            if zone.state != "configured":
                raise ex.excError("zone %s is not configured" % zone.name)

        if self.osver >= 11.0:
            try:
                self.zone_configure_net(zone)
            except:
                zone.zonecfg(["delete", "-F"])
                raise

    def create_zone2clone(self):
        if os.path.exists(self.r.zonepath):
            try:
                os.chmod(self.r.zonepath, 0o0700)
            except:
                pass
        if self.osver >= 11.0:
            self._create_zone2clone_11()
        else:
            self._create_zone2clone_10()

    def _create_zone2clone_11(self):
        zonename = self.container_origin
        zone2clone = ContainerZone(rid="container#skelzone", name=zonename)
        zone2clone.log = self.r.log
        if zone2clone.state == "installed":
            return
        self.zone_configure(zone=zone2clone)
        if zone2clone.state != "configured":
            raise(ex.excError("zone %s is not configured" % (zonename)))
        self.create_sysidcfg(zone2clone)
        #zone2clone.zoneadm("clone", ['-c', self.zonecfg_xml, self.container_origin])
        zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(ex.excError("zone %s is not installed" % (zonename)))
        brand = zone2clone.brand
        if brand == "native":
            zone2clone.boot_and_wait_reboot()
        elif brand == "ipkg":
            zone2clone.zone_boot()
        else:
            raise(ex.excError("zone brand: %s not yet implemented" % (brand)))
        zone2clone.wait_multi_user()
        zone2clone.stop()
        if zone2clone.state != "installed":
            raise(ex.excError("zone %s is not installed" % (zonename)))

    def _create_zone2clone_10(self):
        """verify if self.container_origin zone is installed
        else configure container_origin if required
        then install container_origin if required
        """
        zonename = self.container_origin
        zone2clone = ContainerZone(rid="container#skelzone", name=zonename)
        zone2clone.log = self.r.log
        if zone2clone.state == "installed":
            return
        self.zone_configure(zone=zone2clone)
        if zone2clone.state != "configured":
            raise(ex.excError("zone %s is not configured" % (zonename)))
        zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(ex.excError("zone %s is not installed" % (zonename)))
        self.create_sysidcfg(zone2clone)
        brand = zone2clone.brand
        if brand == "native":
            zone2clone.boot_and_wait_reboot()
        elif brand == "ipkg":
            zone2clone.zone_boot()
        else:
            raise(ex.excError("zone brand: %s not yet implemented" % (brand)))
        zone2clone.wait_multi_user()
        zone2clone.stop()
        if zone2clone.state != "installed":
            raise(ex.excError("zone %s is not installed" % (zonename)))

    def create_cloned_zone(self):
        zone = self.r
        if zone.state == "running":
            self.log.info("zone %s already running"%zone.name)
            return
        if zone.state == "configured":
            if self.osver >= 11.0:
                self._create_cloned_zone_11(zone)
            else:
                self._create_cloned_zone_10(zone)
        if zone.state != "installed":
            raise(ex.excError("zone %s is not installed" % (zone.name)))

    def _create_cloned_zone_11(self, zone):
        zone.zoneadm("clone", ['-c', self.zonecfg_xml, self.container_origin])

    def _create_cloned_zone_10(self, zone):
        zone.zoneadm("clone", [self.container_origin])

    def create_zonepath(self):
        """create zonepath dataset from clone of snapshot of self.snapof
        snapshot for self.snapof will be created
        then cloned to self.clone
        """
        zonename = self.r.name
        source_ds = Dataset(self.snapof)
        if source_ds.exists(type="filesystem") is False:
            raise(ex.excError("source dataset doesn't exist " + self.snapof))
        snapshot = source_ds.snapshot(zonename)
        snapshot.clone(self.clone, ['-o', 'mountpoint=' + self.r.zonepath])

    def provisioner(self, need_boot=True):
        """provision zone
        - configure zone
        - if snapof and zone brand is native
           then create zonepath from snapshot of snapof
           then attach zone
        - if snapof and zone brand is ipkg
           then try to detect zone associated with snapof
           then define container_origin
        - if container_origin
           then clone  container_origin
        - create sysidcfg
        - if need_boot boot and wait multiuser
        """
        self.osver = get_solaris_version()
        self.zone_configure()

        if self.osver >= 11:
            self.create_sysidcfg(self.r)
        else:
            if self.snapof is not None and self.r.brand == 'native':
                self.create_zonepath()
                self.r.zoneadm("attach", ["-F"])
            elif self.snapof is not None and self.r.brand == 'ipkg':
                zones = rcZone.Zones()
                src_dataset = Dataset(self.snapof)
                zonepath = src_dataset.getprop('mountpoint')
                self.container_origin = zones.zonename_from_zonepath(zonepath).zonename
                self.log.info("source zone is %s (detected from snapof %s)" % (self.container_origin, self.snapof))

        if self.container_origin is not None:
            lockname='create_zone2clone-' + self.container_origin
            lockfile = os.path.join(rcEnv.paths.pathlock, lockname)
            self.log.info("wait get lock %s"%(lockname))
            try:
                lockfd = lock.lock(timeout=1200, delay=5, lockfile=lockfile)
            except:
                raise(ex.excError("failure in get lock %s"%(lockname)))
            try:
                self.create_zone2clone()
            except:
                lock.unlock(lockfd)
                raise
            lock.unlock(lockfd)
            self.create_cloned_zone()

        if self.osver < 11:
            self.create_sysidcfg(self.r)

        if need_boot is True:
            self.r.zone_boot()
            self.r.wait_multi_user()

        self.r.log.info("provisioned")
        return True
