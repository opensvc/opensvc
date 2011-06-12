#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2011 Cyril Galibern <cyril.galibern@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#

import os
import resContainerZone
from provisioning import Provisioning
from rcZfs import Dataset
import rcZone

SYSIDCFG="/etc/sysidcfg"

class ProvisioningZone(Provisioning):
    def __init__(self, r):
        """
        """
        Provisioning.__init__(self, r)
        self.log = self.r.log
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
            self.clone = self.section['snap']
        else:
            self.clone = "rpool/zones/" + r.name

        if 'zonepath' in self.section:
            self.zonepath = self.section['zonepath']
        else:
            self.zonepath = None

    def get_ns(self):
        "return (domain, nameservers) detected from resolv.conf"
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

    def create_sysidcfg(self, zone=None):
        try:
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

    def zone_configure(self, zone=None):
        "configure zone, if zone is None, configure self.r"
        if zone is None:
            zone = self.r
        if zone.state is None:
            zone.zonecfg("create; set zonepath=" + zone.zonepath)
            if zone.state != "configured":
                raise(Exception("zone %s is not configured" % (zone.name)))

    def create_zone2clone(self):
        """verify if self.container_origin zone is installed
        else configure container_origin if required
        then install container_origin if required
        """
        zonename = self.container_origin
        zone2clone = resContainerZone.Zone(name=zonename)
        zone2clone.log = self.r.log
        if zone2clone.state == "installed":
            return
        self.zone_configure(zone=zone2clone)
        if zone2clone.state != "configured":
            raise(Exception("zone %s is not configured" % (zonename)))
        zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(Exception("zone %s is not installed" % (zonename)))
        self.create_sysidcfg(zone2clone)
        brand = zone2clone.brand
        if brand == "native":
            zone2clone.boot_and_wait_reboot()
        elif brand == "ipkg":
            zone2clone.boot()
        else:
            raise(Exception("don't known how to manage zone brand: %s" % (brand)))
        zone2clone.wait_multi_user()
        zone2clone.stop()
        zone2clone.zone_refresh()
        if zone2clone.state != "installed":
            raise(Exception("zone %s is not installed" % (zonename)))

    def create_cloned_zone(self):
        "clone zone self.r from container_origin"
        zone = self.r
        if zone.state == "configured":
            zone.zoneadm("clone", [self.container_origin])
        if zone.state != "installed":
            raise(Exception("zone %s is not installed" % (zone.name)))

    def create_zonepath(self):
        """create zonepath dataset from clone of snapshot of self.snapof
        snapshot for self.snapof will be created
        then cloned to self.clone
        """
        zonename = self.r.name
        source_ds = Dataset(self.snapof)
        if source_ds.exists(type="filesystem") is False:
            raise(Exception("source dataset doesn't exist " + self.snapof))
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
        try:
            self.zone_configure()
            if self.snapof is not None and self.r.brand == 'native':
                self.create_zonepath()
                self.r.zoneadm("attach", ["-F"])
            elif self.snapof is not None and self.r.brand == 'ipkg':
                zones = rcZone.Zones()
                src_dataset = Dataset(self.snapof)
                zonepath = src_dataset.getprop('mountpoint')
                self.container_origin = zones.zonename_from_zonepath(zonepath).zonename
                self.log.info("source zone is %s (detected from with snapof %s)" % (self.container_origin, self.snapof))
            if self.container_origin is not None:
                self.create_zone2clone()
                self.create_cloned_zone()

            self.create_sysidcfg(self.r)

            if need_boot is True:
                self.r.boot()
                self.r.wait_multi_user()

            self.r.log.info("provisioned")
            return True
        except Exception, e:
            self.r.log.error("Exception raised: " + e.__str__())
            self.r.save_exc()
            raise(e)
