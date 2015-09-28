#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import resIp
import os
import rcStatus
from rcGlobalEnv import rcEnv
import rcExceptions as ex
from resAmazon import Amazon
from rcUtilities import getaddr

rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(resIp.Ip, Amazon):
    def __init__(self,
                 rid=None,
                 ipName=None,
                 ipDev=None,
                 eip=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        resIp.Ip.__init__(self,
                          rid=rid,
                          ipName=ipName,
                          ipDev=ipDev,
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)
        self.label = "ec2 ip %s"%str(ipName)

        self.eip = eip
        self.instance_id = None
        self.instance_data = None
        
    def get_eip(self):
        ip = getaddr(self.eip, True)
        data = self.aws(["ec2", "describe-addresses", "--public-ips", self.eip], verbose=False)
        try:
            addr = data["Addresses"][0]
        except:
            addr = None
        return addr

    def get_instance_id(self):
        if self.instance_id is not None:
            return self.instance_id
        import httplib
        c = httplib.HTTPConnection("instance-data")
        c.request("GET", "/latest/meta-data/instance-id")
        self.instance_id = c.getresponse().read()
        return self.instance_id

    def get_instance_data(self, refresh=False):
        if self.instance_data is not None and not refresh:
            return self.instance_data
        data = self.aws(["ec2", "describe-instances", "--instance-ids", self.get_instance_id()], verbose=False)
        try:
            self.instance_data = data["Reservations"][0]["Instances"][0]
        except Exception as e:
            self.instance_data = None
        return self.instance_data 

    def get_instance_private_addresses(self):
        self.getaddr()
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        ips = []
        for eni in instance_data["NetworkInterfaces"]:
            ips += [ pa["PrivateIpAddress"] for pa in eni["PrivateIpAddresses"] ]
        return ips

    def get_network_interface(self):
        ifconfig = rcIfconfig.ifconfig()
        intf = ifconfig.interface(self.ipDev)
        ips = set(intf.ipaddr + intf.ip6addr)
        instance_data = self.get_instance_data(refresh=True)
        if instance_data is None:
            raise ex.excError("can't find instance data")

        for eni in instance_data["NetworkInterfaces"]:
            _ips = set([ pa["PrivateIpAddress"] for pa in eni["PrivateIpAddresses"] ])
            if len(ips & _ips) > 0:
                return eni["NetworkInterfaceId"]

    def is_up(self):
        """Returns True if ip is associated with this node
        """
        ips = self.get_instance_private_addresses()
        if self.addr not in ips:
            return False
        return True

    def _status(self, verbose=False):
        try:
            s = self.is_up()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if rcEnv.nodename in self.always_on:
            if s:
                return rcStatus.STDBY_UP
            else:
                return rcStatus.STDBY_DOWN
        else:
            if s:
                return rcStatus.UP
            else:
                return rcStatus.DOWN

    def check_ping(self, count=1, timeout=5):
        pass

    def start_assign(self):
        if self.is_up():
            self.log.info("ec2 ip %s is already assigned to this node" % self.addr)
            return
        eni = self.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipDev)
        data = self.aws([
         "ec2", "assign-private-ip-addresses",
         "--network-interface-id", eni,
         "--private-ip-address", self.addr,
         "--allow-reassignment"
        ])
        self.can_rollback = True

    def start_associate(self):
        if self.eip is None:
            return

        eip = self.get_eip()
        if eip is None:
            raise ex.excError("eip %s is not allocated" % self.eip)
        if "PrivateIpAddress" in eip and eip["PrivateIpAddress"] == self.addr:
            self.log.info("eip %s is already associated to private ip %s" % (eip["PublicIp"], self.addr))
            return
        data = self.aws([
          "ec2", "associate-address",
          "--allocation-id", eip["AllocationId"],
          "--private-ip-address", self.addr,
          "--instance-id", self.get_instance_id()
        ])


    def start(self):
        self.start_assign()
        self.start_associate()

    def stop(self):
        if not self.is_up():
            self.log.info("ec2 ip %s is already unassigned from this node" % self.addr)
            return
        eni = self.get_network_interface()
        if eni is None:
            raise ex.excError("could not find ec2 network interface for %s" % self.ipDev)
        data = self.aws([
         "ec2", "unassign-private-ip-addresses",
         "--network-interface-id", eni,
         "--private-ip-address", self.addr
        ])

    def shutdown(self):
        pass

