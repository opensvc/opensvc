import exceptions as ex
import utilities.ping

from . import Ip as ParentIp, adder as parent_adder
from rcUtilities import to_cidr

def adder(svc, s):
    parent_adder(svc, s, drv=Ip)

class Ip(ParentIp):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability"%self.addr)
        return utilities.ping.check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', '/'.join([self.addr, to_cidr(self.mask)]), 'add']
        else:
            cmd = ['ifconfig', self.ipdev, 'inet', self.addr, 'netmask', '0xffffffff', 'add']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', self.addr, 'delete']
        else:
            cmd = ['ifconfig', self.ipdev, 'inet', self.addr, 'delete']
        return self.vcall(cmd)

