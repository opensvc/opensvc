from . import \
    Ip as ParentIp, \
    adder as parent_adder
import rcExceptions as ex
from rcUtilitiesAIX import check_ping
from rcUtilities import to_cidr, to_dotted

def adder(svc, s):
    parent_adder(svc, s, drv=Ip)

class Ip(ParentIp):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', 'alias', '/'.join([self.addr, to_cidr(self.mask)])]
        else:
            cmd = ['ifconfig', self.ipdev, self.addr, 'netmask', to_dotted(self.mask), 'alias']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', self.addr, 'delete']
        else:
            cmd = ['ifconfig', self.ipdev, self.addr, 'delete']
        return self.vcall(cmd)

