import resIp as Res
import rcExceptions as ex
from rcUtilitiesOSF1 import check_ping
from rcUtilities import to_cidr, to_dotted
from svcdict import KEYS

DRIVER_GROUP = "ip"
DRIVER_BASENAME = None
KEYWORDS = Res.KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    Res.adder(svc, s, drv=Ip)

class Ip(Res.Ip):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', '/'.join([self.addr, to_cidr(self.mask)]), 'add']
        else:
            cmd = ['ifconfig', self.ipdev, 'inet', 'alias', self.addr, 'netmask', to_dotted(self.mask)]
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', self.addr, 'delete']
        else:
            cmd = ['ifconfig', self.ipdev, 'inet', '-alias', self.addr]
        return self.vcall(cmd)

