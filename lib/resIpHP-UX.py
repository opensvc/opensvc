import resIp as Res
u = __import__('rcUtilitiesHP-UX')
import rcExceptions as ex

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
        return u.check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        """ arp_announce job is done by HP-UX ifconfig... """
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', 'up']
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.excError
            cmd = ['ifconfig', self.stacked_dev, 'inet6', self.addr+'/'+to_cidr(self.mask), 'up']
        else:
            cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', to_dotted(self.mask), 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.stacked_dev, "inet6", "::"]
        else:
            cmd = ['ifconfig', self.stacked_dev, "0.0.0.0"]
        return self.vcall(cmd)

