import core.exceptions as ex
import utilities.ping

from .. import Ip
from utilities.net.converters import to_cidr, to_dotted

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "host"

class IpHost(Ip):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability (%ss)", self.addr, timeout)
        return utilities.ping.check_ping(self.addr, count=count, timeout=timeout)

    def arp_announce(self):
        """
        Noop. The arp announce is already done by HP-UX ifconfig...
        """
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', 'up']
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.Error
            cmd = ['ifconfig', self.stacked_dev, 'inet6', self.addr+'/'+to_cidr(self.netmask), 'up']
        else:
            cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', to_dotted(self.netmask), 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.stacked_dev, "inet6", "::"]
        else:
            cmd = ['ifconfig', self.stacked_dev, "0.0.0.0"]
        return self.vcall(cmd)

