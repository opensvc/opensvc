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
        return

    def startip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', 'alias', '/'.join([self.addr, to_cidr(self.netmask)])]
        else:
            cmd = ['ifconfig', self.ipdev, self.addr, 'netmask', to_dotted(self.netmask), 'alias']
        return self.vcall(cmd)

    def stopip_cmd(self):
        if ':' in self.addr:
            cmd = ['ifconfig', self.ipdev, 'inet6', self.addr, 'delete']
        else:
            cmd = ['ifconfig', self.ipdev, self.addr, 'delete']
        return self.vcall(cmd)

