import resIp as Res
from subprocess import *
from rcUtilitiesSunOS import check_ping
import rcExceptions as ex

class Ip(Res.Ip):
    """
    SunOS ip resource driver.
    """

    def arp_announce(self):
        """
        Noop becauce the arp_announce job is done by SunOS ifconfig
        """
        return

    def check_ping(self, count=1, timeout=2):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, timeout=timeout)

    def startip_cmd(self):
        cmd=['/usr/sbin/ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        cmd = ['/usr/sbin/ifconfig', self.stacked_dev, 'unplumb']
        return self.vcall(cmd)

