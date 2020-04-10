import utilities.ping

from .. import Ip

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "host"

class IpHost(Ip):
    """
    SunOS ip resource driver.
    """

    def arp_announce(self):
        """
        Noop becauce the arp_announce job is done by SunOS ifconfig
        """
        return

    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability (%ss)", self.addr, timeout)
        return utilities.ping.check_ping(self.addr, timeout=timeout)

    def startip_cmd(self):
        cmd = [
            "/usr/sbin/ifconfig", self.stacked_dev,
            "plumb", self.addr,
            "netmask", "+", "broadcast", "+", "up",
        ]
        return self.vcall(cmd)

    def stopip_cmd(self):
        cmd = ["/usr/sbin/ifconfig", self.stacked_dev, "unplumb"]
        return self.vcall(cmd)

