from subprocess import *

import rcExceptions as ex
import utilities.ping

from . import Ip as ParentIp
from svcBuilder import init_kwargs

def adder(svc, s, drv=None):
    """
    Add a resource instance to the object, parsing parameters
    from a configuration section dictionnary.
    """
    _drv = drv or Ip
    kwargs = init_kwargs(svc, s)
    kwargs["expose"] = svc.oget(s, "expose")
    kwargs["check_carrier"] = svc.oget(s, "check_carrier")
    kwargs["alias"] = svc.oget(s, "alias")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    kwargs["ipname"] = svc.oget(s, "ipname")
    kwargs["mask"] = svc.oget(s, "netmask")
    kwargs["gateway"] = svc.oget(s, "gateway")
    zone = svc.oget(s, "zone")

    if zone is not None and drv is None:
        kwargs["zone"] = zone
        ip = __import__("resIpZone")
        r = ip.Ip(**kwargs)
    else:
        r = _drv(**kwargs)
    svc += r


class Ip(ParentIp):
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
        return utilities.ping.check_ping(self.addr, timeout=timeout)

    def startip_cmd(self):
        cmd=['/usr/sbin/ifconfig', self.stacked_dev, 'plumb', self.addr, \
            'netmask', '+', 'broadcast', '+', 'up']
        return self.vcall(cmd)

    def stopip_cmd(self):
        cmd = ['/usr/sbin/ifconfig', self.stacked_dev, 'unplumb']
        return self.vcall(cmd)

