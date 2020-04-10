"""
This module implements the Windows ip resource driver
"""

import time

import core.exceptions as ex
import utilities.ping

from .. import Ip

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "host"


class IpHost(Ip):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability (%ss)", self.addr, timeout)
        return utilities.ping.check_ping(self.addr, timeout=timeout, count=count)

    def startip_cmd(self):
        #netsh interface ip add address "Local Area Connection" 33.33.33.33 255.255.255.255
        if ":" in self.addr:
            if "." in self.netmask:
                self.log.error("netmask parameter is mandatory for ipv6 adresses")
                raise ex.Error
            cmd = ["netsh", "interface", "ipv6", "add", "address", self.ipdev, self.addr, self.netmask]
        else:
            cmd = ["netsh", "interface", "ipv4", "add", "address", self.ipdev, self.addr, self.netmask]

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # ip activation may still be incomplete
        # wait for activation, to avoid startapp scripts to fail binding their listeners
        for i in range(5, 0, -1):
            if utilities.ping.check_ping(self.addr, timeout=1, count=1):
                return ret, out, err
            time.sleep(1)
        self.log.error("timed out waiting for ip activation")
        raise ex.Error

    def stopip_cmd(self):
        if ":" in self.addr:
            cmd = ["netsh", "interface", "ipv6", "delete", "address", self.ipdev, "addr="+self.addr]
        else:
            cmd = ["netsh", "interface", "ipv4", "delete", "address", self.ipdev, "addr="+self.addr]
        return self.vcall(cmd)

