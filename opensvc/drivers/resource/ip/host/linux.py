import core.exceptions as ex
import utilities.ping

from .. import Ip
from env import Env
from utilities.net.converters import to_cidr, to_dotted
from utilities.proc import which

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "host"

class IpHost(Ip):
    def check_ping(self, count=1, timeout=5):
        self.log.info("checking %s availability (%ss)", self.addr, timeout)
        return utilities.ping.check_ping(self.addr, timeout=timeout, count=count)

    def start_link(self):
        if which(Env.syspaths.ip):
           cmd = [Env.syspaths.ip, 'link', 'set', 'dev', self.ipdev, 'up']
        else:
           cmd = ['ifconfig', self.ipdev, 'up']
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

    def startip_cmd(self):
        if which("ifconfig") and self.alias:
            if ':' in self.addr:
                cmd = ['ifconfig', self.ipdev, 'inet6', 'add', '/'.join([self.addr, to_cidr(self.netmask)])]
            else:
                cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', to_dotted(self.netmask), 'up']
        else:
            cmd = [Env.syspaths.ip, "addr", "add", '/'.join([self.addr, to_cidr(self.netmask)]), "dev", self.ipdev]

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # ip activation may still be incomplete
        # wait for activation, to avoid startapp scripts to fail binding their listeners
        for i in range(5, 0, -1):
            if utilities.ping.check_ping(self.addr, timeout=1, count=1):
                return ret, out, err
        self.log.error("timed out waiting for ip activation")
        raise ex.Error

    def stopip_cmd(self):
        if which("ifconfig") and self.alias:
            if ':' in self.addr:
                cmd = ['ifconfig', self.ipdev, 'inet6', 'del', '/'.join([self.addr, to_cidr(self.netmask)])]
            else:
                if self.stacked_dev is None:
                    return 1, "", "no stacked dev found"
                if ":" in self.stacked_dev:
                    cmd = ['ifconfig', self.stacked_dev, 'down']
                else:
                    cmd = [Env.syspaths.ip, "addr", "del", '/'.join([self.addr, to_cidr(self.netmask)]), "dev", self.ipdev]
        else:
            cmd = [Env.syspaths.ip, "addr", "del", '/'.join([self.addr, to_cidr(self.netmask)]), "dev", self.ipdev]
        return self.vcall(cmd)

