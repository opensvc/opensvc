import resIp as Res
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilitiesLinux import check_ping
from rcUtilities import which, to_cidr, to_dotted

def adder(svc, s):
    Res.adder(svc, s, drv=Ip)

class Ip(Res.Ip):
    def check_ping(self, timeout=5, count=1):
        self.log.info("checking %s availability"%self.addr)
        return check_ping(self.addr, timeout=timeout, count=count)

    def start_link(self):
        if which(rcEnv.syspaths.ip):
           cmd = [rcEnv.syspaths.ip, 'link', 'set', 'dev', self.ipdev, 'up']
        else:
           cmd = ['ifconfig', self.ipdev, 'up']
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

    def startip_cmd(self):
        if which("ifconfig") and self.alias:
            if ':' in self.addr:
                cmd = ['ifconfig', self.ipdev, 'inet6', 'add', '/'.join([self.addr, to_cidr(self.mask)])]
            else:
                cmd = ['ifconfig', self.stacked_dev, self.addr, 'netmask', to_dotted(self.mask), 'up']
        else:
            cmd = [rcEnv.syspaths.ip, "addr", "add", '/'.join([self.addr, to_cidr(self.mask)]), "dev", self.ipdev]

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return ret, out, err

        # ip activation may still be incomplete
        # wait for activation, to avoid startapp scripts to fail binding their listeners
        for i in range(5, 0, -1):
            if check_ping(self.addr, timeout=1, count=1):
                return ret, out, err
        self.log.error("timed out waiting for ip activation")
        raise ex.excError

    def stopip_cmd(self):
        if which("ifconfig") and self.alias:
            if ':' in self.addr:
                cmd = ['ifconfig', self.ipdev, 'inet6', 'del', '/'.join([self.addr, to_cidr(self.mask)])]
            else:
                if self.stacked_dev is None:
                    return 1, "", "no stacked dev found"
                if ":" in self.stacked_dev:
                    cmd = ['ifconfig', self.stacked_dev, 'down']
                else:
                    cmd = [rcEnv.syspaths.ip, "addr", "del", '/'.join([self.addr, to_cidr(self.mask)]), "dev", self.ipdev]
        else:
            cmd = [rcEnv.syspaths.ip, "addr", "del", '/'.join([self.addr, to_cidr(self.mask)]), "dev", self.ipdev]
        return self.vcall(cmd)

