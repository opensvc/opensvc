import time
import resIpSunOS as Res
import rcExceptions as ex
from subprocess import *
from rcGlobalEnv import rcEnv
from rcUtilities import which, to_cidr
rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)

class Ip(Res.Ip):
    def __init__(self,
                 rid=None,
                 ipdev=None,
                 ipname=None,
                 mask=None,
                 gateway=None,
                 ipdevExt="v4",
                 **kwargs):
        self.ipdevExt = ipdevExt
        Res.Ip.__init__(self,
                        rid=rid,
                        ipdev=ipdev,
                        ipname=ipname,
                        mask=mask,
                        gateway=gateway,
                        **kwargs)
        self.label = self.label + "/" + self.ipdevExt
        self.type = "ip"
        if not which('ipadm'):
            raise ex.excInitError("crossbow ips are not supported on this system")
        if 'noalias' not in self.tags:
            self.tags.add('noalias')

    def stopip_cmd(self):
        ret, out, err = (0, '', '')
        if self.gateway is not None:
            cmd=['route', '-q', 'delete', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        cmd=['ipadm', 'delete-addr', self.stacked_dev+'/'+self.ipdevExt]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        cmd = ['ipadm', 'show-addr', '-p', '-o', 'state', self.stacked_dev ]
        p = Popen(cmd, stdin=None, stdout=PIPE, stderr=PIPE, close_fds=True)
        _out = p.communicate()[0].strip().split("\n")
        if len(_out) > 0:
            self.log.info("skip delete-ip because addrs still use the ip")
            return ret, out, err
        cmd=['ipadm', 'delete-ip', self.stacked_dev]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        return ret, out, err

    def wait_net_smf(self, max_wait=30):
        r = 0
        prev_s = None
        while True:
            s = self.get_smf_status("network/routing-setup")
            if s == "online":
                break
            if s != prev_s or prev_s is None:
                self.log.info("waiting for network/routing-setup online state. current state: %s" % s)
            prev_s = s
            r += 1
            if r > max_wait:
                self.log.error("timeout waiting for network/routing-setup online state")
                break
            time.sleep(1)

    def get_smf_status(self, fmri):
        cmd = ["/usr/bin/svcs", "-H", "-o", "state", fmri]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return "undef"
        return out.strip()

    def startip_cmd(self):
        self.wait_net_smf()
        ret, out, err = (0, '', '')
        cmd = ['ipadm', 'show-if', '-p', '-o', 'state', self.stacked_dev]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        _out = p.communicate()[0].strip().split("\n")
        if len(_out) == 0:
            cmd=['ipadm', 'create-ip', '-t', self.stacked_dev ]
            r, o, e = self.vcall(cmd)
        cmd=['ipadm', 'create-addr', '-t', '-T', 'static', '-a', self.addr+"/"+to_cidr(self.mask), self.stacked_dev+'/'+self.ipdevExt]
        r, o, e = self.vcall(cmd)
        if r != 0:
            cmd=['ipadm', 'show-if' ]
            self.vcall(cmd)
            raise ex.excError("Interface %s is not up. ipadm cannot create-addr over it. Retrying..." % self.stacked_dev)
        ret += r
        out += o
        err += e
        if self.gateway is not None:
            cmd=['route', '-q', 'add', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        return ret, out, err

    def allow_start(self):
        if 'noaction' in self.tags:
            raise ex.IpNoActions(self.addr)
        retry = 10
        interval = 3
        import time
        ok = False
        if self.is_up() is True:
            self.log.info("%s is already up on %s" % (self.addr, self.ipdev))
            raise ex.IpAlreadyUp(self.addr)
        if not hasattr(self, 'abort_start_done') and 'nonrouted' not in self.tags and self.check_ping():
            self.log.error("%s is already up on another host" % (self.addr))
            raise ex.IpConflict(self.addr)

