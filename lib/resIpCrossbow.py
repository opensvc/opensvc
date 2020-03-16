import time
import resIpSunOS as Res
import rcExceptions as ex
from subprocess import *
from rcGlobalEnv import rcEnv
from rcUtilities import which, to_cidr, justcall
from svcBuilder import init_kwargs

rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)


def adder(svc, s):
    """
    Add a resource instance to the object, parsing parameters
    from a configuration section dictionnary.
    """
    zone = svc.oget(s, "zone")
    if zone is not None:
        svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s" % s)
        return
    kwargs = init_kwargs(svc, s)
    kwargs["expose"] = svc.oget(s, "expose")
    kwargs["check_carrier"] = svc.oget(s, "check_carrier")
    kwargs["alias"] = svc.oget(s, "alias")
    kwargs["ipdev"] = svc.oget(s, "ipdev")
    kwargs["wait_dns"] = svc.oget(s, "wait_dns")
    kwargs["ipdevExt"] = svc.oget(s, "ipdevext")
    kwargs["ipname"] = svc.oget(s, "ipname")
    kwargs["mask"] = svc.oget(s, "netmask")
    kwargs["gateway"] = svc.oget(s, "gateway")
    r = Ip(**kwargs)
    svc += r


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
        if 'noalias' not in self.tags:
            self.tags.add('noalias')

    def set_label(self):
        """
        Set the resource label property.
        """
        try:
             self.get_mask()
        except ex.excError:
             pass
        try:
            self.getaddr()
            addr = self.addr
        except ex.excError:
            addr = self.ipname
        self.label = "%s/%s %s/%s" % (addr, to_cidr(self.mask), self.ipdev, self.ipdevExt)
        if self.ipname != addr:
            self.label += " " + self.ipname

    def stopip_cmd(self):
        if not which(rcEnv.syspaths.ipadm):
            raise ex.excError("crossbow ips are not supported on this system")
        ret, out, err = (0, '', '')
        if self.gateway is not None:
            cmd=['route', '-q', 'delete', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        cmd=[rcEnv.syspaths.ipadm, 'delete-addr', self.stacked_dev+'/'+self.ipdevExt]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        cmd = [rcEnv.syspaths.ipadm, 'show-addr', '-p', '-o', 'state', self.stacked_dev ]
        _out, _, _ = justcall(cmd)
        _out = _out.strip().split("\n")
        if len(_out) > 0:
            self.log.info("skip delete-ip because addrs still use the ip")
            return ret, out, err
        cmd=[rcEnv.syspaths.ipadm, 'delete-ip', self.stacked_dev]
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
        out, err, ret = justcall(cmd)
        if ret != 0:
            return "undef"
        return out.strip()

    def startip_cmd(self):
        if not which(rcEnv.syspaths.ipadm):
            raise ex.excError("crossbow ips are not supported on this system")
        if self.mask is None:
            raise ex.excError("netmask not specified nor guessable")
        self.wait_net_smf()
        ret, out, err = (0, '', '')
        cmd = [rcEnv.syspaths.ipadm, 'show-if', '-p', '-o', 'state', self.stacked_dev]
        _out, err, ret = justcall(cmd)
        _out = _out.strip().split("\n")
        if len(_out) == 0:
            cmd=[rcEnv.syspaths.ipadm, 'create-ip', '-t', self.stacked_dev ]
            r, o, e = self.vcall(cmd)
        cmd=[rcEnv.syspaths.ipadm, 'create-addr', '-t', '-T', 'static', '-a', self.addr+"/"+to_cidr(self.mask), self.stacked_dev+'/'+self.ipdevExt]
        r, o, e = self.vcall(cmd)
        if r != 0:
            cmd=[rcEnv.syspaths.ipadm, 'show-if' ]
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

    def is_up(self):
        cmd = [rcEnv.syspaths.ipadm, "show-addr", "-p", "-o", "STATE,ADDR", self.ipdev+'/'+self.ipdevExt]
        out, err, ret = justcall(cmd)
        if ret != 0:
            # normal down state
            return False
        try:
            state, addr = out.strip("\n").split(":")
        except ValueError:
            self.status_log(out)
            return False
        if state != "ok":
            self.status_log("state: %s" % state)
            return False
        try:
            _addr, _mask = addr.split("/")
        except ValueError:
            self.status_log(out)
            return False
        if _addr != self.addr:
            self.status_log("wrong addr: %s" % addr)
            return False
        if self.mask is None:
            self.status_log("netmask not specified nor guessable")
        elif _mask != to_cidr(self.mask):
            self.status_log("wrong mask: %s, expected %s" % (_mask, to_cidr(self.mask)))
            return True
        return True

