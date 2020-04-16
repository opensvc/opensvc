import time

from subprocess import *

import core.exceptions as ex

from drivers.resource.ip.host.sunos import IpHost
from drivers.resource.ip import COMMON_KEYWORDS, KW_IPNAME, KW_IPDEV, KW_NETMASK, KW_GATEWAY
from env import Env
from core.objects.svcdict import KEYS
from utilities.net.converters import to_cidr
from utilities.proc import justcall, which

DRIVER_GROUP = "ip"
DRIVER_BASENAME = "crossbow"
KEYWORDS = [
    KW_IPNAME,
    KW_IPDEV,
    {
        "keyword": "ipdevext",
        "at": True,
        "example": "v4",
        "default": "v4",
        "text": "The interface name extension for crossbow ipadm configuration."
    },
    KW_NETMASK,
    KW_GATEWAY,
] + COMMON_KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("ipadm"):
        return ["ip.crossbow"]
    return []

class IpCrossbow(IpHost):
    def __init__(self, ipdevext="v4", **kwargs):
        self.ipdevext = ipdevext
        super(IpCrossbow, self).__init__(type="ip.crossbow", **kwargs)
        if 'noalias' not in self.tags:
            self.tags.add('noalias')

    def set_label(self):
        """
        Set the resource label property.
        """
        try:
             self.get_mask()
        except ex.Error:
             pass
        try:
            self.getaddr()
            addr = self.addr
        except ex.Error:
            addr = self.ipname
        self.label = "%s/%s %s/%s" % (addr, to_cidr(self.netmask), self.ipdev, self.ipdevext)
        if self.ipname != addr:
            self.label += " " + self.ipname

    def stopip_cmd(self):
        if not which(Env.syspaths.ipadm):
            raise ex.Error("crossbow ips are not supported on this system")
        ret, out, err = (0, '', '')
        if self.gateway is not None:
            cmd=['route', '-q', 'delete', 'default', self.gateway]
            r, o, e = self.call(cmd, info=True, outlog=False, errlog=False)
            ret += r
        cmd=[Env.syspaths.ipadm, 'delete-addr', self.stacked_dev+'/'+self.ipdevext]
        r, o, e =  self.vcall(cmd)
        ret += r
        out += o
        err += e
        cmd = [Env.syspaths.ipadm, 'show-addr', '-p', '-o', 'state', self.stacked_dev ]
        _out, _, _ = justcall(cmd)
        _out = _out.strip().split("\n")
        if len(_out) > 0:
            self.log.info("skip delete-ip because addrs still use the ip")
            return ret, out, err
        cmd=[Env.syspaths.ipadm, 'delete-ip', self.stacked_dev]
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
        if not which(Env.syspaths.ipadm):
            raise ex.Error("crossbow ips are not supported on this system")
        if self.netmask is None:
            raise ex.Error("netmask not specified nor guessable")
        self.wait_net_smf()
        ret, out, err = (0, '', '')
        cmd = [Env.syspaths.ipadm, 'show-if', '-p', '-o', 'state', self.stacked_dev]
        _out, err, ret = justcall(cmd)
        _out = _out.strip().split("\n")
        if len(_out) == 0:
            cmd=[Env.syspaths.ipadm, 'create-ip', '-t', self.stacked_dev ]
            r, o, e = self.vcall(cmd)
        cmd=[Env.syspaths.ipadm, 'create-addr', '-t', '-T', 'static', '-a', self.addr+"/"+to_cidr(self.netmask), self.stacked_dev+'/'+self.ipdevext]
        r, o, e = self.vcall(cmd)
        if r != 0:
            cmd=[Env.syspaths.ipadm, 'show-if' ]
            self.vcall(cmd)
            raise ex.Error("Interface %s is not up. ipadm cannot create-addr over it. Retrying..." % self.stacked_dev)
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
        cmd = [Env.syspaths.ipadm, "show-addr", "-p", "-o", "STATE,ADDR", self.ipdev+'/'+self.ipdevext]
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
        if self.netmask is None:
            self.status_log("netmask not specified nor guessable")
        elif _mask != to_cidr(self.netmask):
            self.status_log("wrong netmask: %s, expected %s" % (_mask, to_cidr(self.netmask)))
            return True
        return True

