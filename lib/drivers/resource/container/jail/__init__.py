import os

from datetime import datetime

import exceptions as ex
import rcStatus
import utilities.ping

from .. import \
    BaseContainer, \
    KW_START_TIMEOUT, \
    KW_STOP_TIMEOUT, \
    KW_NO_PREEMPT_ABORT, \
    KW_NAME, \
    KW_HOSTNAME, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV
from core.resource import Resource
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs, container_kwargs
from core.objects.svcdict import KEYS
from utilities.proc import qcall

DRIVER_GROUP = "container"
DRIVER_BASENAME = "jail"
KEYWORDS = [
    {
        "keyword": "jailroot",
        "text": "Sets the root fs directory of the container",
        "required": True,
    },
    {
        "keyword": "ips",
        "convert": "list",
        "at": True,
        "text": "The ipv4 addresses of the jail."
    },
    {
        "keyword": "ip6s",
        "convert": "list",
        "at": True,
        "text": "The ipv6 addresses of the jail."
    },
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["jailroot"] = svc.oget(s, "jailroot")
    kwargs["ips"] = svc.oget(s, "ips")
    kwargs["ip6s"] = svc.oget(s, "ip6s")
    r = ContainerJail(**kwargs)
    svc += r


class ContainerJail(BaseContainer):
    """ jail -c name=jail1
                path=/usr/local/opt/jail1.opensvc.com
                host.hostname=jail1.opensvc.com
                ip4.addr=192.168.0.208
                command=/bin/sh /etc/rc
    """
    def __init__(self,
                 guestos="FreeBSD",
                 jailroot="/tmp",
                 ips=[],
                 ip6s=[],
                 **kwargs):
        super(ContainerJail, self).__init__(
            guestos=guestos,
            type="container.jail",
            **kwargs
        )
        self.jailroot = jailroot
        self.ips = ips
        self.ip6s = ip6s

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)
    def operational(self):
        return True

    def install_drp_flag(self):
        rootfs = self.jailroot
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def container_start(self):
        cmd = ['jail', '-c', 'name='+self.name, 'path='+self.jailroot,
               'host.hostname='+self.name]
        if len(self.ips) > 0:
            cmd += ['ip4.addr='+','.join(self.ips)]
        if len(self.ip6s) > 0:
            cmd += ['ip6.addr='+','.join(self.ip6s)]
        cmd += ['command=/bin/sh', '/etc/rc']
        self.log.info(' '.join(cmd))
        ret = qcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_stop(self):
        cmd = ['jail', '-r', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_forcestop(self):
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        self.log.error("no forced stop method")
        raise ex.Error

    def ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1)

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['jls']
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if l[2] == self.name:
                return True
        return False

    def get_container_info(self):
        print("TODO: get_container_info()")
        return {'vcpus': '0', 'vmem': '0'}

    def _status(self, verbose=False):
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN


