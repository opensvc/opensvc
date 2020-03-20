import os
from datetime import datetime

from rcGlobalEnv import rcEnv
import rcStatus
import resources as Res
from rcUtilitiesFreeBSD import check_ping
from rcUtilities import qcall
from svcBuilder import init_kwargs, container_kwargs
from svcdict import KEYS
import resContainer
import rcExceptions as ex

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
    resContainer.KW_START_TIMEOUT,
    resContainer.KW_STOP_TIMEOUT,
    resContainer.KW_NO_PREEMPT_ABORT,
    resContainer.KW_NAME,
    resContainer.KW_HOSTNAME,
    resContainer.KW_OSVC_ROOT_PATH,
    resContainer.KW_GUESTOS,
    resContainer.KW_PROMOTE_RW,
    resContainer.KW_SCSIRESERV,
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
    r = Jail(**kwargs)
    svc += r


class Jail(resContainer.Container):
    """ jail -c name=jail1
                path=/usr/local/opt/jail1.opensvc.com
                host.hostname=jail1.opensvc.com
                ip4.addr=192.168.0.208
                command=/bin/sh /etc/rc
    """
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
            raise ex.excError

    def container_stop(self):
        cmd = ['jail', '-r', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        self.log.error("no forced stop method")
        raise ex.excError

    def ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['jls']
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError
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

    def __init__(self,
                 rid,
                 name,
                 guestos="FreeBSD",
                 jailroot="/tmp",
                 ips=[],
                 ip6s=[],
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        guestos=guestos,
                                        type="container.jail",
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.jailroot = jailroot
        self.ips = ips
        self.ip6s = ip6s

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

