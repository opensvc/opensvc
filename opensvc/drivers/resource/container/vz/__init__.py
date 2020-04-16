import os

import core.exceptions as ex

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
from env import Env
from core.capabilities import capabilities
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall, qcall

DRIVER_GROUP = "container"
DRIVER_BASENAME = "vz"
KEYWORDS = [
    {
        "keyword": "rootfs",
        "text": "Sets the root fs directory of the container",
        "required": False,
        "provisioning": True
    },
    {
        "keyword": "template",
        "text": "Sets the url of the template unpacked into the container root fs.",
        "required": True,
        "provisioning": True
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

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("vzctl"):
        data.append("container.vz")
    return data


class ContainerVz(BaseContainer):
    def __init__(self, guestos="Linux", rootfs=None, template=None, **kwargs):
        super(ContainerVz, self).__init__(type="container.vz", guestos=guestos, **kwargs)
        self.rootfs = rootfs
        self.template = template

    @lazy
    def runmethod(self):
        return ["vzctl", "exec", self.name]

    @lazy
    def _cf(self):
        return os.path.join(os.sep, "etc", "vz", "conf", "%s.conf" % self.name)

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def files_to_sync(self):
        return [self._cf]

    def get_cf_value(self, param):
        value = None
        try:
            cf = self.cf()
        except:
            return value
        with open(cf, 'r') as f:
            for line in f.readlines():
                if param not in line:
                    continue
                if line.strip()[0] == '#':
                    continue
                l = line.replace('\n', '').split('=')
                if len(l) < 2:
                    continue
                if l[0].strip() != param:
                    continue
                value = ' '.join(l[1:]).strip().rstrip('/')
                break
        return value

    def get_rootfs(self):
        with open(self.cf(), 'r') as f:
            for line in f:
                if 'VE_PRIVATE' in line:
                    return line.strip('\n').split('=')[1].strip('"').replace('$VEID', self.name)
        self.log.error("could not determine lxc container rootfs")
        return ex.Error

    def rcp_from(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.Error()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.Error()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def vzctl(self, action, options=None):
        if options is None:
            options = []
        cmd = ['vzctl', action, self.name] + options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        return out

    def container_start(self):
        self.vzctl('start')

    def container_stop(self):
        self.vzctl('stop')

    def container_forcestop(self):
        raise ex.Error

    def operational(self):
        cmd = self.runmethod + ['/sbin/ifconfig', '-a']
        ret = qcall(cmd)
        if ret == 0:
            return True
        return False

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        """ CTID 101 exist mounted running
        """
        cmd = ['vzctl', 'status', self.name]
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        ret, out, err = self.call(cmd)
        if ret != 0:
            return False
        l = out.split()
        if len(l) != 5:
            return False
        if l[2] != 'exist' or \
           l[3] != 'mounted' or \
           l[4] != 'running':
            return False
        return True

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        try:
            cf = self.cf()
        except:
            return True
        with open(self.cf(), 'r') as f:
            for line in f:
                if 'ONBOOT' in line and 'yes' in line:
                    return False
        return True

    def check_capabilities(self):
        if "vzctl" not in capabilities:
            self.log.debug("vzctl is not in PATH")
            return False
        return True

    def cf(self):
        if not os.path.exists(self._cf):
            self.log.error("%s does not exist"%self._cf)
            raise ex.Error
        return self._cf


