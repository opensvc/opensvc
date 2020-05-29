import os

import core.exceptions as ex
import utilities.ping

from .. import \
    BaseContainer, \
    KW_SNAP, \
    KW_SNAPOF, \
    KW_VIRTINST, \
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
from env import Env
from core.objects.svcdict import KEYS

DRIVER_GROUP = "container"
DRIVER_BASENAME = "xen"
KEYWORDS = [
    KW_SNAP,
    KW_SNAPOF,
    KW_VIRTINST,
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
    if which("xm"):
        data.append("container.xen")
    return data

class ContainerXen(BaseContainer):
    def __init__(self, **kwargs):
        super(ContainerXen, self).__init__(type="container.xen", **kwargs)

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def list_conffiles(self):
        cf = os.path.join(os.sep, 'opt', 'opensvc', 'var', self.name+'.xml')
        if os.path.exists(cf):
            return [cf]
        return []

    def files_to_sync(self):
        return self.list_conffiles()

    def check_capabilities(self):
        cmd = ['virsh', 'capabilities']
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            self.status_log("can not fetch capabilities")
            return False
        return True

    def ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def container_start(self):
        cf = os.path.join(os.sep, 'opt', 'opensvc', 'var', self.name+'.xml')
        if os.path.exists(cf):
            cmd = ['virsh', 'define', cf]
            (ret, buff, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.Error
        cmd = ['virsh', 'start', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_stop(self):
        cmd = ['virsh', 'shutdown', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_forcestop(self):
        cmd = ['virsh', 'destroy', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['virsh', 'dominfo', self.name]
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        if "running" in out.split() or "idle" in out.split() :
            return True
        return False

    def get_container_info(self):
        cmd = ['virsh', 'dominfo', self.name]
        (ret, out, err) = self.call(cmd, errlog=False, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        for line in out.split('\n'):
            if "CPU(s):" in line: self.info['vcpus'] = line.split(':')[1].strip()
            if "Max memory" in line: self.info['vmem'] = line.split(':')[1].strip()
            if "Autostart:" in line: self.info['autostart'] = line.split(':')[1].strip()
        return self.info

    def check_manual_boot(self):
        self.get_container_info()
        if self.info['autostart'] == 'disable' :
                return True
        else:
                return False
