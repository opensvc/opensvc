import os

import core.exceptions as ex
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
from drivers.resource.disk.hpvm import DiskHpvm
from env import Env
from core.resource import Resource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "container"
DRIVER_BASENAME = "hpvm"
KEYWORDS = [
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
    if which("/opt/hpvm/bin/hpvmstatus"):
        data.append("container.hpvm")
    return data

class ContainerHpvm(BaseContainer):
    def __init__(self, guestos="HP-UX", **kwargs):
        super(ContainerHpvm, self).__init__(type="container.hpvm", guestos=guestos, **kwargs)

    def on_add(self):
        self.vg = DiskHpvm(
            rid = 'vmdg#'+self.rid,
            container_name = self.name
        )
        self.vg.svc = self.svc

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def files_to_sync(self):
        a = self.vg.files_to_sync()
        guest = os.path.join(os.sep, 'var', 'opt', 'hpvm', 'guests', self.name)
        uuid = os.path.realpath(guest)
        if os.path.exists(guest):
            a.append(guest)
        if os.path.exists(uuid):
            a.append(uuid)
        return a

    def ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def container_start(self):
        cmd = ['/opt/hpvm/bin/hpvmstart', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_stop(self):
        cmd = ['/opt/hpvm/bin/hpvmstop', '-g', '-F', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_forcestop(self):
        cmd = ['/opt/hpvm/bin/hpvmstop', '-F', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def check_manual_boot(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        (ret, out, err) = self.call(cmd, cache=True)
        if ret != 0:
            return False
        if out.split(":")[11] == "Manual":
            return True
        self.log.info("Auto boot should be turned off")
        return False

    def get_container_info(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        (ret, out, err) = self.call(cmd, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        self.info['vcpus'] = out.split(':')[19].split(';')[0]
        self.info['vmem'] = out.split(':')[20].split(';')[0]
        if 'GB' in self.info['vmem']:
            self.info['vmem'] = str(1024*1024*int(self.info['vmem'].replace('GB','')))
        return self.info

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return False
        if out.split(":")[10] == "On":
            return True
        return False

    def check_capabilities(self):
        if os.path.exists('/opt/hpvm/bin/hpvmstatus'):
            return True
        return False

    def _migrate(self):
        cmd = ['hpvmmigrate', '-o', '-P', self.name, '-h', self.svc.options.to]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def sub_disks(self):
        return self.vg.sub_disks()

    def sub_devs(self):
        return self.vg.sub_devs()

    def presync(self):
        return self.vg.presync()

    def postsync(self):
        return self.vg.postsync()
