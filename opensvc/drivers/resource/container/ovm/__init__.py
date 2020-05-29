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
from env import Env
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.fcache import fcache

DRIVER_GROUP = "container"
DRIVER_BASENAME = "ovm"
KEYWORDS = [
    {
        "keyword": "uuid",
        "at": True,
        "required": True,
        "text": "The virtual machine unique identifier used to pass commands on the VM."
    },
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
    if which("xm") and os.path.exists("/OVS"):
        data.append("container.ovm")
    return data


class ContainerOvm(BaseContainer):
    def __init__(self, uuid=None, **kwargs):
        super(ContainerOvm, self).__init__(type="container.ovm", **kwargs)
        self.uuid = uuid
        self.xen_d = os.path.join(os.sep, 'etc', 'xen')
        self.xen_auto_d = os.path.join(self.xen_d, 'auto')

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def list_conffiles(self):
        cf = os.path.join(self.xen_d, self.uuid)
        if os.path.exists(cf):
            return [cf]
        return []

    def files_to_sync(self):
        return self.list_conffiles()

    def check_capabilities(self):
        cmd = ['xm', 'info']
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            self.status_log("can not fetch xm info")
            return False
        return True

    def ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def find_vmcf(self):
        import glob
        l = glob.glob('/OVS/Repositories/*/VirtualMachines/'+self.uuid+'/vm.cfg')+glob.glob(os.path.join(self.xen_d, self.uuid))
        if len(l) > 1:
            self.log.warning("%d configuration files found in repositories (%s)"%(len(l), str(l)))
        elif len(l) == 0:
            raise ex.Error("no configuration file found in repositories")
        return l[0]

    def _migrate(self):
        cmd = ['xm', 'migrate', '-l', self.uuid, self.svc.options.to]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_start(self):
        cf = self.find_vmcf()
        cmd = ['xm', 'create', cf]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_stop(self):
        cmd = ['xm', 'shutdown', self.uuid]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def container_forcestop(self):
        cmd = ['xm', 'destroy', self.uuid]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['xm', 'list', '--state=running']
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if self.uuid == l[0]:
                return True
        return False

    def get_container_info(self):
        cmd = ['xm', 'list', self.uuid]
        (ret, out, err) = self.call(cmd, errlog=False, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if self.uuid != l[0]:
                continue
            self.info['vcpus'] = l[3]
            self.info['vmem'] = l[2]
            return self.info
        self.log.error("malformed 'xm list %s' output: %s"%(self.uuid, line))
        self.info = {'vcpus': '0', 'vmem': '0'}
        return self.info

    def check_manual_boot(self):
        f = os.path.join(self.xen_auto_d, self.uuid)
        if os.path.exists(f):
            return False
        return True

    @fcache
    def devmap(self):
        devmapping = []

        cf = self.find_vmcf()
        with open(cf, 'r') as f:
            buff = f.read()

        for line in buff.split('\n'):
            if not line.startswith('disk'):
                continue
            disks = line[line.index('['):]
            if len(line) <= 2:
                break
            disks = disks[1:-1]
            disks = disks.split(', ')
            for disk in disks:
                disk = disk.strip("'")
                d = disk.split(',')
                if not d[0].startswith('phy:'):
                    continue
                l = [d[0].strip('phy:'), d[1]]
                devmapping.append(l)
            break

        return devmapping

    def sub_devs(self):
        devs = set(map(lambda x: x[0], self.devmap()))
        return devs

