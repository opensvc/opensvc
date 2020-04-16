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
from core.capabilities import capabilities
from core.resource import Resource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "container"
DRIVER_BASENAME = "esx"
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
    if which("vmware-cmd"):
        data.append("container.esx")
    return data


class ContainerEsx(BaseContainer):
    def __init__(self, **kwargs):
        super(ContainerEsx, self).__init__(type="container.esx", **kwargs)
        self.vmx = None

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def vmcmd(self, l, verbose=False):
        self.find_vmx()
        if verbose:
            ret, out, err = self.vcall(['vmware-cmd', self.vmx] + l)
        else:
            ret, out, err = self.call(['vmware-cmd', self.vmx] + l)
        return ret, out, err

    def check_capabilities(self):
        return "node.x.vmware-cmd" in capabilities

    def ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1, count=1)

    def find_vmx(self):
        if self.vmx is not None:
            return self.vmx
        cmd = ['vmware-cmd', '-l']
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error
        l = out.split()
        pattern = '/'+self.name+'.vmx'
        for vmx in l:
            if pattern in vmx:
                self.vmx = vmx
        if self.vmx is None:
            self.log.error("vm %s not found on this node"%self.name)
            raise ex.Error
        return self.vmx

    def _migrate(self):
        print("TODO")
        pass

    def container_start(self):
        cmd = ['start']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.Error

    def container_stop(self):
        cmd = ['stop', 'soft']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.Error

    def container_forcestop(self):
        cmd = ['stop', 'hard']
        ret, buff, err = self.vmcmd(cmd, verbose=True)
        if ret != 0:
            raise ex.Error

    def is_up(self):
        try:
            (ret, out, err) = self.vmcmd(['getstate'])
        except:
            return False
        if ret != 0:
            return False
        l = out.split()
        if len(l) != 3:
            return False
        if l[-1] == 'on':
            return True
        return False

    def getconfig(self, key):
        cmd = ['getconfig', key]
        ret, out, err = self.vmcmd(cmd)
        if ret != 0:
            return None
        l = out.split()
        if len(l) != 3:
            return None
        return l[-1]

    def get_container_info(self):
        self.info = {'vcpus': '0', 'vmem': '0'}
        try:
            self.info['vcpus'] = self.getconfig('numvcpus')
        except:
            self.info['vcpus'] = '0'
        try:
            self.info['vmem'] = self.getconfig('memsize')
        except:
            self.info['vmem'] = '0'
        return self.info

    def check_manual_boot(self):
        """ ESX will handle the vm startup itself """
        return True
