import os

import resources as Res
import resContainer
import rcExceptions as ex

from rcGlobalEnv import rcEnv
from rcUtilities import qcall
from rcUtilitiesSunOS import check_ping
from svcBuilder import init_kwargs, container_kwargs
from svcdict import KEYS

DRIVER_GROUP = "container"
DRIVER_BASENAME = "ldom"
KEYWORDS = [
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
    r = Ldom(**kwargs)
    svc += r


class Ldom(resContainer.Container):
    def __init__(self,
                 rid,
                 name,
                 guestos="SunOS",
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.ldom",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.sshbin = '/usr/local/bin/ssh'


    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def check_capabilities(self):
        cmd = ['/usr/sbin/ldm', 'list' ]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return False
        return True

    def state(self):
        """ ldm state : None/inactive/bound/active
            ldm list -p domainname outputs:
                VERSION
                DOMAIN|[varname=varvalue]*
        """
        cmd = ['/usr/sbin/ldm', 'list', '-p', self.name]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return None
        for word in out.split("|"):
            a=word.split('=')
            if len(a) == 2:
                if a[0] == 'state':
                    return a[1]
        return None

    def ping(self):
        return check_ping(self.addr)

    def container_action(self,action):
        cmd = ['/usr/sbin/ldm', action, self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        return None

    def container_start(self):
        """ ldm bind domain
            ldm start domain
        """
        state = self.state()
        if state == 'None':
            raise ex.excError
        if state == 'inactive':
            self.container_action('bind')
            self.container_action('start')
        if state == 'bound' :
            self.container_action('start')

    def container_forcestop(self):
        """ ldm unbind domain
            ldm stop domain
        """
        if self.state == 'active':
            try:
                self.container_action('stop')
            except ex.excError:
                pass
        self.container_action('unbind')

    def container_stop(self):
        """ launch init 5 into container
            wait_for_shutdown
            ldm stop domain
            ldm unbind domain
        """
        state = self.state()
        if state == 'None':
            raise ex.excError
        if state == 'inactive':
            return None
        if state == 'bound' :
            self.container_action('unbind')
        if state == 'active' :
            cmd = rcEnv.rsh.split() + [ self.name, '/usr/sbin/init', '5' ]
            (ret, buff, err) = self.vcall(cmd)
            if ret == 0:
                try:
                    self.log.info("wait for container shutdown")
                    self.wait_for_fn(self.is_shutdown, self.stop_timeout, 2)
                except ex.excError:
                    pass
            self.container_forcestop()

    def check_manual_boot(self):
        cmd = ['/usr/sbin/ldm', 'list-variable', 'auto-boot?', self.name]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return False
        if out != 'auto-boot?=False' :
            return True
        self.log.info("Auto boot should be turned off")
        return False

    def is_shutdown(self):
        state = self.state()
        if state == 'inactive' or state == 'bound':
            return True
        return False

    def is_down(self):
        if self.state() == 'inactive':
            return True
        return False

    def is_up(self):
        if self.state() == 'active':
            return True
        return False

