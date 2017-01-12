import resHb
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall, which
import rcOvm

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 **kwargs):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.ovm",
                          **kwargs)
        self.ovsinit = os.path.join(os.sep, 'etc', 'init.d', 'ovs-agent')

    def process_running(self):
        cmd = [self.ovsinit, 'status']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return False
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            if not line.startswith('ok!'):
                return False
        return True

    def stop(self):
        try:
            self.manager = rcOvm.Ovm(log=self.log)
            for r in self.svc.get_resources('container.ovm'):
                self.manager.vm_disable_ha(r.name)
        except ex.excError as e:
            self.log.error(str(e))
            raise

    def start(self):
        try:
            self.manager = rcOvm.Ovm(log=self.log)
            for r in self.svc.get_resources('container.ovm'):
                self.manager.vm_enable_ha(r.name)
        except ex.excError as e:
            self.log.error(str(e))
            raise
        self.can_rollback = True

    def __status(self, verbose=False):
        if not os.path.exists(self.ovsinit):
            self.status_log("OVM agent is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("OVM agent daemons are not running")
            return rcStatus.WARN
        try:
            self.manager = rcOvm.Ovm(log=self.log)
            for r in self.svc.get_resources('container.ovm'):
                ha_enabled = self.manager.vm_ha_enabled(r.name)
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if not ha_enabled:
            self.status_log("HA not enabled for this VM")
            return rcStatus.WARN
        return rcStatus.UP

