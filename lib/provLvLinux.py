from provisioning import Provisioning
from rcUtilities import justcall, which, convert_size
from rcGlobalEnv import rcEnv
from svcBuilder import conf_get_string_scope
from subprocess import *
import os
import rcExceptions as ex
import time
import signal

def restore_signals():
    # from http://hg.python.org/cpython/rev/768722b2ae0a/
    signals = ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ')
    for sig in signals:
        if hasattr(signal, sig):
           signal.signal(getattr(signal, sig), signal.SIG_DFL)

class ProvisioningLv(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provision_lv(self):
        if not which('vgdisplay'):
            self.r.log.error("vgdisplay command not found")
            raise ex.excError

        if not which('lvcreate'):
            self.r.log.error("lvcreate command not found")
            raise ex.excError

        try:
            self.size = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "size")
            self.size = convert_size(self.size, _to="m")
            self.vg = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vg")
        except Exception as e:
            self.r.log.info("skip lv provisioning: %s" % str(e))
            return

        cmd = ['vgdisplay', self.vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("volume group %s does not exist"%self.vg)
            raise ex.excError

        # get lvname from fs dev
        if self.r.device.startswith('/dev/mapper/'):
            dev = self.r.device.replace('/dev/mapper/', '').replace(self.vg.replace('-', '--')+'-', '')
            dev = dev.replace('--', '-')
        elif "/"+self.vg+"/" in self.r.device:
            dev = os.path.basename(self.r.device)
        else:
            self.r.log.error("unexpected dev %s format" % self.r.device)
            raise ex.excError

        # create the logical volume
        cmd = ['lvcreate', '-n', dev, '-L', str(self.size)+'M', self.vg]
        _cmd = "yes | " + " ".join(cmd)
        self.r.log.info(_cmd)
        p1 = Popen(["yes"], stdout=PIPE, preexec_fn=restore_signals)
        p2 = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=p1.stdout, close_fds=True)
        out, err = p2.communicate()
        if p2.returncode != 0:
            raise ex.excError(err)
        if len(out) > 0:
            self.r.log.info(out)
        if len(err) > 0:
            self.r.log.error(err)

        # /dev/mapper/$vg-$lv and /dev/$vg/$lv creation is delayed ... refresh
        try:
            justcall(["dmsetup", "mknodes"])
        except:
            # best effort
            pass
        mapname = "%s-%s"%(self.vg.replace('-','--'),
                           dev.replace('-','--'))
        dev = '/dev/mapper/'+mapname

        for i in range(3, 0, -1):
            if os.path.exists(dev):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.r.log.error("timed out waiting for %s to appear"%dev)
            raise ex.excError


