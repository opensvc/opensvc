import os
import signal
import time

from subprocess import *

import core.exceptions as ex

from . import BaseDiskLv
from utilities.converters import convert_size
from env import Env
from utilities.proc import justcall, which
from utilities.string import bdecode

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "lv"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("lvchange"):
        return ["disk.lv"]
    return []

def restore_signals():
    # from http://hg.python.org/cpython/rev/768722b2ae0a/
    signals = ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ')
    for sig in signals:
        if hasattr(signal, sig):
           signal.signal(getattr(signal, sig), signal.SIG_DFL)

class DiskLv(BaseDiskLv):
    def get_dev(self):
        """
        Return the device path in the /dev/<vg>/<lv> format
        """
        return "/dev/" + self.fullname

    def activate(self, dev):
        if not which('lvchange'):
            self.log.debug("lvchange command not found")
            return

        dev = self.get_dev()
        if dev is None:
            return
        cmd = ["lvchange", "-a", "y", dev]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def unprovisioner(self):
        if self.vg is None:
            self.log.debug("skip lv unprovision: no vg option")
            return

        if not which('lvdisplay'):
            self.log.debug("skip lv unprovision: lvdisplay command not found")
            return

        dev = self.get_dev()
        if dev is None:
            return
        cmd = ["lvdisplay", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.log.debug("skip lv unprovision: %s is not a lv" % dev)
            return

        if not which('lvremove'):
            self.log.error("lvcreate command not found")
            raise ex.Error

        if which('wipefs') and os.path.exists(dev):
            self.vcall(["wipefs", "-a", dev])

        cmd = ["lvremove", "-f", dev]
        ret, out, err = self.vcall(cmd)
        self.clear_cache("lvs.attr")
        if ret != 0:
            raise ex.Error
        self.svc.node.unset_lazy("devtree")

    def provisioned(self):
        dev = self.get_dev()
        cmd = ["lvdisplay", dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        return False

    def provisioner(self):
        if not which('vgdisplay'):
            raise ex.Error("vgdisplay command not found")

        if not which('lvcreate'):
            raise ex.Error("lvcreate command not found")

        if not which('lvdisplay'):
            raise ex.Error("lvdisplay command not found")

        if self.vg is None:
            raise ex.Error("skip lv provisioning: vg is not set")

        dev = self.get_dev()

        try:
            self.size = str(self.size).upper()
            if "%" not in self.size:
                size_parm = ["-L", str(convert_size(self.size, _to="m"))+'M']
            else:
                size_parm = ["-l", self.size]
        except Exception as e:
            self.log.info("skip lv provisioning: %s" % str(e))
            return

        cmd = ['vgdisplay', self.vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("volume group %s does not exist" % self.vg)

        lvname = os.path.basename(dev)

        # create the logical volume
        cmd = ['lvcreate', '-n', lvname] + size_parm + self.create_options + [self.vg]
        _cmd = "yes | " + " ".join(cmd)
        self.log.info(_cmd)
        p1 = Popen(["yes"], stdout=PIPE, preexec_fn=restore_signals)
        p2 = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=p1.stdout, close_fds=True)
        out, err = p2.communicate()
        out = bdecode(out)
        err = bdecode(err)
        self.clear_cache("lvs.attr")
        if p2.returncode != 0:
            raise ex.Error(err)
        self.can_rollback = True
        if len(out) > 0:
            for line in out.splitlines():
                self.log.info(line)
        if len(err) > 0:
            for line in err.splitlines():
                if line.startswith("WARNING:"):
                    self.log.warning(line.replace("WARNING: ", ""))
                else:
                    self.log.error(err)

        # /dev/mapper/$vg-$lv and /dev/$vg/$lv creation is delayed ... refresh
        try:
            cmd = [Env.syspaths.dmsetup, 'mknodes']
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            p.communicate()
        except:
            # best effort
            pass
        mapname = "%s-%s" % (self.vg.replace('-','--'),
                             lvname.replace('-','--'))
        dev = '/dev/mapper/'+mapname

        for i in range(3, 0, -1):
            if os.path.exists(dev):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.log.error("timed out waiting for %s to appear"%dev)
            raise ex.Error

        self.svc.node.unset_lazy("devtree")

