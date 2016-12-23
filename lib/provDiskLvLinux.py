from provisioning import Provisioning
from rcUtilities import justcall, which, convert_size
from rcUtilitiesLinux import label_to_dev
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

class ProvisioningDisk(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def get_dev(self):
        """
        Return the device path in the /dev/<vg>/<lv> format
        """
        try:
            dev = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "dev")
        except ex.OptNotFound:
            raise ex.excError("the '%s.dev' keyword is mandatory" % self.r.rid)

        if dev.startswith("LABEL=") or dev.startswith("UUID="):
            _dev = label_to_dev(dev)
            if _dev is None:
                self.r.log.info("unable to find device identified by %s", dev)
                return
            dev = _dev

        try:
            vg = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vg")
        except ex.OptNotFound:
            raise ex.excError("the '%s.vg' keyword is mandatory" % self.r.rid)

        if dev.startswith('/dev/mapper/'):
            dev = dev.replace(vg.replace('-', '--')+'-', '')
            dev = dev.replace('--', '-')
            return "/dev/"+vg+"/"+os.path.basename(dev)
        if "/"+vg+"/" in dev:
            return dev
        self.r.log.error("unexpected dev %s format" % self.r.device)
        raise ex.excError

    def activate(self, dev):
        if not which('lvchange'):
            self.r.log.debug("lvchange command not found")
            return

        dev = self.get_dev()
        if dev is None:
            return
        cmd = ["lvchange", "-a", "y", dev]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def unprovisioner(self):
        try:
            vg = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vg")
        except:
            self.r.log.debug("skip lv unprovision: no vg option")
            return

        if not which('lvdisplay'):
            self.r.log.debug("skip lv unprovision: lvdisplay command not found")
            return

        dev = self.get_dev()
        if dev is None:
            return
        cmd = ["lvdisplay", dev]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.debug("skip lv unprovision: %s is not a lv" % dev)
            return

        if not which('lvremove'):
            self.r.log.error("lvcreate command not found")
            raise ex.excError

        cmd = ["lvremove", "-f", dev]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def provisioner(self):
        if not which('vgdisplay'):
            self.r.log.error("vgdisplay command not found")
            raise ex.excError

        if not which('lvcreate'):
            self.r.log.error("lvcreate command not found")
            raise ex.excError

        if not which('lvdisplay'):
            self.r.log.error("lvdisplay command not found")
            raise ex.excError

        dev = self.get_dev()
        cmd = ["lvdisplay", dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            self.r.log.debug("skip lv provision: %s already exists" % dev)
            if "NOT available" in out:
                self.activate(dev)
            return

        try:
            self.size = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "size")
            self.size = str(self.size).upper()
            if "%FREE" not in self.size:
                size_parm = ["-L", str(convert_size(self.size, _to="m"))+'M']
            else:
                size_parm = ["-l", self.size]
            vg = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "vg")
        except Exception as e:
            self.r.log.info("skip lv provisioning: %s" % str(e))
            return

        cmd = ['vgdisplay', vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.r.log.error("volume group %s does not exist" % vg)
            raise ex.excError

        lvname = os.path.basename(dev)

        # create the logical volume
        cmd = ['lvcreate', '-n', lvname] + size_parm + [vg]
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
            cmd = ['dmsetup', 'mknodes']
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            p.communicate()
        except:
            # best effort
            pass
        mapname = "%s-%s" % (vg.replace('-','--'),
                             lvname.replace('-','--'))
        dev = '/dev/mapper/'+mapname

        for i in range(3, 0, -1):
            if os.path.exists(dev):
                break
            if i != 0:
                time.sleep(1)
        if i == 0:
            self.r.log.error("timed out waiting for %s to appear"%dev)
            raise ex.excError


