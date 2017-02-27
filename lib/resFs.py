import os

import resources as Res
import rcExceptions as ex
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which, mimport

class Mount(Res.Resource):
    """Define a mount resource
    """
    def __init__(self,
                 rid=None,
                 mount_point=None,
                 device=None,
                 fs_type=None,
                 mount_options=None,
                 snap_size=None,
                 **kwargs):
        Res.Resource.__init__(self,
                              rid=rid,
                              type="fs",
                              **kwargs)
        self.mount_point = mount_point
        self.device = device
        self.fs_type = fs_type
        self.mount_options = mount_options
        self.snap_size = snap_size
        self.label = device + '@' + mount_point
        if self.fs_type != "none":
            self.label = self.fs_type + " " + self.label
        self.fsck_h = {}
        self.testfile = os.path.join(mount_point, '.opensvc')
        self.netfs = ['nfs', 'nfs4', 'cifs', 'smbfs', '9pfs', 'gpfs', 'afs', 'ncpfs']

    def info(self):
        data = [
          ["dev", self.device],
          ["mnt", self.mount_point],
          ["mnt_opt", self.mount_options if self.mount_options else ""],
        ]
        return self.fmt_info(data)

    def start(self):
        self.validate_dev()
        self.create_mntpt()

    def validate_dev(self):
        if self.fs_type in ["zfs", "advfs"] + self.netfs:
            return
        if self.device == "none":
            # pseudo fs have no dev
            return
        if self.device.startswith("UUID=") or self.device.startswith("LABEL="):
            return
        if not os.path.exists(self.device):
            raise ex.excError("device does not exist %s" % self.device)

    def create_mntpt(self):
        if self.fs_type in ["zfs", "advfs"]:
            return
        if os.path.exists(self.mount_point):
            return
        try:
            os.makedirs(self.mount_point)
            self.log.info("create missing mountpoint %s" % self.mount_point)
        except:
            self.log.warning("failed to create missing mountpoint %s" % self.mount_point)

    def fsck(self):
        if self.fs_type in ("", "none") or os.path.isdir(self.device):
            # bind mounts are in this case
            return
        if self.fs_type not in self.fsck_h:
            self.log.debug("no fsck method for %s"%self.fs_type)
            return
        bin = self.fsck_h[self.fs_type]['bin']
        if which(bin) is None:
            self.log.warning("%s not found. bypass."%self.fs_type)
            return
        if 'reportcmd' in self.fsck_h[self.fs_type]:
            cmd = self.fsck_h[self.fs_type]['reportcmd']
            (ret, out, err) = self.vcall(cmd, err_to_info=True)
            if ret not in self.fsck_h[self.fs_type]['reportclean']:
                return
        cmd = self.fsck_h[self.fs_type]['cmd']
        (ret, out, err) = self.vcall(cmd)
        if 'allowed_ret' in self.fsck_h[self.fs_type]:
            allowed_ret = self.fsck_h[self.fs_type]['allowed_ret']
        else:
            allowed_ret = [0]
        if ret not in allowed_ret:
            raise ex.excError

    def need_check_writable(self):
        if 'ro' in self.mount_options.split(','):
            return False
        if self.fs_type in self.netfs:
            return False
        return True

    def can_check_writable(self):
        """ orverload in child classes to check os-specific conditions
            when a write test might hang (solaris lockfs, linux multipath
            with queueing on and no active path)
        """
        return True

    @staticmethod
    def alarm_handler(signum, frame):
        raise ex.excSignal

    def check_stat(self):
        if which("stat") is None:
            return True

        import signal, subprocess
        signal.signal(signal.SIGALRM, self.alarm_handler)
        signal.alarm(5)

        try:
            proc = subprocess.Popen('stat '+self.device, shell=True,
                                    stderr=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            out, err = proc.communicate()
            signal.alarm(0)
        except ex.excSignal:
            return False

        return True

    def check_writable(self):
        if not self.can_check_writable():
            return False

        try:
            f = open(self.testfile, 'w')
            f.write(' ')
            f.close()
        except IOError as e:
            if e.errno == 28:
                self.log.error('No space left on device. Invalidate writable test.')
                return True
            return False
        except:
            return False

        return True

    def _status(self, verbose=False):
        if self.is_up():
            if not self.check_stat():
                self.status_log("fs is not responding to stat")
                return rcStatus.WARN
            if self.need_check_writable() and not self.check_writable():
                self.status_log("fs is not writable")
                return rcStatus.WARN
            return self.status_stdby(rcStatus.UP)
        else:
            return self.status_stdby(rcStatus.DOWN)

    def devlist(self):
        pseudofs = [
          'lofs',
          'none',
          'proc',
          'sysfs',
        ]
        if self.fs_type in pseudofs + self.netfs:
            return set([])
        for res in self.svc.get_resources():
            if hasattr(res, "is_child_dev") and res.is_child_dev(self.device):
                # don't account fs device if the parent resource is driven by the service
                return set([])
        return set([self.device])

    def __str__(self):
        return "%s mnt=%s dev=%s fs_type=%s mount_options=%s" % (Res.Resource.__str__(self),\
                self.mount_point, self.device, self.fs_type, self.mount_options)

    def __lt__(self, other):
        """
        Order so that deepest mountpoint can be umount first.
        """
        return self.mount_point < other.mount_point

    def provision(self):
        m = mimport("prov", "fs", self.fs_type, fallback=True)
        if not hasattr(m, "ProvisioningFs"):
            raise ex.excError("missing ProvisioningFs class in module %s" % str(m))
        prov = getattr(m, "ProvisioningFs")(self)
        prov.provisioner()

    def unprovision(self):
        m = mimport("prov", "fs", self.fs_type, fallback=True)
        if not hasattr(m, "ProvisioningFs"):
            raise ex.excError("missing ProvisioningFs class in module %s" % str(m))
        prov = getattr(m, "ProvisioningFs")(self)
        if hasattr(prov, "unprovisioner"):
            prov.unprovisioner()

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)
    print("""   m=Mount("/mnt1","/dev/sda1","ext3","rw")   """)
    m=Mount("/mnt1","/dev/sda1","ext3","rw")
    print("show m", m)


