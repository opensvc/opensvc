import resources as Res
import os
import rcExceptions as ex
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which, mimport

class Mount(Res.Resource):
    """Define a mount resource
    """
    def __init__(self,
                 rid=None,
                 mountPoint=None,
                 device=None,
                 fsType=None,
                 mntOpt=None,
                 snap_size=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Resource.__init__(self,
                              rid=rid,
                              type="fs",
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              monitor=monitor,
                              restart=restart,
                              subset=subset)
        self.mountPoint = mountPoint
        self.device = device
        self.fsType = fsType
        self.mntOpt = mntOpt
        self.snap_size = snap_size
        self.always_on = always_on
        self.label = device + '@' + mountPoint
        if self.fsType != "none":
            self.label = self.fsType + " " + self.label
        self.fsck_h = {}
        self.testfile = os.path.join(mountPoint, '.opensvc')
        self.netfs = ['nfs', 'nfs4', 'cifs', 'smbfs', '9pfs', 'gpfs', 'afs', 'ncpfs']

    def pre_action(self, rset=None, action=None):
        if action not in ("stop", "shutdown"):
            return
        cwd = os.getcwd()
        for r in rset.resources:
            if r.skip or r.disabled:
                continue
            if "noaction" in r.tags:
                continue
            if cwd.startswith(r.mountPoint):
                raise ex.excError("parent process current working directory %s is held by the %s resource" % (cwd, r.rid))

    def start(self):
        self.validate_dev()
        self.create_mntpt()

    def validate_dev(self):
        if self.fsType in ["zfs", "advfs"] + self.netfs:
            return
        if self.device == "none":
            # pseudo fs have no dev
            return
        if self.device.startswith("UUID=") or self.device.startswith("LABEL="):
            return
        if not os.path.exists(self.device):
            raise ex.excError("device does not exist %s" % self.device)

    def create_mntpt(self):
        if self.fsType in ["zfs", "advfs"]:
            return
        if os.path.exists(self.mountPoint):
            return
        try:
            os.makedirs(self.mountPoint)
            self.log.info("create missing mountpoint %s" % self.mountPoint)
        except:
            self.log.warning("failed to create missing mountpoint %s" % self.mountPoint)

    def fsck(self):
        if self.fsType in ("", "none") or os.path.isdir(self.device):
            # bind mounts are in this case
            return
        if self.fsType not in self.fsck_h:
            self.log.debug("no fsck method for %s"%self.fsType)
            return
        bin = self.fsck_h[self.fsType]['bin']
        if which(bin) is None:
            self.log.warning("%s not found. bypass."%self.fsType)
            return
        if 'reportcmd' in self.fsck_h[self.fsType]:
            cmd = self.fsck_h[self.fsType]['reportcmd']
            (ret, out, err) = self.vcall(cmd, err_to_info=True)
            if ret not in self.fsck_h[self.fsType]['reportclean']:
                return
        cmd = self.fsck_h[self.fsType]['cmd']
        (ret, out, err) = self.vcall(cmd)
        if 'allowed_ret' in self.fsck_h[self.fsType]:
            allowed_ret = self.fsck_h[self.fsType]['allowed_ret']
        else:
            allowed_ret = [0]
        if ret not in allowed_ret:
            raise ex.excError

    def need_check_writable(self):
        if 'ro' in self.mntOpt.split(','):
            return False
        if self.fsType in self.netfs:
            return False
        return True

    def can_check_writable(self):
        """ orverload in child classes to check os-specific conditions
            when a write test might hang (solaris lockfs, linux multipath
            with queueing on and no active path)
        """
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
        if rcEnv.nodename in self.always_on:
            if self.is_up():
                if self.need_check_writable() and not self.check_writable():
                    self.status_log("fs is not writable")
                    return rcStatus.WARN
                return rcStatus.STDBY_UP
            else:
                return rcStatus.STDBY_DOWN
        else:
            if self.is_up():
                if self.need_check_writable() and not self.check_writable():
                    self.status_log("fs is not writable")
                    return rcStatus.WARN
                return rcStatus.UP
            else:
                return rcStatus.DOWN

    def devlist(self):
        pseudofs = [
          'lofs',
          'none',
          'proc',
          'sysfs',
        ]
        if self.fsType in pseudofs + self.netfs:
            return set([])
        for res in self.svc.get_resources():
            if hasattr(res, "is_child_dev") and res.is_child_dev(self.device):
                # don't account fs device if the parent resource is driven by the service
                return set([])
        return set([self.device])

    def __str__(self):
        return "%s mnt=%s dev=%s fsType=%s mntOpt=%s" % (Res.Resource.__str__(self),\
                self.mountPoint, self.device, self.fsType, self.mntOpt)

    def __cmp__(self, other):
        """order so that deepest mountpoint can be umount first
        """
        return cmp(self.mountPoint, other.mountPoint)

    def provision(self):
        m = mimport(["prov", "fs", self.fsType], fallback=True)
        if not hasattr(m, "ProvisioningFs"):
            raise ex.excError("missing ProvisioningFs class in module %s" % str(m))
        prov = getattr(m, "ProvisioningFs")(self)
        prov.provisioner()

    def unprovision(self):
        m = mimport(["prov", "fs", self.fsType], fallback=True)
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


