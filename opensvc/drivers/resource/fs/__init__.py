import os
import subprocess
import shutil
import time

import core.exceptions as ex
import core.status
from core.resource import Resource
from core.objects.svcdict import KEYS
from env import Env
from utilities.files import protected_dir
from utilities.drivers import driver_import
from utilities.lazy import lazy
from utilities.proc import justcall, which


KW_PRKEY = {
    "keyword": "prkey",
    "at": True,
    "text": "Defines a specific persistent reservation key for the resource. Takes priority over the service-level defined prkey and the node.conf specified prkey."
}
KW_CREATE_OPTIONS = {
    "keyword": "create_options",
    "convert": "shlex",
    "default": [],
    "at": True,
    "provisioning": True,
    "text": "Additional options to pass to the logical volume create command. Size and name are alread set.",
    "example": "--contiguous y"
}
KW_SCSIRESERV = {
    "keyword": "scsireserv",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to every disks held by this resource. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to ``false`` or not set, :kw:`scsireserv` can be activated on a per-resource basis."
}
KW_NO_PREEMPT_ABORT = {
    "keyword": "no_preempt_abort",
    "at": True,
    "candidates": (True, False),
    "default": False,
    "convert": "boolean",
    "text": "If set to ``true``, OpenSVC will preempt scsi reservation with a preempt command instead of a preempt and and abort. Some scsi target implementations do not support this last mode (esx). If set to ``false`` or not set, :kw:`no_preempt_abort` can be activated on a per-resource basis."
}
KW_DEV = {
    "keyword": "dev",
    "protoname": "device",
    "at": True,
    "required": True,
    "text": "The block device file or filesystem image file hosting the filesystem to mount. Different device can be set up on different nodes using the ``dev@nodename`` syntax"
}
KW_VG = {
    "keyword": "vg",
    "required": False,
    "at": True,
    "text": "The name of the disk group the filesystem device should be provisioned from.",
    "provisioning": True
}
KW_SIZE = {
    "keyword": "size",
    "required": False,
    "convert": "size",
    "at": True,
    "text": "The size of the logical volume to provision for this filesystem. A size expression or <n>%{FREE|PVS|VG}.",
    "provisioning": True
}
KW_MKFS_OPT = {
    "keyword": "mkfs_opt",
    "convert": "shlex",
    "default": [],
    "provisioning": True,
    "at": True,
    "text": "Eventual mkfs additional options."
}
KW_STAT_TIMEOUT = {
    "keyword": "stat_timeout",
    "convert": "duration",
    "default": 5,
    "at": True,
    "text": "The maximum wait time for a stat call to respond. When expired, the resource status is degraded is to warn, which might cause a TOC if the resource is monitored."
}
KW_SNAP_SIZE = {
    "keyword": "snap_size",
    "at": True,
    "text": "If this filesystem is build on a snapable logical volume or is natively snapable (jfs, vxfs, ...) this setting overrides the default 10% of the filesystem size to compute the snapshot size. The snapshot is created by snap-enabled rsync-type sync resources. The unit is Megabytes."
}
KW_MNT = {
    "keyword": "mnt",
    "protoname": "mount_point",
    "at": True,
    "required": True,
    "text": "The mount point where to mount the filesystem."
}
KW_MNT_OPT = {
    "keyword": "mnt_opt",
    "protoname": "mount_options",
    "at": True,
    "text": "The mount options."
}
KW_PROMOTE_RW = {
    "keyword": "promote_rw",
    "default": False,
    "convert": "boolean",
    "candidates": (True, False),
    "text": "If set to ``true``, OpenSVC will try to promote the base devices to read-write on start."
}
KW_ZONE = {
    "keyword": "zone",
    "at": True,
    "text": "The zone name the fs refers to. If set, the fs mount point is reparented into the zonepath rootfs."
}


KWS_VIRTUAL = [
    KW_MNT,
    KW_MNT_OPT,
    KW_SIZE,
    KW_DEV,
    KW_STAT_TIMEOUT,
    KW_ZONE,
]

KEYWORDS = [
    KW_MNT,
    KW_DEV,
    KW_MNT_OPT,
    KW_SIZE,
    KW_STAT_TIMEOUT,
    KW_SNAP_SIZE,
    KW_PRKEY,
    KW_SCSIRESERV,
    KW_NO_PREEMPT_ABORT,
    KW_MKFS_OPT,
    KW_CREATE_OPTIONS,
    KW_VG,
    KW_ZONE,
]

KWS_POOLING = [
    KW_MNT,
    KW_DEV,
    KW_MNT_OPT,
    KW_SIZE,
    KW_STAT_TIMEOUT,
    KW_SNAP_SIZE,
    KW_PRKEY,
    KW_SCSIRESERV,
    KW_NO_PREEMPT_ABORT,
    KW_MKFS_OPT,
    KW_ZONE,
]

DRIVER_GROUP = "fs"
DRIVER_BASENAME = ""

KEYS.register_driver(
    "fs",
    "",
    name=__name__,
    keywords=KEYWORDS,
)


class BaseFs(Resource):
    """Define a mount resource
    """

    def __init__(self,
                 mount_point=None,
                 device=None,
                 fs_type=None,
                 mount_options=None,
                 stat_timeout=5,
                 snap_size=None,
                 zone=None,
                 vg=None,
                 size=None,
                 mkfs_opt=None,
                 **kwargs):
        super(BaseFs, self).__init__(type="fs", **kwargs)
        if mount_point:
            mount_point = mount_point.rstrip(os.sep)
        self.mount_point = mount_point
        self._device = device
        self.fs_type = fs_type or ""
        self.stat_timeout = stat_timeout
        self.mount_options = mount_options or ""
        self.snap_size = snap_size
        self.zone = zone
        self.fsck_h = {}
        self.vg = vg
        self.size = size
        self.mkfs_opt = mkfs_opt or []

    def on_add(self):
        if self.zone is None:
            return
        zp = None
        for r in [r for r in self.svc.resources_by_id.values() if r.type == "container.zone"]:
            if r.name == self.zone:
                try:
                    zp = r.zonepath
                except:
                    zp = "<%s>" % self.zone
                break
        if zp is None:
            raise ex.Error("zone %s, referenced in %s, not found" % (self.zone, self.rid))
        self.mount_point = zp + "/root" + self.mount_point
        if "<%s>" % self.zone != zp:
            self.mount_point = os.path.realpath(self.mount_point)
        r.tags.add(self.zone)
        r.tags.add("zone")

    @lazy
    def testfile(self):
        if not self.mount_point:
            return
        return os.path.join(self.mount_point, '.opensvc')

    def set_fsck_h(self):
        """
        Placeholder
        """
        pass

    @lazy
    def device(self):
        if self._device is not None:
            if self.fs_type == "lofs" and not self._device.startswith(os.sep):
                l = self._device.split("/")
                vol = self.svc.get_volume(l[0])
                if vol.mount_point is not None:
                    l[0] = vol.mount_point
                    return "/".join(l)
            return self._device
        # lazy ref support, like {<rid>.exposed_devs[<n>]}
        return self.dev or self.conf_get("dev")

    @lazy
    def label(self): # pylint: disable=method-hidden
        if self.device is None:
            label = self.svc._get(self.rid+".dev", evaluate=False)
        else:
            label = self.device
        if self.mount_point is not None:
            label += "@" + self.mount_point
        if self.fs_type not in ("tmpfs", "shm", "shmfs", "none", None):
            label = self.fs_type + " " + label
        return label

    def _info(self):
        data = [
          ["dev", self.device],
          ["mnt", self.mount_point],
          ["mnt_opt", self.mount_options if self.mount_options else ""],
        ]
        return data

    def start(self):
        self.validate_dev()
        self.promote_rw()
        self.create_mntpt()

    def validate_dev(self):
        if self.fs_type in ["zfs", "advfs"] + Env.fs_net:
            return
        if self.fs_type in ["bind", "lofs"] or "bind" in self.mount_options:
            return
        if self.device in ("tmpfs", "shm", "shmfs", "none"):
            # pseudo fs have no dev
            return
        if not self.device:
            raise ex.Error("device keyword not set or evaluates to None")
        if self.device.startswith("UUID=") or self.device.startswith("LABEL="):
            return
        if not os.path.exists(self.device):
            raise ex.Error("device does not exist %s" % self.device)

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
        if self.fs_type in ("", "tmpfs", "shm", "shmfs", "none") or os.path.isdir(self.device):
            # bind mounts are in this case
            return
        self.set_fsck_h()
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
            raise ex.Error

    def need_check_writable(self):
        if 'ro' in self.mount_options.split(','):
            return False
        if self.fs_type in Env.fs_net + ["tmpfs"]:
            return False
        return True

    def can_check_writable(self):
        """ orverload in child classes to check os-specific conditions
            when a write test might hang (solaris lockfs, linux multipath
            with queueing on and no active path)
        """
        return True

    def check_stat(self):
        if which("stat") is None:
            return True

        if self.device is None:
            return True

        proc = subprocess.Popen(['stat', self.device],
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        for retry in range(self.stat_timeout*10, 0, -1):
            if proc.poll() is None:
                time.sleep(0.1)
            else:
                return True
        try:
            proc.kill()
        except OSError:
            pass
        return False

    def check_writable(self):
        if self.testfile is None:
            return True
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

    def is_up(self):
        """
        Placeholder
        """
        return

    def _status(self, verbose=False):
        if self.is_up():
            if not self.check_stat():
                self.status_log("fs is not responding to stat")
                return core.status.WARN
            if self.need_check_writable() and not self.check_writable():
                self.status_log("fs is not writable")
                return core.status.WARN
            return core.status.UP
        else:
            return core.status.DOWN

    def sub_devs(self):
        pseudofs = [
          'lofs',
          'none',
          'proc',
          'sysfs',
        ]
        if self.fs_type in pseudofs + Env.fs_net:
            return set()
        if self.fs_type == "zfs":
            from utilities.subsystems.zfs import zpool_devs
            return set(zpool_devs(self.device.split("/")[0], self.svc.node))
        for res in self.svc.get_resources():
            if hasattr(res, "is_child_dev") and res.is_child_dev(self.device):
                # don't account fs device if the parent resource is driven by the service
                return set()
        return set([self.device])

    def __str__(self):
        return "%s mnt=%s dev=%s fs_type=%s mount_options=%s" % (super(BaseFs, self).__str__(),\
                self.mount_point, self.device, self.fs_type, self.mount_options)

    def __lt__(self, other):
        """
        Order so that deepest mountpoint can be umount first.
        If no ordering constraint, honor the rid order.
        """
        try:
            smnt = os.path.dirname(self.mount_point)
            omnt = os.path.dirname(other.mount_point)
        except AttributeError:
            return self.rid < other.rid
        return (smnt, self.rid) < (omnt, other.rid)

    """
    Provisioning:
    
    required attributes from child classes:
    *  mkfs = ['mkfs.ext4', '-F']
    *  info = ['tune2fs', '-l']
    """

    def check_fs(self):
        if not hasattr(self, "info"):
            return True
        if self.mkfs_dev is None:
            return True
        cmd = getattr(self, "info") + [self.mkfs_dev]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        self.log.info("%s is not formatted"%self.mkfs_dev)
        return False

    def lv_name(self):
        raise ex.Error
        return "dummy"

    def lv_resource(self):
        try:
            name = self.lv_name()
        except ex.Error:
            return
        vg = self.oget("vg")
        size = self.oget("size")
        try:
            mod = driver_import("resource", "disk", "lv")
            if mod is None:
                return
        except ImportError:
            return
        res = mod.DiskLv(rid="disk#", name=name, vg=vg, size=size)
        res.svc = self.svc
        return res

    def provision_dev(self):
        res = self.lv_resource()
        if not res:
            return
        res.provisioner()

    def unprovision_dev(self):
        res = self.lv_resource()
        if not res:
            return
        res.unprovisioner()

    def provisioned(self):
        if "bind" in self.mount_options or self.fs_type in ("bind", "lofs"):
            return
        try:
            self.dev = self.dev or self.conf_get("dev")
            self.mnt = self.mnt or self.conf_get("mnt")
        except ex.OptNotFound:
            return
        if self.dev is None:
            return
        if self.mnt is None:
            return
        if not os.path.exists(self.mnt):
            return False
        if self.fs_type in Env.fs_net + ["tmpfs"]:
            return
        try:
            self.get_mkfs_dev()
        except ex.Error:
            self.mkfs_dev = None
        if not os.path.exists(self.dev) and (self.mkfs_dev is None or not os.path.exists(self.mkfs_dev)):
            return False
        return self.check_fs()

    def get_mkfs_dev(self):
        self.mkfs_dev = self.dev
        if Env.sysname == 'HP-UX':
            l = self.dev.split('/')
            l[-1] = 'r'+l[-1]
            self.mkfs_dev = '/'.join(l)
            if not os.path.exists(self.mkfs_dev):
                raise ex.Error("%s raw device does not exists"%self.mkfs_dev)
        elif Env.sysname == 'Darwin':
            if os.path.isfile(self.mkfs_dev):
                import utilities.devices.darwin
                devs = utilities.devices.darwin.file_to_loop(self.mkfs_dev)
                if len(devs) == 1:
                    self.mkfs_dev = devs[0]
                else:
                    raise ex.Error("unable to find a device associated to %s" % self.mkfs_dev)
        elif Env.sysname == 'Linux':
            if os.path.isfile(self.mkfs_dev):
                import utilities.devices.linux
                devs = utilities.devices.linux.file_to_loop(self.mkfs_dev)
                if len(devs) == 1:
                    self.mkfs_dev = devs[0]
                else:
                    raise ex.Error("unable to find a device associated to %s" % self.mkfs_dev)

    def provisioner_fs(self):
        if self.fs_type in Env.fs_net + ["tmpfs"]:
            return
        if "bind" in self.mount_options or self.fs_type in ("bind", "lofs"):
            return

        if not self.dev:
            self.dev = self.conf_get("dev")
        if not self.mnt:
            self.mnt = self.conf_get("mnt")

        if self.dev is None:
            raise ex.Error("device %s not found. parent resource is down ?" % self.dev)
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
            self.log.info("%s mount point created"%self.mnt)

        if not os.path.exists(self.dev):
            try:
                vg = self.vg or self.conf_get("vg", verbose=False)
            except ValueError:
                # keyword not supported (ex. bind mounts)
                vg = None
            except ex.OptNotFound:
                vg = None
            if vg:
                self.provision_dev()

        self.get_mkfs_dev()

        if not os.path.exists(self.mkfs_dev):
            raise ex.Error("abort fs provisioning: %s does not exist" % self.mkfs_dev)

        if self.check_fs():
            self.log.info("already provisioned")
            return

        if hasattr(self, "do_mkfs"):
            getattr(self, "do_mkfs")()
        elif hasattr(self, "mkfs"):
            try:
                opts = self.svc.conf_get(self.rid, "mkfs_opt")
            except:
                opts = []
            cmd = getattr(self, "mkfs") + opts + [self.mkfs_dev]
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                self.log.error('Failed to format %s'%self.mkfs_dev)
                raise ex.Error
        else:
            raise ex.Error("no mkfs method implemented")

    def provisioner_shared_non_leader(self):
        self.unset_lazy("device")
        self.unset_lazy("label")

    def provisioner(self):
        self.unset_lazy("device")
        self.unset_lazy("label")
        self.provisioner_fs()

    def purge_mountpoint(self):
        if self.mount_point is None:
            return
        if os.path.exists(self.mount_point) and not protected_dir(self.mount_point):
            self.log.info("rm -rf %s" % self.mount_point)
            try:
                shutil.rmtree(self.mount_point)
            except Exception as e:
                raise ex.Error(str(e))

    def unprovisioner_fs(self):
        pass

    def unprovisioner(self):
        if self.fs_type in Env.fs_net + ["tmpfs"]:
            return
        self.unprovisioner_fs()
        self.purge_mountpoint()
        try:
            vg = self.conf_get("vg", verbose=False)
        except ValueError:
            # keyword not supported (ex. bind mounts)
            vg = None
        except ex.OptNotFound:
            vg = None
        if vg:
            self.unprovision_dev()


