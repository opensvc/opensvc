import glob
import os

from stat import *

import core.exceptions as ex
import utilities.lock

from .. import BaseDisk, BASE_KEYWORDS
from subprocess import *
from utilities.lazy import lazy
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import qcall

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vg"
DRIVER_BASENAME_ALIASES = ["lvm"]
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the volume group"
    },
    {
        "keyword": "options",
        "default": [],
        "at": True,
        "convert": "shlex",
        "provisioning": True,
        "text": "The vgcreate options to use upon vg provisioning."
    },
    {
        "keyword": "dsf",
        "candidates": (True, False),
        "default": True,
        "convert": "boolean",
        "text": "HP-UX only. 'dsf' must be set to false for LVM to use never-multipathed ``/dev/dsk/...`` devices. Otherwize, ad-hoc multipathed ``/dev/disk/...`` devices."
    },
    {
        "keyword": "pvs",
        "required": True,
        "convert": "list",
        "default": [],
        "text": "The list of paths to the physical volumes of the volume group.",
        "provisioning": True
    },
]
DEPRECATED_KEYWORDS = {
    "disk.lvm.vgname": "name",
    "disk.vg.vgname": "name",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "disk.lvm.name": "vgname",
    "disk.vg.name": "vgname",
}
DEPRECATED_SECTIONS = {
    "vg": ["disk", "vg"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("vgdisplay"):
        return ["disk.vg"]
    return []


class DiskVg(BaseDisk):
    def __init__(self, dsf=True, options=None, pvs=None, **kwargs):
        super(DiskVg, self).__init__(type='disk.vg', **kwargs)
        self.label = "vg %s " % self.name
        self.dsf = dsf
        self.options = options or []
        self.raw_pvs = pvs or []

    def is_child_dev(self, device):
        l = device.split("/")
        if len(l) != 4 or l[1] != "dev":
            return False
        vgname = l[2]
        if vgname == self.name:
            return True
        return False

    def files_to_sync(self):
        return [self.mapfile_name(), self.mksffile_name()]

    def mapfile_name(self):
        return os.path.join(self.var_d, 'map')

    def mksffile_name(self):
        return os.path.join(self.var_d, 'mksf')

    def has_it(self):
        """ returns True if the volume is present
        """
        if self.is_active():
            return True
        if not os.path.exists(self.mapfile_name()):
            return False
        if self.is_imported():
            return True
        return False

    def dev2char(self, dev):
        dev = dev.replace("/dev/disk", "/dev/rdisk")
        dev = dev.replace("/dev/dsk", "/dev/rdsk")
        return dev

    def dsf_name(self, dev):
        cmd = ['scsimgr', 'get_attr', '-D', self.dev2char(dev), '-a', 'device_file', '-p']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
        return out.split()[0]

    def write_mksf(self):
        cmd = ['ioscan', '-F', '-m', 'dsf']
        (ret, buff, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
        if len(buff) == 0:
            return
        mksf = {}
        devs = self.sub_devs()
        dsf_names = map(self.dsf_name, devs)
        with open(self.mksffile_name(), 'w') as f:
            for line in buff.split('\n'):
                if len(line) == 0:
                    return
                a = line.split(':')[0]
                if '/dev/pt/pt' not in a and '/dev/rdisk/disk' not in a and not a.endswith(".pt") and self.dsf_name(a) in dsf_names:
                    cmd = ['scsimgr', 'get_attr', '-D', self.dev2char(a), '-a', 'wwid', '-p']
                    (ret, out, err) = self.call(cmd)
                    if ret != 0:
                        raise ex.Error
                    f.write(":".join([a, out.split()[0].replace('0x', '')])+'\n')

    def do_mksf(self):
        if not os.path.exists(self.mksffile_name()):
            return

        instance = {}
        cmd = ['scsimgr', 'get_attr', 'all_lun', '-a', 'wwid', '-a', 'instance', '-p']
        (ret, buff, err) = self.call(cmd)
        for line in buff.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            instance[l[0].replace('0x', '')] = l[1]

        r = 0
        with open(self.mksffile_name(), 'r') as f:
            for line in f.readlines():
                a = line.replace('\n', '').split(':')
                if len(a) == 0:
                    continue
                if os.path.exists(a[0]):
                    continue
                if a[1] not in instance.keys():
                    self.log.error("expected lun %s not present on node %s"%(a[1], Env.nodename))
                    r += 1
                    continue
                cmd = ['mksf', '-r', '-C', 'disk', '-I', instance[a[1]], a[0]]
                (ret, buff, err) = self.vcall(cmd)
                if ret != 0:
                    r += 1
                    continue
        if r > 0:
            raise ex.Error

    def presync(self):
        """ this one is exported as a service command line arg
        """
        cmd = [ 'vgexport', '-m', self.mapfile_name(), '-p', '-s', self.name ]
        ret = qcall(cmd)
        if ret != 0:
            raise ex.Error
        self.write_mksf()

    def is_active(self):
        cmd = [ 'vgdisplay', self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "available" in buff[0]:
            return False
        return True

    def is_imported(self):
        r = self.is_imported_lvm2()
        if r:
            return True
        return self.is_imported_lvm1()

    def is_imported_lvm2(self):
        if not os.path.exists('/etc/lvmtab_p'):
            return False
        cmd = ['strings', '/etc/lvmtab_p']
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        out, err = process.communicate()
        l = out.split('\n')
        map(lambda x: x.strip(), l)
        s = '/dev/'+self.name
        if s in l:
            return True
        return False

    def is_imported_lvm1(self):
        if not os.path.exists('/etc/lvmtab'):
            return False
        cmd = ['strings', '/etc/lvmtab']
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        out, err = process.communicate()
        l = out.split('\n')
        map(lambda x: x.strip(), l)
        s = '/dev/'+self.name
        if s in l:
            return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not os.path.exists(self.mapfile_name()):
            try:
                self.do_export(force_preview=True)
            except ex.Error:
                # vg does not exist
                return False
        if not self.is_imported():
            return False
        if not self.is_active():
            return False
        return True

    def clean_group(self):
        gp = os.path.join(os.sep, "dev", self.name, "group")
        if not os.path.exists(gp):
            return
        cmd = ["rmsf", gp]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to remove pre-existing %s"%gp)
            raise ex.Error

    def do_import(self):
        if self.is_imported():
            self.log.info("%s is already imported" % self.name)
            return
        if self.dsf:
            dsfflag = '-N'
        else:
            dsfflag = ''
        self.lock()
        self.clean_group()
        cmd = [ 'vgimport', '-m', self.mapfile_name(), '-s', dsfflag, self.name ]
        self.log.info(' '.join(cmd))
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        self.unlock()

        # we will modify buff[1], so convert from tuple to list
        buff = list(buff)

        # test string for warnings
        #buff[1] = """Warning: Cannot determine block size of Physical Volume "/dev/rdisk/disk394".
        #Assuming a default value of 1024 bytes. Continuing.
        #Warning: Cannot determine block size of Physical Volume "/dev/rdisk/disk395".
        #Assuming a default value of 1024 bytes. Continuing.
        #vgimport:"""

        if len(buff[1]) > 0:
            import re
            regex = re.compile("Warning:.*\n.*Continuing.\n", re.MULTILINE)
            w = regex.findall(buff[1])
            if len(w) > 0:
                warnings = '\n'.join(w)
                self.log.warning(warnings)
                buff[1] = regex.sub('', buff[1])
            if buff[1] != "vgimport: " and buff[1] != "vgimport:":
                self.log.error('error:\n' + buff[1])

        if len(buff[0]) > 0:
            self.log.debug('output:\n' + buff[0])

        if process.returncode != 0:
            raise ex.Error

    def do_export(self, force_preview=False):
        preview = False
        if os.path.exists(self.mapfile_name()):
            if not self.is_imported():
                self.log.info("%s is already exported" % self.name)
                return
        elif self.is_active():
            preview = True
        if preview or force_preview:
            cmd = [ 'vgexport', '-p', '-m', self.mapfile_name(), '-s', self.name ]
        else:
            cmd = [ 'vgexport', '-m', self.mapfile_name(), '-s', self.name ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_activate(self):
        if self.is_active():
            self.log.info("%s is already available" % self.name)
            return
        cmd = ['vgchange', '-c', 'n', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        cmd = ['vgchange', '-a', 'y', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_deactivate(self):
        if not self.is_active():
            self.log.info("%s is already unavailable" % self.name)
            return
        cmd = ['vgchange', '-a', 'n', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_start(self):
        self.do_import()
        self.do_activate()

    def do_stop(self):
        self.do_deactivate()
        self.do_export()

    def start(self):
        self.do_mksf()
        self.can_rollback = True
        self.do_start()

    def sub_devs(self):
        need_export = False
        if not self.is_active() and not self.is_imported():
            self.do_import()
            need_export = True

        self.sub_devs_cache = set()
        if os.path.exists('/etc/lvmtab'):
            self.sub_devs_cache |= self._sub_devs('/etc/lvmtab')
        if os.path.exists('/etc/lvmtab_p'):
            self.sub_devs_cache |= self._sub_devs('/etc/lvmtab_p')

        if need_export:
            self.do_export()

        return self.sub_devs_cache

    def _sub_devs(self, tabp):
        cmd = ['strings', tabp]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error

        tab = out.split('\n')
        insection = False
        devs = set()
        for e in tab:
            """ move to the first disk of the vg
            """
            if e == "/dev/" + self.name:
                insection = True
                continue
            if not insection:
                continue
            if not  e.startswith('/dev/'):
                continue
            if not e.startswith('/dev/disk') and not e.startswith('/dev/dsk'):
                break
            devs |= set([e])

        return devs

    def lock(self, timeout=30, delay=1):
        lockfile = os.path.join(Env.paths.pathlock, 'vgimport')
        lockfd = None
        try:
            lockfd = utilities.lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except utilities.lock.LockTimeout:
            self.log.error("timed out waiting for lock (%s)"%lockfile)
            raise ex.Error
        except utilities.lock.LockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.Error
        except utilities.lock.LockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.Error
        except utilities.lock.LockAcquire as e:
            self.log.warning("another action is currently running (pid=%s)"%e.pid)
            raise ex.Error
        except ex.Signal:
            self.log.error("interrupted by signal")
            raise ex.Error
        except:
            self.save_exc()
            raise ex.Error("unexpected locking error")
        self.lockfd = lockfd

    def unlock(self):
        utilities.lock.unlock(self.lockfd)

    @lazy
    def pvs(self):
        if not self.raw_pvs:
            # update lazy reference
            self.raw_pvs = self.oget("pvs")
        if not self.raw_pvs:
            raise ex.Error("pvs provisioning keyword is not set")
        l = []
        for pv in self.raw_pvs:
            l += glob.glob(pv)
        if not l:
            raise ex.Error("pvs provisioning keyword expands to an empty list")
        return l

    def provisioner(self):
        if self.has_it():
            self.log.info("already provisioned")
            return

        err = False
        for i, pv in enumerate(self.pvs):
            if not os.path.exists(pv):
                self.log.error("pv %s does not exist"%pv)
                err |= True
            mode = os.stat(pv)[ST_MODE]
            if S_ISBLK(mode):
                continue
            else:
                self.log.error("pv %s is not a block device nor a loop file"%pv)
                err |= True
        if err:
            raise ex.Error

        for pv in self.pvs:
            pv = pv.replace('/disk/', '/rdisk/')
            cmd = ['pvcreate', '-f', pv]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

        pvs = []
        for pv in self.pvs:
            pvs.append(pv.replace('/rdisk/', '/disk/'))
        cmd = ['vgcreate']
        if len(self.options) > 0:
            cmd += self.options
        cmd += [self.name] + pvs
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        self.log.info("provisioned")
        self.svc.node.unset_lazy("devtree")
