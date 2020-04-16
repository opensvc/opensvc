import fnmatch
import glob
import os
import re

from collections import namedtuple
from stat import *

import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall, qcall, which

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vxdg"
DRIVER_BASENAME_ALIASES = ["veritas"]
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the volume group"
    },
    {
        "keyword": "pvs",
        "required": True,
        "text": "The list of paths to the physical volumes of the volume group.",
        "provisioning": True
    },
]
DEPRECATED_SECTIONS = {
    "disk.veritas": ["disk", "vxdg"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("vxdg"):
        return ["disk.vxdg"]
    return []


class DiskVxdg(BaseDisk):
    """
    Veritas Volume group resource
    """
    def __init__(self, pvs=None, **kwargs):
        super(DiskVxdg, self).__init__(type='disk.vxdg', **kwargs)
        self.label = "vxdg %s" % self.name
        self.sub_devs_cache = set()
        self.pvs = pvs or None

    def vxprint(self):
        cmd = ["vxprint", "-g", self.name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = {}
        for line in out.splitlines():
            words = line.split()
            if len(words) < 7:
                continue
            if words[0] == "TY":
                headers = list(words)
                continue
            line = namedtuple("line", headers)._make(words)
            data[(line.TY, line.NAME)] = line
        return data

    def has_it(self):
        """
        Return True if the vg is present
        """
        if not which("vxdg"):
            raise ex.Error("vxdg command not found")
        ret = qcall(["vxdg", "list", self.name])
        if ret == 0 :
            return True
        else:
            return False

    def is_up(self):
        """Returns True if the vg is present and not disabled
        """
        if not which("vxdg"):
            self.status_log("vxdg command not found")
            return False
        if not self.has_it():
            return False
        cmd = ["vxprint", "-ng", self.name]
        ret = qcall(cmd)
        if ret == 0 :
            return True
        else:
            return False

    def defects(self):
        try:
            data = self.vxprint()
        except ex.Error:
            # dg does not exist
            return []
        errs = ["%s:%s:%s" % (key[0], key[1], val.STATE) for key, val in data.items() if val.STATE not in ("-", "ACTIVE")]
        errs += ["%s:%s:%s" % (key[0], key[1], val.KSTATE) for key, val in data.items() if val.KSTATE not in ("-", "ENABLED")]
        return sorted(errs)

    def _status(self, **kwargs):
        for defect in self.defects():
             self.status_log(defect, "warn")
        return super(DiskVxdg, self)._status(**kwargs)

    def has_vxvol_resources(self):
        for res in self.svc.get_resources("disk.vxvol"):
            if res.vg == self.name:
                return True
        return False

    def do_startvol(self):
        if self.has_vxvol_resources():
            return 0
        cmd = ['vxvol', '-g', self.name, '-f', 'startall']
        ret, out, err = self.vcall(cmd)
        return ret

    def do_stopvol(self):
        cmd = [ 'vxvol', '-g', self.name, '-f', 'stopall' ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            ret = self.do_startvol()
            if ret == 0 :
                return 0
            else:
                return ret
        self.can_rollback = True
        for flag in [ '-t', '-tC', '-tCf']:
            cmd = [ 'vxdg', flag, 'import', self.name ]
            (ret, out, err) = self.vcall(cmd)
            if ret == 0 :
                ret = self.do_startvol()
                return ret
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        ret = self.do_stopvol()
        cmd = [ 'vxdg', 'deport', self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def vxdisk_list(self):
        if not which("vxdisk"):
            return {}
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = {}
        for line in out.splitlines():
            words = line.split(None, 4)
            if len(words) < 5:
                continue
            if words[0] == "DEVICE":
                headers = list(words)
                continue
            dev = namedtuple("dev", headers)._make(words)
            if dev.GROUP != self.name and dev.GROUP != "(%s)"%self.name:
                continue
            data[dev.DEVICE] = dev
        return data

    def sub_devs(self):
        """
        Return the set of devices used by the dg.
        """
        if hasattr(self, "sub_devs_cache") and len(self.sub_devs_cache) > 0:
            return self.sub_devs_cache

        devs = ["/dev/vx/dsk/"+dev for dev in self.vxdisk_list()]
        if Env.sysname == "SunOS":
            for idx, dev in enumerate(devs):
                if re.match('^.*s[0-9]$', dev) is None:
                    devs[idx] += "s2"

        self.log.debug("found devs %s held by vg %s" % (devs, self.name))
        self.sub_devs_cache = devs

        return devs


    def provisioned(self):
        return self.prov_has_it()

    def prov_has_it(self):
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        words  = out.split()
        if "(%s)" % self.name in words or \
           " %s " % self.name in words:
            return True
        return False

    def unprovisioner(self):
        #if self.has_it():
        #    self.destroy_vg()
        self.unsetup()

    def unsetup(self):
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        for line in out.splitlines():
            words = line.split()
            if "(%s)" % self.name in words:
                cmd = ["/opt/VRTS/bin/vxdiskunsetup", "-f", words[0]]
                ret, out, err = self.vcall(cmd)
                if ret != 0:
                    raise ex.Error

    def destroy_vg(self):
        cmd = ["vxdg", "destroy", self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.svc.node.unset_lazy("devtree")

    def has_pv(self, pv):
        cmd = ["vxdisk", "list", pv]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        for line in out.splitlines():
            if line.startswith("group:"):
                if "name=%s "%self.name in line:
                    self.log.info("pv %s is already a member of vg %s", pv, self.name)
                    return True
                elif "name= " in line:
                    # pv already initialized but not in a dg
                    return True
                else:
                    vg = line.split("name=", 1)[0].split()[0]
                    raise ex.Error("pv %s in use by vg %s" % (pv, vg))
            if line.startswith("flags:") and "invalid" in line:
                return False
        return False

    @lazy
    def vxdisks(self):
        """
        Parse vxdisk list output.

        Example:

        # vxdisk list
        DEVICE          TYPE            DISK         GROUP        STATUS
        aluadisk_0   auto:cdsdisk    aluadisk_0   osvcdg       online
        aluadisk_1   auto:cdsdisk    aluadisk_1   osvcdg       online
        aluadisk_2   auto:none       -            -            online invalid

        """
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        data = []
        for line in out.splitlines():
            words = line.split()
            if len(words) < 1:
                continue
            if words[0] == "DEVICE":
                continue
            data.append(words[0])
        return data

    def vxname_glob(self, pattern):
        """
        Return the list of vxdisks matching the name globing pattern.
        """
        return [name for name in self.vxdisks if fnmatch.fnmatch(name, pattern)]

    def sysname_glob(self, pattern):
        """
        Return the list of vxdisks matching the system devpath globing pattern.
        """
        data = []
        for devpath in glob.glob(pattern):
            dev = self.svc.node.devtree.get_dev_by_devpath(devpath)
            if dev is None:
                continue
            data.append(dev.alias)
        return data

    def provisioner(self):
        if not self.pvs:
            # lazy reference
            self.pvs = self.oget("pvs")

        if self.pvs is None:
            # lazy reference not resolvable
            raise ex.Error("%s.pvs value is not valid" % self.rid)

        self.pvs = self.pvs.split()
        l = []
        for pv in self.pvs:
            if os.sep not in pv:
                _l = self.vxname_glob(pv)
            else:
                _l = self.sysname_glob(pv)
            if _l:
                self.log.info("expand %s to %s" % (pv, ', '.join(_l)))
            l += _l
        self.pvs = l

        if len(self.pvs) == 0:
            raise ex.Error("no pvs specified")

        for pv in self.pvs:
            if self.has_pv(pv):
                continue
            cmd = ["/opt/VRTS/bin/vxdisksetup", "-i", pv]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

        if self.prov_has_it():
            self.log.info("vg %s already exists")
            return

        cmd = ["vxdg", "init", self.name] + self.pvs
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        self.can_rollback = True
        self.svc.node.unset_lazy("devtree")
