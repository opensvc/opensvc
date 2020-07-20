import glob
import os
import re

from stat import *

import core.exceptions as ex
import utilities.devices.linux

from .. import BaseDisk, BASE_KEYWORDS
from core.objects.svcdict import KEYS
from env import Env
from utilities.cache import cache
from utilities.lazy import lazy
from utilities.proc import justcall
from utilities.subsystems.lvm.linux import get_lvs_attr

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "vg"
DRIVER_BASENAME_ALIASES = ["lvm"]
KEYWORDS = [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the volume group"
    },
    {
        "keyword": "options",
        "default": [],
        "convert": "shlex",
        "at": True,
        "provisioning": True,
        "text": "The vgcreate options to use upon vg provisioning."
    },
    {
        "keyword": "pvs",
        "convert": "list",
        "default": [],
        "text": "The list of paths to the physical volumes of the volume group.",
        "provisioning": True
    },
] + BASE_KEYWORDS
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
    def __init__(self, pvs=None, options=None, **kwargs):
        super(DiskVg, self).__init__(type='disk.vg', **kwargs)
        self.label = "vg %s" % self.name
        self.pvs = pvs or []
        self.options = options or []
        self.tag = Env.nodename
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True

    def _info(self):
        data = [
            ["name", self.name],
        ]
        return data

    def is_child_dev(self, device):
        l = device.split("/")
        if len(l) != 4 or l[1] != "dev":
            return False
        if l[2] == "mapper":
            dmname = l[3]
            if "-" not in dmname:
                return False
            i = 0
            dmname.replace("--", "#")
            _l = dmname.split("-")
            if len(_l) != 2:
                return False
            vgname = _l[0].replace("#", "-")
        else:
            vgname = l[2]
        if vgname == self.name:
            return True
        return False

    def has_it(self):
        data = self.get_tags()
        if self.name in data:
            return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.has_it():
            return False
        data = get_lvs_attr()
        if self.name not in data:
            # no lv ... happens in provisioning, where lv are not created yet
            self.log.debug("no logical volumes. consider up")
            return self.tag in self.get_tags()[self.name]
        for attr in data[self.name].values():
            if re.search('....a.', attr) is not None:
                # at least one lv is active
                return True
        return False

    @cache("vg.tags")
    def get_tags(self):
        cmd = [Env.syspaths.vgs, '-o', 'vg_name,tags', '--noheadings', '--separator=;']
        out, err, ret = justcall(cmd)
        data = {}
        for line in out.splitlines():
            l = line.split(";")
            if len(l) == 1:
                data[l[0].strip()] = []
            if len(l) == 2:
                data[l[0].strip()] = l[1].strip().split(",")
        return data

    def test_vgs(self):
        data = self.get_tags()
        if self.name not in data:
            self.clear_cache("vg.tags")
            return False
        return True

    def remove_tag(self, tag):
        cmd = ['vgchange', '--deltag', '@'+tag, self.name]
        (ret, out, err) = self.vcall(cmd)
        self.clear_cache("vg.tags")

    @lazy
    def has_metad(self):
        cmd = ["pgrep", "lvmetad"]
        out, err, ret = justcall(cmd)
        return ret == 0

    def pvscan(self):
        cmd = ["pvscan"]
        if self.has_metad:
            cmd += ["--cache"]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")

    def list_tags(self, tags=None):
        if tags is None:
            tags = []
        if not self.test_vgs():
            self.pvscan()
        data = self.get_tags()
        if self.name not in data:
            raise ex.Error("vg %s not found" % self.name)
        return data[self.name]

    def remove_tags(self, tags=None):
        if tags is None:
            tags = []
        for tag in tags:
            tag = tag.lstrip('@')
            if len(tag) == 0:
                continue
            self.remove_tag(tag)

    def add_tags(self):
        cmd = ['vgchange', '--addtag', '@'+self.tag, self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.clear_cache("vg.tags")

    def activate_vg(self):
        cmd = ['vgchange', '-a', 'y', self.name]
        ret, out, err = self.vcall(cmd)
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        if ret != 0:
            raise ex.Error

    def _deactivate_vg(self):
        cmd = ['vgchange', '-a', 'n', self.name]
        ret, out, err = self.vcall(cmd, err_to_info=True)
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        if ret == 0:
            return True
        if not self.is_up():
            return True
        return False

    def deactivate_vg(self):
        self.wait_for_fn(self._deactivate_vg, 3, 1, errmsg="deactivation failed to release all logical volumes")

    def do_start(self):
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        curtags = self.list_tags()
        tags_to_remove = set(curtags) - set([self.tag])
        if len(tags_to_remove) > 0:
            self.remove_tags(tags_to_remove)
        if self.tag not in curtags:
            self.add_tags()
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return 0
        self.can_rollback = True
        self.activate_vg()

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.sub_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.Error("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        import glob
        tree = self.svc.node.devtree
        for lvdev in glob.glob("/dev/mapper/%s-*"%self.name.replace("-", "--")):
             if "_rimage_" in lvdev or "_rmeta_" in lvdev or \
                "_mimage_" in lvdev or " _mlog_" in lvdev or \
                lvdev.endswith("_mlog"):
                 continue
             self.remove_dev_holders(lvdev, tree)

    def do_stop(self):
        need_deactivate = self.is_up()
        if need_deactivate:
            self.remove_holders()
            utilities.devices.linux.udevadm_settle()
            self.deactivate_vg()
        try:
            need_remove_tag = self.tag in self.get_tags()[self.name]
        except KeyError:
            need_remove_tag = False
        if need_remove_tag:
            self.remove_tag(self.tag)
        if not need_deactivate and not need_remove_tag:
            self.log.info("vg %s is already down", self.name)

    @cache("vg.lvs")
    def vg_lvs(self):
        cmd = [Env.syspaths.vgs, '--noheadings', '-o', 'vg_name,lv_name', '--separator', ';']
        out, err, ret = justcall(cmd)
        data = {}
        for line in out.splitlines():
            try:
                vgname, lvname = line.split(";")
                vgname = vgname.strip()
            except:
                pass
            if vgname not in data:
                data[vgname] = []
            data[vgname].append(lvname.strip())
        return data


    @cache("vg.pvs")
    def vg_pvs(self):
        cmd = [Env.syspaths.vgs, '--noheadings', '-o', 'vg_name,pv_name', '--separator', ';']
        out, err, ret = justcall(cmd)
        data = {}
        for line in out.splitlines():
            try:
                vgname, pvname = line.split(";")
                vgname = vgname.strip()
            except:
                pass
            if vgname not in data:
                data[vgname] = []
            data[vgname].append(os.path.realpath(pvname.strip()))
        return data

    def sub_devs(self):
        if not self.has_it():
            return set()
        devs = set()
        data = self.vg_pvs()
        if self.name in data:
            devs |= set(data[self.name])
        return devs

    def exposed_devs(self):
        if not self.has_it():
            return set()
        devs = set()
        data = self.vg_lvs()
        if self.name in data:
            for lvname in data[self.name]:
                lvp = "/dev/"+self.name+"/"+lvname
                if os.path.exists(lvp):
                    devs.add(lvp)
        return devs

    def boot(self):
        self.do_stop()



    def provisioned(self):
        # don't trust cache for that
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        self.clear_cache("vg.pvs")
        self.vgscan()
        return self.has_it()

    def unprovisioner(self):
        if not self.has_it():
            return
        cmd = ['vgremove', '-ff', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        self.clear_cache("vg.pvs")
        self.svc.node.unset_lazy("devtree")

    def vgscan(self):
        cmd = [Env.syspaths.vgscan, "--cache"]
        justcall(cmd)

    def has_pv(self, pv):
        cmd = [Env.syspaths.pvscan, "--cache", pv]
        justcall(cmd)
        cmd = [Env.syspaths.pvs, "-o", "vg_name", "--noheadings", pv]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        out = out.strip()
        if out == self.name:
            self.log.info("pv %s is already a member of vg %s", pv, self.name)
            return True
        if out != "":
            raise ex.Error("pv %s in use by vg %s" % (pv, out))
        return True

    def provisioner(self):
        self.pvs = self.pvs or self.oget("pvs")
        if not self.pvs:
            # lazy reference not resolvable
            raise ex.Error("%s.pvs value is not valid" % self.rid)

        l = []
        for pv in self.pvs:
            _l = glob.glob(pv)
            self.log.info("expand %s to %s" % (pv, ', '.join(_l)))
            l += _l
        self.pvs = l

        pv_err = False
        for i, pv in enumerate(self.pvs):
            pv = os.path.realpath(pv)
            if not os.path.exists(pv):
                self.log.error("pv %s does not exist"%pv)
                pv_err |= True
            mode = os.stat(pv)[ST_MODE]
            if S_ISBLK(mode):
                continue
            elif S_ISREG(mode):
                cmd = [Env.syspaths.losetup, '-j', pv]
                out, err, ret = justcall(cmd)
                if ret != 0 or not out.startswith('/dev/loop'):
                    self.log.error("pv %s is a regular file but not a loop"%pv)
                    pv_err |= True
                    continue
                self.pvs[i] = out.split(':')[0]
            else:
                self.log.error("pv %s is not a block device nor a loop file"%pv)
                pv_err |= True
        if pv_err:
            raise ex.Error

        for pv in self.pvs:
            if self.has_pv(pv):
                continue
            cmd = ['pvcreate', '-f', pv]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

        if len(self.pvs) == 0:
            raise ex.Error("no pvs specified")

        if self.has_it():
            self.log.info("vg %s already exists", self.name)
            return

        cmd = ["vgcreate"] + self.options + [self.name] + self.pvs
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

        self.can_rollback = True
        self.clear_cache("vg.lvs")
        self.clear_cache("lvs.attr")
        self.clear_cache("vg.tags")
        self.clear_cache("vg.pvs")
        self.svc.node.unset_lazy("devtree")
