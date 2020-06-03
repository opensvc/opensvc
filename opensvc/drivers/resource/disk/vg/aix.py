import json
import os
import shutil

from subprocess import *

import core.exceptions as ex

from .. import BaseDisk, BASE_KEYWORDS
from core.objects.svcdict import KEYS

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
        "default": "",
        "at": True,
        "convert": "shlex",
        "provisioning": True,
        "text": "The vgcreate options to use upon vg provisioning."
    },
    {
        "keyword": "pvs",
        "required": True,
        "convert": "list",
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
    if which("lsvg"):
        return ["disk.vg"]
    return []

# ajouter un dump regulier de la config des vg (pour ne pas manquer les extensions de vol)

class DiskVg(BaseDisk):
    def __init__(self, options=None, pvs=None, **kwargs):
        super(DiskVg, self).__init__(type='disk.vg', **kwargs)
        self.options = options or []
        self.pvs = pvs or []
        self.label = "vg %s" % self.name

    def has_it(self):
        """ returns True if the volume is present
        """
        if self.is_active():
            return True
        if self.is_imported():
            return True
        return False

    def is_active(self):
        cmd = [ 'lsvg', self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "active" in buff[0]:
            return False
        return True

    def is_imported(self):
        cmd = ['lsvg']
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        for vg in buff[0].split('\n'):
            if vg == self.name:
                return True
        return False

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not self.is_imported():
            return False
        if not self.is_active():
            return False
        return True

    def pvid2hdisk(self,mypvid):
        cmd = ['lspv']
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        hdisk = "notfound"
        for line in buff[0].split('\n'):
            if mypvid in line:
                elem = line.split()
                #print("<%s> {%s}"%(line, elem[0]))
                return elem[0]    # first hdisk name matching requested pvid

    def dumped_pvids(self, p):
        if not os.path.exists(p):
            return []
        with open(p) as f:
            s = f.read()
        try:
            data = json.loads(s)
        except:
            return []
        l = []
        for line in data:
            pvid = line.get('pvid')
            if pvid is not None:
                l.append(pvid)
        return l

    def dump_changed(self):
        pvids1 = self.dumped_pvids(self.vgfile_name())
        pvids2 = self.dumped_pvids(self.vgimportedfile_name())
        if set(pvids1) == set(pvids2):
            return False
        return True

    def do_import(self):
        if not os.path.exists(self.vgfile_name()):
            raise ex.Error("%s should exist" % self.vgfile_name())
        if not self.dump_changed() and self.is_imported():
            self.log.info("%s is already imported" % self.name)
            return
        if self.dump_changed() and self.is_imported():
            if self.is_active():
                self.log.warning("%s is active. can't reimport." % self.name)
                return
            self.do_export()
        with open(self.vgfile_name()) as f:
            s = f.read()
        try:
            data = json.loads(s)
        except:
            raise ex.Error("%s is misformatted" % self.vgfile_name())
        self.pvids = {}
        missing = []
        for l in data:
            pvid = l.get('pvid')
            if pvid is None:
                continue
            hdisk = self.pvid2hdisk(pvid)
            self.pvids[pvid] = hdisk
            if hdisk == "notfound":
                missing.append(pvid)

        # check for missing devices
        if len(missing) > 1:
            raise ex.Error("Missing hdisks for pvids %s to be able to import vg" % ','.join(missing))
        elif len(missing) == 1:
            raise ex.Error("Missing hdisk for pvid %s to be able to import vg" % ','.join(missing))

        myhdisks = self.pvids.values()
        cmd = ['importvg', '-n', '-y', self.name, myhdisks[0]]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        shutil.copy2(self.vgfile_name(), self.vgimportedfile_name())

    def do_export(self):
        if not self.is_imported():
            self.log.info("%s is already exported" % self.name)
            return
        cmd = ['exportvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_activate(self):
        if self.is_active():
            self.log.info("%s is already available" % self.name)
            return
        cmd = ['varyonvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_deactivate(self):
        if not self.is_active():
            self.log.info("%s is already unavailable" % self.name)
            return
        cmd = ['varyoffvg', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def do_start(self):
        self.do_import()
        self.do_activate()
        self.do_dumpcfg()
        self.can_rollback = True

    def do_stop(self):
        self.do_dumpcfg()
        self.do_deactivate()

    def vgfile_name(self):
        return os.path.join(self.var_d, self.name + '.vginfo')

    def vgimportedfile_name(self):
        return os.path.join(self.var_d, self.name + '.vginfo.imported')

    def files_to_sync(self):
        return [self.vgfile_name()]

    def do_dumpcfg(self):
        cmd = ['lspv']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        data = []
        for line in out.split('\n'):
            l = line.split()
            n = len(l)
            h = {}
            for i, key in enumerate(['hdisk', 'pvid', 'vg', 'state']):
                if i >= n -1:
                    break
                h[key] = l[i]
            vg = h.get('vg')
            if vg is not None and vg == self.name:
                data.append(h)
        if len(data) == 0:
            # don't overwrite existing dump file with an empty dataset
            return
        s = json.dumps(data)
        with open(self.vgfile_name(), 'w') as f:
            f.write(s)

        """
        root@host:/$ lspv
        hdisk0          00078e0b282e417a                    rootvg          active
        hdisk1          none                                None
        hdisk2          00078e0bb1618c92                    tstvg           active
        hdisk3          00078e0bb161b59e                    tstvg           active
        hdisk4          none                                None
        hdisk5          none                                None

        =>

        [{'hdisk': 'hdisk0', 'pvid': '00078e0b282e417a', 'vg': 'rootvg', 'state': 'active'},
         {'hdisk': 'hdisk1', 'pvid': 'none', 'vg': 'None'},
         {'hdisk': 'hdisk2', 'pvid': '00078e0bb1618c92', 'vg': 'testvg', 'state': 'active'},
         {'hdisk': 'hdisk3', 'pvid': '00078e0bb161b59e', 'vg': 'testvg', 'state': 'active'},
         {'hdisk': 'hdisk4', 'pvid': 'none', 'vg': 'None'},
         {'hdisk': 'hdisk5', 'pvid': 'none', 'vg': 'None'}]
        """

    def sub_devs(self):
        if self.is_active():
            return self.sub_devs_active()
        return self.sub_devs_inactive()

    def sub_devs_active(self):
        devs = set()
        cmd = ['lsvg', '-p', self.name]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error

        for e in out.split('\n'):
            x = e.split()
            if len(x) != 5:
                continue
            devs |= set([x[0]])

        return devs

    def sub_devs_inactive(self):
        devs = set()
        if not os.path.exists(self.vgfile_name()):
            return devs
        with open(self.vgfile_name()) as f:
            s = f.read()
        try:
            data = json.loads(s)
        except:
            return devs
        for l in data:
            pvid = l.get('pvid')
            if pvid is None:
                continue
            hdisk = self.pvid2hdisk(pvid)
            if hdisk == "notfound":
                continue
            devs.add(hdisk)
        return devs

