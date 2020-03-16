import re
import os
import rcExceptions as ex
import resDisk
from rcGlobalEnv import rcEnv
from rcUtilitiesLinux import major, get_blockdev_sd_slaves, \
                             devs_to_disks, udevadm_settle
from rcUtilities import which, justcall, cache
from svcBuilder import init_kwargs


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    kwargs["vg"] = svc.oget(s, "vg")
    r = Disk(**kwargs)
    svc += r


class Disk(resDisk.Disk):
    def __init__(self,
                 rid=None,
                 name=None,
                 vg=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                              rid=rid,
                              name=name,
                              type='disk.lv',
                              **kwargs)
        self.fullname = "%s/%s" % (vg, name)
        self.label = "lv %s" % self.fullname
        self.vg = vg
        self.refresh_provisioned_on_provision = True
        self.refresh_provisioned_on_unprovision = True

    def _info(self):
        data = [
          ["name", self.name],
          ["vg", self.vg],
        ]
        return data

    def has_it(self):
        attr = self.get_lv_attr()
        if attr is None:
            return False
        return True

    def is_up(self):
        """
        Returns True if the logical volume is present and activated
        """
        attr = self.get_lv_attr()
        if attr is None:
            return False
        if re.search('....a.', attr) is not None:
            return True
        return False

    def get_lv_attr(self):
        cmd = [rcEnv.syspaths.lvs, '-o', 'lv_attr', '--noheadings', '--separator=;', self.fullname]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        return out.strip()

    def activate_lv(self):
        cmd = ['lvchange', '-a', 'y', self.fullname]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def deactivate_lv(self):
        cmd = ['lvchange', '-a', 'n', self.fullname]
        ret, out, err = self.vcall(cmd, err_to_info=True)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return 0
        self.activate_lv()
        self.can_rollback = True

    def remove_dev_holders(self, devpath, tree):
        dev = tree.get_dev_by_devpath(devpath)
        holders_devpaths = set()
        holder_devs = dev.get_children_bottom_up()
        for holder_dev in holder_devs:
            holders_devpaths |= set(holder_dev.devpath)
        holders_devpaths -= set(dev.devpath)
        holders_handled_by_resources = self.svc.sub_devs() & holders_devpaths
        if len(holders_handled_by_resources) > 0:
            raise ex.excError("resource %s has holders handled by other resources: %s" % (self.rid, ", ".join(holders_handled_by_resources)))
        for holder_dev in holder_devs:
            holder_dev.remove(self)

    def remove_holders(self):
        tree = self.svc.node.devtree
        lvdev  = "/dev/mapper/%s-%s" % (
            self.vg.replace("-", "--"),
            self.name.replace("-", "--")
        )
        if "_rimage_" in lvdev or "_rmeta_" in lvdev or \
           "_mimage_" in lvdev or " _mlog_" in lvdev or \
           lvdev.endswith("_mlog"):
            return
        self.remove_dev_holders(lvdev, tree)

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return
        self.remove_holders()
        udevadm_settle()
        self.deactivate_lv()

    def lv_devices(self):
        cmd = ["lvs", "-o", "devices", "--noheadings", self.fullname]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return set()
        devs = set()
        for dev in out.split():
            if "(" in dev:
                devs.add(dev[:dev.index("(")])
        return devs
        
    def sub_devs(self):
        if not self.has_it():
            return set()
        return self.lv_devices()

    def exposed_devs(self):
        if not self.has_it():
            return set()
        devs = set()
        lvp = "/dev/"+self.vg+"/"+self.name
        if os.path.exists(lvp):
            devs.add(lvp)
        return devs

