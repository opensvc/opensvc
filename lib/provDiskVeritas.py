from __future__ import print_function

import os
import glob
import fnmatch
import re
from stat import *

from rcGlobalEnv import rcEnv
import provisioning
import rcExceptions as ex
from rcUtilities import justcall, lazy

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        return self.has_it()

    def has_it(self):
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        words  = out.split()
        if "(%s)" % self.r.name in words or \
           " %s " % self.r.name in words:
            return True
        return False

    def unprovisioner(self):
        if not self.has_it():
            return
        if self.r.has_it():
            self.destroy_vg()
        self.unsetup()

    def unsetup(self):
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        for line in out.splitlines():
            words = line.split()
            if "(%s)" % self.r.name in words:
                cmd = ["/opt/VRTS/bin/vxdiskunsetup", "-f", words[0]]
                ret, out, err = self.r.vcall(cmd)
                if ret != 0:
                    raise ex.excError

    def destroy_vg(self):
        cmd = ["vxdg", "destroy", self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.svc.node.unset_lazy("devtree")

    def has_pv(self, pv):
        cmd = ["vxdisk", "list", pv]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        for line in out.splitlines():
            if line.startswith("group:"):
                if "name=%s "%self.r.name in line:
                    self.r.log.info("pv %s is already a member of vg %s", pv, self.r.name)
                    return True
                else:
                    vg = line.split("name=", 1)[0].split()[0]
                    raise ex.excError("pv %s in use by vg %s" % (pv, vg))
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
            dev = self.node.devtree.get_dev_by_devpath(devpath)
            if dev is None:
                continue
            data.append(dev.alias)
        return data

    def provisioner(self):
        try:
            self.pvs = self.r.svc.conf_get(self.r.rid, "pvs")
        except ex.RequiredOptNotFound:
            raise ex.excError

        if self.pvs is None:
            # lazy reference not resolvable
            raise ex.excError("%s.pvs value is not valid" % self.r.rid)

        self.pvs = self.pvs.split()
        l = []
        for pv in self.pvs:
            if os.sep not in pv:
                _l = self.vxname_glob(pv)
            else:
                _l = self.sysname_glob(pv)
            if _l:
                self.r.log.info("expand %s to %s" % (pv, ', '.join(_l)))
            l += _l
        self.pvs = l

        if len(self.pvs) == 0:
            raise ex.excError("no pvs specified")

        for pv in self.pvs:
            if self.has_pv(pv):
                continue
            cmd = ["/opt/VRTS/bin/vxdisksetup", "-i", pv]
            ret, out, err = self.r.vcall(cmd)
            if ret != 0:
                raise ex.excError

        if self.has_it():
            self.r.log.info("vg %s already exists")
            return

        cmd = ["vxdg", "init", self.r.name] + self.pvs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

        self.r.can_rollback = True
        self.r.svc.node.unset_lazy("devtree")
