import os
import json
import math
import re
import time
from subprocess import *

import core.exceptions as ex
from core.capabilities import capabilities
from env import Env
from utilities.diskinfo import DiskInfo
from utilities.files import makedirs
from utilities.lazy import lazy
from utilities.proc import justcall
from utilities.string import bdecode
from . import BaseDiskScsireserv

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("sg_persist"):
        data.append("disk.scsireserv")
        data.append("disk.scsireserv.sg_persist")
    if which("mpathpersist"):
        out, err, ret = justcall(["multipath", "-h"])
        for line in err.splitlines():
            version = [int(v) for v in line.split()[1].strip("v").split(".")]
            break
        if version > [0, 7, 8]:
            data.append("disk.scsireserv")
            data.append("disk.scsireserv.mpathpersist")
    return data

class DiskScsireservSg(BaseDiskScsireserv):
    reg_pp = None

    def scsireserv_supported(self):
        if not self.has_capability("disk.scsireserv"):
            self.status_log("sg_persist or mpathpersist must be installed to use scsi-3 reservations")
            return False
        return True

    def set_read_only(self, val):
        if Env.sysname != "Linux":
            return
        os.environ["SG_PERSIST_O_RDONLY"] = str(val)
        os.environ["SG_PERSIST_IN_RDONLY"] = str(val)

    def ack_unit_attention(self, d):
        if not os.path.exists(d):
            return 0
        if self.use_mpathpersist(d):
            return 0
        i = self.preempt_timeout
        self.set_read_only(0)
        while i > 0:
            i -= 1
            cmd = ["sg_persist", "-n", "-r", d]
            out, err, ret = justcall(cmd)
            if "unsupported service action" in err:
                raise ex.ScsiPrNotsupported("disk %s does not support persistent reservation" % d)
            if "Not ready" in err:
                # huawei dorado hypermetropair paused member set that.
                raise ex.ScsiPrNotsupported("disk %s Not Ready" % d)
            if "error opening file" in err:
                return 0
            if "Unit Attention" in out or ret != 0:
                self.log.debug("disk %s reports 'Unit Attention' ... waiting" % d)
                time.sleep(1)
                continue
            break
        if i == 0:
            self.log.error("timed out waiting for 'Unit Attention' to go away on disk %s" % d)
            return 1
        return 0

    def read_mpath_registrations(self, disk):
        if not os.path.exists(disk):
            return 1, "", ""
        self.set_read_only(1)
        cmd = ["mpathpersist", "-i", "-k", disk]
        ret, out, err = self.call(cmd)
        return ret, out, err

    def read_path_registrations(self, disk):
        if not os.path.exists(disk):
            return 1, "", ""
        self.set_read_only(1)
        cmd = ["sg_persist", "-n", "-k", disk]
        ret, out, err = self.call(cmd)
        return ret, out, err


    def var_data_dump_path(self):
        return os.path.join(self.var_d, "data.json")

    def var_data_load(self):
        try:
            with open(self.var_data_dump_path(), "r") as f:
                return json.load(f)
        except Exception:
            return {
                "registrations_per_path": {},
            }

    def var_data_dump(self, data):
        p = self.var_data_dump_path()
        makedirs(os.path.dirname(p))
        self.log.debug("write %s in %s", data, p)
        with open(p, "w") as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def read_gen(buff):
        r = re.findall("generation=(0x[0-9a-fA-F]+)", buff, re.MULTILINE)
        try:
            return int(r[0], 0)
        except Exception:
            return -1

    def read_dev_registrations(self, mpath):
        n_paths = 0
        n_registered = 0
        out = ""
        paths = self.devs.get(mpath, [])
        n_paths = len(paths)
        if self.use_mpathpersist(mpath):
            ret, out, err = self.read_mpath_registrations(mpath)
            n_registered = out.count(self.hostid)
        else:
            for path in paths:
                ret, out, err = self.read_path_registrations(path)
                if ret != 0:
                    continue
                n_registered = out.count(self.hostid)
                break
        gen = self.read_gen(out)
        return n_paths, n_registered, gen

    def read_registrations(self):
        n_paths = 0
        n_registered = 0
        for dev in self.devs:
            dev_n_paths, dev_n_registered, _ = self.read_dev_registrations(dev)
            n_paths += dev_n_paths
            n_registered += dev_n_registered
        return n_paths, n_registered

    def check_all_paths_registered(self):
        infos, warns, errs = self.check_all_paths_registered_issues()
        if len(errs) > 0:
            # keep first: this one degrades the resource status to warn
            raise ex.Error("\n".join(errs+warns))
        for s in warns:
            self.status_log(s, "warn")
        for s in infos:
            self.status_log(s, "info")

    def get_reg_pp(self, dev):
        did = DiskInfo(deferred=True).disk_id(dev)
        if self.reg_pp is None or not isinstance(self.reg_pp, dict):
            try:
                self.reg_pp = self.var_data_load()["registrations_per_path"]
            except Exception:
                self.reg_pp = {}
        try:
            return self.reg_pp.get(did)
        except AttributeError:
            # corrupted cache
            self.reg_pp = {}
            return None

    def set_reg_pp(self, dev, val):
        did = DiskInfo(deferred=True).disk_id(dev)
        current = self.get_reg_pp(did)
        if current == val:
            return
        self.reg_pp[did] = val
        data = self.var_data_load()
        data["registrations_per_path"] = self.reg_pp
        self.var_data_dump(data)

    def check_all_paths_registered_issues(self):
        infos = []
        warns = []
        errs = []
        for dev in self.devs:
            reg_pp = self.get_reg_pp(dev)
            if reg_pp is None:
                infos.append("%s registered paths count check disabled until restart" % dev)
                continue
            n_paths, n_registered, _ = self.read_dev_registrations(dev)
            n_paths = math.ceil(n_paths/reg_pp)
            if n_registered == n_paths:
                continue
            if n_registered == 0:
                continue
            if n_registered > n_paths:
                warns.append("%s %d/%d paths registered (%d reg per path)" % (dev, n_registered, n_paths, reg_pp))
            errs.append("%s %d/%d paths registered (%d reg per path)" % (dev, n_registered, n_paths, reg_pp))
        return infos, warns, errs

    def disk_registered(self, disk):
        ret, out, err = self.read_path_registrations(disk)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def use_mpathpersist(self, disk):
        if not self.has_capability("disk.scsireserv.mpathpersist"):
            return False
        if [disk] != self.devs.get(disk, []):
            return True
        return False

    def disk_register(self, disk):
        _, _, gen1 = self.read_dev_registrations(disk)
        try:
            return self._disk_register(disk)
        finally:
            _, n_paths, gen2 = self.read_dev_registrations(disk)
            if n_paths >0:
                reg_pp = int((gen2 - gen1)/n_paths)
                self.set_reg_pp(disk, reg_pp)

    def _disk_register(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_register(disk)
        else:
            ret = 0
            for path in self.devs[disk]:
                ret += self.path_register(path)
            return ret

    def mpath_register(self, disk):
        self.set_read_only(0)
        cmd = ["mpathpersist", "--out", "--register-ignore", "--param-sark=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def path_register(self, disk):
        self.set_read_only(0)
        cmd = ["sg_persist", "-n", "--out", "--register-ignore", "--param-sark=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_unregister(disk)
        else:
            return self.path_unregister(disk)

    def mpath_unregister(self, disk):
        self.set_read_only(0)
        cmd = ["mpathpersist", "--out", "--register-ignore", "--param-rk=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

    def path_unregister(self, disk):
        self.set_read_only(0)
        cmd = ["sg_persist", "-n", "--out", "--register-ignore", "--param-rk=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

    def dev_to_mpath_dev(self, devpath):
        if "node.x.multipath" not in capabilities:
            return devpath
        cmd = [Env.syspaths.multipath, "-l", "-v1", devpath]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        _devpath = "/dev/mapper/" + out.strip()
        if not out:
            raise ex.Error()
        if not os.path.exists(_devpath):
            raise ex.Error("%s does not exist" % _devpath)
        return _devpath

    def get_reservation_key(self, disk):
        try:
            return self._get_reservation_key(disk)
        except ex.Error:
            disk = self.dev_to_mpath_dev(disk)
            return self._get_reservation_key(disk)

    def _get_reservation_key(self, disk):
        self.set_read_only(1)
        if self.use_mpathpersist(disk):
            cmd = ["mpathpersist", "-i", "-r", disk]
        else:
            cmd = ["sg_persist", "-n", "-r", disk]
        ret, out, err = self.call(cmd, errlog=None)
        if ret != 0:
            raise ex.Error("failed to list reservation for disk %s" % disk)
        if "Key=" in out:
            # sg_persist format
            for w in out.split():
                if "Key=" in w:
                    return w.split("=")[1]
        elif "Key = " in out:
            # mpathpersist format
            return out.split("Key = ")[-1].split()[0]
        else:
            return None
        raise Exception()

    def disk_reserved(self, disk):
        try:
            return self._disk_reserved(disk)
        except ex.Error:
            disk = self.dev_to_mpath_dev(disk)
            return self._disk_reserved(disk)

    def _disk_reserved(self, disk):
        self.set_read_only(1)
        if self.use_mpathpersist(disk):
            cmd = ["mpathpersist", "-i", "-r", disk]
        else:
            cmd = ["sg_persist", "-n", "-r", disk]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error("failed to read reservation for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_release(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_release(disk)
        else:
            return self.path_release(disk)

    def mpath_release(self, disk):
        self.set_read_only(0)
        cmd = ["mpathpersist", "--out", "--release", "--param-rk=" + self.hostid, "--prout-type=" + self.prtype, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def path_release(self, disk):
        self.set_read_only(0)
        cmd = ["sg_persist", "-n", "--out", "--release", "--param-rk=" + self.hostid, "--prout-type=" + self.prtype,
               disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def disk_clear_reservation(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_clear_reservation(disk)
        else:
            return self.path_clear_reservation(disk)

    def mpath_clear_reservation(self, disk):
        cmd = ["mpathpersist", "--out", "--clear", "--param-rk=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to clear reservation on disk %s" % disk)
        return ret

    def path_clear_reservation(self, disk):
        cmd = ["sg_persist", "-n", "--out", "--clear", "--param-rk=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to clear reservation on disk %s" % disk)
        return ret

    def disk_reserve(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_reserve(disk)
        else:
            return self.path_reserve(disk)

    def mpath_reserve(self, disk):
        self.set_read_only(0)
        cmd = ["mpathpersist", "--out", "--reserve", "--param-rk=" + self.hostid, "--prout-type=" + self.prtype, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def path_reserve(self, disk):
        self.set_read_only(0)
        cmd = ["sg_persist", "-n", "--out", "--reserve", "--param-rk=" + self.hostid, "--prout-type=" + self.prtype,
               disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def _disk_preempt_reservation(self, disk, oldkey):
        if self.no_preempt_abort or DiskInfo(deferred=True).disk_vendor(disk).strip() in ["VMware"]:
            preempt_opt = "--preempt"
        else:
            preempt_opt = "--preempt-abort"
        self.set_read_only(0)
        if self.use_mpathpersist(disk):
            cmd = ["mpathpersist", "--out", preempt_opt, "--param-sark=" + oldkey, "--param-rk=" + self.hostid,
                   "--prout-type=" + self.prtype, disk]
        else:
            cmd = ["sg_persist", "-n", "--out", preempt_opt, "--param-sark=" + oldkey, "--param-rk=" + self.hostid,
                   "--prout-type=" + self.prtype, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to preempt reservation for disk %s" % disk)
        return ret
