import os
import time
from subprocess import *

import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import which, bdecode, lazy
from . import BaseDiskScsireserv


class DiskScsireservSg(BaseDiskScsireserv):
    def scsireserv_supported(self):
        if which("sg_persist") is None and which("mpathpersist") is None:
            self.status_log("sg_persist or mpathpersist must be installed to use scsi-3 reservations")
            return False
        return True

    def set_read_only(self, val):
        if rcEnv.sysname != "Linux":
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
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            out, err = p.communicate()
            out = bdecode(out)
            err = bdecode(err)
            ret = p.returncode
            if "unsupported service action" in err:
                raise ex.excScsiPrNotsupported("disk %s does not support persistent reservation" % d)
            if "Not ready" in err:
                # huawei dorado hypermetropair paused member set that.
                raise ex.excScsiPrNotsupported("disk %s Not Ready" % d)
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

    def read_registrations(self):
        n_paths = 0
        n_registered = 0
        for mpath, paths in self.devs.items():
            if self.use_mpathpersist(mpath):
                ret, out, err = self.read_mpath_registrations(mpath)
                n_paths += len(paths)
                n_registered += out.count(self.hostid)
            else:
                for path in paths:
                    ret, out, err = self.read_path_registrations(path)
                    if ret != 0:
                        continue
                    n_paths += len(paths)
                    n_registered += out.count(self.hostid)
                    break
        return n_paths, n_registered

    def check_all_paths_registered(self):
        n_paths, n_registered = self.read_registrations()
        if n_registered == n_paths:
            return
        if n_registered == 0:
            return
        if n_registered > n_paths:
            raise ex.excSignal("%d/%d paths registered" % (n_registered, n_paths))
        raise ex.excError("%d/%d paths registered" % (n_registered, n_paths))

    def disk_registered(self, disk):
        ret, out, err = self.read_path_registrations(disk)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    @lazy
    def has_mpathpersist(self):
        return which("mpathpersist")

    def use_mpathpersist(self, disk):
        if not self.has_mpathpersist:
            return False
        if [disk] != self.devs[disk]:
            return True
        return False

    def disk_register(self, disk):
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
        if which(rcEnv.syspaths.multipath) is None:
            return devpath
        cmd = [rcEnv.syspaths.multipath, "-l", "-v1", devpath]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.excError(err)
        _devpath = "/dev/mapper/" + out.strip()
        if not out:
            raise ex.excError()
        if not os.path.exists(_devpath):
            raise ex.excError("%s does not exist" % _devpath)
        return _devpath

    def get_reservation_key(self, disk):
        try:
            return self._get_reservation_key(disk)
        except ex.excError:
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
            raise ex.excError("failed to list reservation for disk %s" % disk)
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
        except ex.excError:
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
            raise ex.excError("failed to read reservation for disk %s" % disk)
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
        m = __import__("rcDiskInfo" + rcEnv.sysname)
        if self.no_preempt_abort or m.diskInfo(deferred=True).disk_vendor(disk).strip() in ["VMware"]:
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
