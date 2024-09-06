import os
import time
import re

import core.exceptions as ex
from core.capabilities import capabilities
from env import Env
from utilities.proc import justcall, which
from . import BaseDiskScsireserv

# maximum number of unit attention to read from a device during ack_unit_attention()
ACK_UNIT_ATTENTION_RETRY_MAX = 10

# delay to wait before retry ack unit attention on a device that reports unit attention
ACK_UNIT_ATTENTION_RETRY_DELAY = 0.1


def mpathpersist_enabled_in_conf(output):
    for conf_line in output.splitlines():
        if re.search(r'^\s*reservation_key\s+("|)?file\1\s*$', conf_line) is not None:
            return True
    return False


def parse_version(buff):
    for line in buff.splitlines():
        try:
            return [int(v) for v in line.split()[1].strip("v").split(".")]
        except Exception:
            continue
    return [0, 0, 0]

# noinspection PyUnusedLocal
def driver_capabilities(node=None):
    data = []
    if which("sg_persist"):
        data.append("disk.scsireserv")
        data.append("disk.scsireserv.sg_persist")
    if which("mpathpersist"):
        _, err, ret = justcall(["multipath", "-h"])
        version = parse_version(err)
        if version > [0, 7, 8]:
            def multipath_get_conf():
                conf_output, _, exit_code = justcall(["multipathd", "show", "config"])
                if exit_code == 0:
                    return conf_output
                # fallback to config file if multipathd is not running yet
                conf_output, _, exit_code = justcall(["multipath", "-t"])
                if exit_code == 0:
                    return conf_output
                return ""

            if mpathpersist_enabled_in_conf(multipath_get_conf()):
                data.append("disk.scsireserv")
                data.append("disk.scsireserv.mpathpersist")
    return data


class DiskScsireservSg(BaseDiskScsireserv):
    def scsireserv_supported(self):
        if not self.has_capability("disk.scsireserv"):
            self.status_log("sg_persist or mpathpersist must be installed to use scsi-3 reservations")
            return False
        return True

    @staticmethod
    def set_read_only(val):
        if Env.sysname != "Linux":
            return
        os.environ["SG_PERSIST_O_RDONLY"] = str(val)
        os.environ["SG_PERSIST_IN_RDONLY"] = str(val)

    def ack_unit_attention(self, d):
        if not os.path.exists(d):
            return 0
        if self.use_mpathpersist(d):
            return 0
        i = ACK_UNIT_ATTENTION_RETRY_MAX
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
                time.sleep(ACK_UNIT_ATTENTION_RETRY_DELAY)
                continue
            break
        if i == 0:
            self.log.error("timed out waiting for 'Unit Attention' to go away on disk %s" % d)
            return 1
        return 0

    def get_mpath_registrations(self, path, paths):
        ret, out, err = self.read_mpath_registrations(path)
        n_expected = self.get_expected_registration_count(path, paths)
        n_registered = out.count(self.hostid)
        return n_expected, n_registered

    def read_mpath_registrations(self, disk):
        if not os.path.exists(disk):
            return 1, "", ""
        self.set_read_only(1)
        cmd = ["mpathpersist", "-i", "-k", disk]
        ret, out, err = self.call(cmd)
        return ret, out, err

    def get_path_registrations(self, path, paths):
        self.ack_unit_attention(path)
        ret, out, err = self.read_path_registrations(path)
        if ret != 0:
            return 0, 0
        n_expected = self.get_expected_registration_count(path, paths)
        n_registered = out.count(self.hostid)
        return n_expected, n_registered

    def get_expected_registration_count(self, path, paths):
        if Env.sysname != "Linux":
            return len(paths)
        try:
            from utilities.diskinfo import DiskInfo
            if DiskInfo(deferred=True).disk_vendor(path).strip() != "3PARdata":
                return len(paths)
            if DiskInfo(deferred=True).disk_model(path).strip() != "VV":
                return len(paths)
        except Exception:
            return len(paths)

        # Here we known the mpath is served by a 3PARdata array.
        # This array show one registration per I_L nexus, instead of the
        # standard per I_T_L nexus. So the expected registration count is
        # the number of unique I (hostid) of the paths of L. 

        hostids = set()
        # example:
        #  ["/dev/sdy", "/dev/sdaf", "/dev/sdk", "/dev/sdr"]
        for p in paths:
            sysdev = os.path.realpath("/sys/block/%s/device"%os.path.basename(p))
            # => /sys/block/sdy/device  (symlink to ../../../3:0:0:172/)

            t = os.path.basename(sysdev)
            # => "3:0:0:172"

            hostid = t.split(":")[0]
            # => "3"

            hostids.add(hostid)
        # set("3", "4")
        return len(hostids)

    def read_path_registrations(self, disk):
        if not os.path.exists(disk):
            return 1, "", ""
        self.set_read_only(1)
        cmd = ["sg_persist", "-n", "-k", disk]
        ret, out, err = self.call(cmd)
        return ret, out, err

    def read_registrations(self):
        n_expected = 0
        n_registered = 0
        for mpath, paths in self.devs.items():
            n_paths = len(paths)
            if self.use_mpathpersist(mpath):
                i, j = self.get_mpath_registrations(mpath, paths)
                n_expected += i
                n_registered += j
            else:
                for path in paths:
                    i, j = self.get_path_registrations(path, paths)
                    if not i and not j:
                        continue
                    n_expected += i
                    n_registered += j
                    break
        return n_expected, n_registered

    def check_all_paths_registered(self):
        n_paths, n_registered = self.read_registrations()
        if n_registered == n_paths:
            return
        if n_registered == 0:
            return
        if n_registered > n_paths:
            raise ex.Signal("%d/%d paths registered" % (n_registered, n_paths))
        raise ex.Error("%d/%d paths registered" % (n_registered, n_paths))

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
        # Need ack unit attention from possible previous register-ignore on other paths
        # example:
        # Persistent reservation out:
        #  Fixed format, current; Sense key: Unit Attention
        #  Additional sense: Registrations preempted
        #  PR out (Register and ignore existing key): Unit attention
        self.ack_unit_attention(disk)
        cmd = ["sg_persist", "-n", "--out", "--register-ignore", "--param-sark=" + self.hostid, disk]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        if self.use_mpathpersist(disk):
            return self.mpath_unregister(disk)
        else:
            ret = 0
            for path in self.devs[disk]:
                ret += self.path_unregister(path)
            return ret

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

    @staticmethod
    def dev_to_mpath_dev(devpath):
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
            ret = self.path_clear_reservation(disk)
            if ret == 24:
                self.log.warning("clear %s failed, will try clear on sub devs" % disk)
                for path in self.devs[disk]:
                    sub_ret = self.path_clear_reservation(path)
                    if sub_ret != 0:
                        self.log.warning("clear %s sub device %s failed" % (disk, path))
                        continue
                    return sub_ret
            return ret

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
        from utilities.diskinfo import DiskInfo
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
