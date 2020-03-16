from datetime import datetime
import time
import os
import stat

import rcStatus
import resContainer
import resources as Res
import rcExceptions as ex
from rcUtilities import justcall, qcall, which, lazy
from rcUtilitiesSunOS import get_solaris_version
from rcZfs import zfs_setprop
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs, container_kwargs

ZONECFG = "/usr/sbin/zonecfg"
ZONEADM = "/usr/sbin/zoneadm"
PGREP = "/usr/bin/pgrep"
PWAIT = "/usr/bin/pwait"
INIT = "/sbin/init"
SVCS = "/usr/bin/svcs"
MULTI_USER_SMF = "svc:/milestone/multi-user:default"
VALID_ACTIONS = [
    "ready",
    "boot",
    "shutdown",
    "halt",
    "attach",
    "detach",
    "install",
    "clone"
]


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    kwargs["delete_on_stop"] = svc.oget(s, "delete_on_stop")
    r = Zone(**kwargs)
    svc += r


class Zone(resContainer.Container):
    """
    Zone container resource driver.
    """
    def __init__(self,
                 rid,
                 name,
                 guestos="SunOS",
                 delete_on_stop=False,
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.zone",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.label = name
        self.delete_on_stop = delete_on_stop
        self.runmethod = ["/usr/sbin/zlogin", name]
        self.zone_cf = "/etc/zones/%s.xml" % self.name
        self.delayed_noaction = True

    def zone_cfg_dir(self):
        return os.path.join(self.var_d, "zonecfg")

    def zone_cfg_path(self):
        return os.path.join(self.zone_cfg_dir(), self.name+".cfg")

    def export_zone_cfg(self):
        cfg_d = self.zone_cfg_dir()
        if not os.path.exists(cfg_d):
            os.makedirs(cfg_d)

        cfg = self.zone_cfg_path()
        cmd = [ZONECFG, "-z", self.name, "export", "-f", cfg]
        ret, out, err = self.vcall(cmd)
        if ret != 0 and not os.path.exists(cfg):
            raise ex.excError(err)

    def get_zonepath_from_zonecfg_cmd(self):
        cmd = [ZONECFG, "-z", self.name, "info", "zonepath"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("unable to determine zonepath using %s" % " ".join(cmd))
        zp = out.replace("zonepath: ", "").strip()
        return zp

    def get_zonepath_from_zonecfg_export(self):
        fpath = self.zone_cfg_path()
        if not os.path.exists(fpath):
            raise ex.excError("zone config export file %s not found. "
                              "unable to determine zonepath" % fpath)
        with open(fpath, "r") as f:
            buff = f.read()
        for line in buff.split("\n"):
            if "set zonepath" in line:
                return line.split("=")[-1].strip()
        raise ex.excError("set zonepath command not found in %s" % fpath)

    def zonecfg(self, zonecfg_args=None):
        zonecfg_args = zonecfg_args or []
        cmd = [ZONECFG, "-z", self.name] + zonecfg_args
        ret, out, err = self.vcall(cmd, err_to_info=True)
        if ret != 0:
            msg = "%s failed status: %i\n%s" % (" ".join(cmd), ret, out)
            self.log.error(msg)
            raise ex.excError(msg)
        else:
            msg = "%s done status: %i\n%s" % (" ".join(cmd), ret, out)
            self.log.info(msg)
        self.zone_refresh()
        return ret

    def is_zone_locked(self):
        zonelock = "/system/volatile/zones/%s.zoneadm.lock" % self.name
        if not os.path.exists(zonelock):
            self.log.debug("zone %s lockfile does not exist" % self.name)
            return False
        import fcntl
        locked = None
        try:
            fd = open(zonelock, "a", 1)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.log.debug("zone %s is not locked" % self.name)
            locked = False
        except IOError:
            self.log.debug("zone %s is locked" % self.name)
            locked = True
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except:
                pass
            try:
                fd.close()
            except:
                pass
        return locked

    def is_zone_unlocked(self):
        return not self.is_zone_locked()

    def zoneadm(self, action, option=None):
        if action in VALID_ACTIONS:
            cmd = [ZONEADM, "-z", self.name, action]
        else:
            self.log.error("unsupported zone action: %s", action)
            return 1
        if option is not None:
            cmd += option

        begin = datetime.now()
        if os.environ.get("OSVC_ACTION_ORIGIN") == "daemon" and which("su"):
            # the zoneadm command gives an error when executed from osvcd.
            # su creates a clean execution context and makes zoneadm succeed.
            cmd = ["su", "root", "-c", " ".join(cmd)]
        self.log.info("%s", " ".join(cmd))
        ret = self.lcall(cmd, env={})
        duration = datetime.now() - begin
        if ret != 0:
            raise ex.excError("%s failed in %s - ret %i" % (" ".join(cmd), duration, ret))
        else:
            self.log.info("%s done in %s - ret %i" % (" ".join(cmd), duration, ret))
        self.zone_refresh()
        return ret

    def set_zonepath_perms(self):
        if not os.path.exists(self.zonepath):
            os.makedirs(self.zonepath)
        s = os.stat(self.zonepath)
        if s.st_uid != 0 or s.st_gid != 0:
            self.log.info("set %s ownership to uid 0 gid 0"%self.zonepath)
            os.chown(self.zonepath, 0, 0)
        mode = s[stat.ST_MODE]
        if (stat.S_IWOTH & mode) or (stat.S_IXOTH & mode) or (stat.S_IROTH & mode) or \
           (stat.S_IWGRP & mode) or (stat.S_IXGRP & mode) or (stat.S_IRGRP & mode):
            self.vcall(["chmod", "700", self.zonepath])

    def rcp_from(self, src, dst):
        src = os.path.realpath(self.zonepath + "/root/" + src)
        cmd = ["cp", src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(" ".join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        dst = os.path.realpath(self.zonepath + "/root/" + dst)
        cmd = ["cp", src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(" ".join(cmd), err))
        return out, err, ret

    def attach(self):
        if self.state in ("installed", "ready", "running"):
            self.log.info("zone container %s already installed" % self.name)
            return
        elif self.state is None:
            cmd = [ZONECFG, "-z", self.name, "-f", self.zone_cfg_path()]
            ret, out, err = self.vcall(cmd)
            self.zone_refresh()
            if ret != 0:
                raise ex.excError
        options = []
        if get_solaris_version() >= 11.3:
            options += ["-x", "deny-zbe-clone"]
        try:
            self.umount_fs_in_zonepath()
            self.zoneadm("attach", options)
        except ex.excError:
            options.append("-F")
            self.zoneadm("attach", options)
        self.can_rollback = True

    def delete(self):
        if not self.delete_on_stop:
            return 0
        if self.state is None:
            self.log.info("zone container %s already deleted" % self.name)
            return 0
        cmd = [ZONECFG, "-z", self.name, "delete", "-F"]
        ret, out, err = self.vcall(cmd)
        self.zone_refresh()
        if ret != 0:
            raise ex.excError
        return 0

    def detach(self):
        if self.state == "configured":
            self.log.info("zone container %s already detached/configured" % self.name)
            return 0
        self.wait_for_fn(self.is_zone_unlocked, self.stop_timeout, 2)
        return self.zoneadm("detach")

    def ready(self):
        if self.state == "ready" or self.state == "running":
            self.log.info("zone container %s already ready" % self.name)
            return 0
        self.set_zonepath_perms()
        return self.zoneadm("ready")

    def install_drp_flag(self):
        flag = os.path.join(self.zonepath, ".drp_flag")
        self.log.info("install drp flag in container: %s"%flag)
        with open(flag, "w") as f:
            f.write(" ")
            f.close()

    def get_smf_state(self, smf=None):
        cmd = self.runmethod + [SVCS, "-H", "-o", "state", smf]
        (out, err, status) = justcall(cmd)
        if status == 0:
            return out.split("\n")[0]
        else:
            return False

    def is_smf_state(self, smf=None, value=None):
        current_value = self.get_smf_state(smf)
        if current_value is False:
            return False
        if current_value == value:
            return True
        return False

    def is_multi_user(self):
        return self.is_smf_state(MULTI_USER_SMF, "online")

    def wait_multi_user(self):
        self.log.info("wait for smf state on on %s", MULTI_USER_SMF)
        self.wait_for_fn(self.is_multi_user, self.start_timeout, 2)

    def zone_boot(self):
        """
        Return 0 if zone is running else return self.zoneadm("boot")
        """
        if self.state == "running":
            self.log.info("zone container %s already running" % self.name)
            return
        self.zoneadm("boot")
        if self.state != "running":
            raise ex.excError("zone should be running")
        self.wait_multi_user()

    def halt(self):
        """ Need wait poststat after returning to installed state on ipkg
            example: /bin/ksh -p /usr/lib/brand/ipkg/poststate zonename zonepath 5 4
        """
        if self.state != "running":
            self.log.info("skip zone %s halt: state %s" % (self.name, self.state))
            return 0
        ret, out, err = self.vcall(["zlogin", self.name, "/sbin/init", "0"])
        for _ in range(self.stop_timeout):
            self.zone_refresh()
            if self.state == "installed":
                for t2 in range(self.stop_timeout):
                    time.sleep(1)
                    out, err, st = justcall(["pgrep", "-fl", "ipkg/poststate.*" + self.name])
                    if st == 0:
                        self.log.info("waiting for ipkg poststate complete: %s" % out)
                    else:
                        break
                return 0
            time.sleep(1)
        self.log.info("timeout out waiting for %s shutdown", self.name)
        return self.zoneadm("halt")

    def container_start(self):
        self.zone_boot()

    def _status(self, verbose=False):
        if self.state == "running":
            return rcStatus.UP
        return rcStatus.DOWN

    def zone_refresh(self):
        self.unset_lazy("zone_data")
        self.unset_lazy("state")
        self.unset_lazy("brand")
        self.unset_lazy("zonepath")

    @lazy
    def state(self):
        if self.zone_data is None:
            raise ex.excError("zone %s does not exist" % self.name)
        else:
            return self.zone_data.get("state")

    @lazy
    def zonepath(self):
        zp = self.zone_data.get("zonepath")
        if zp:
            return zp
        try:
            zp = self.get_zonepath_from_zonecfg_cmd()
        except ex.excError:
            try:
                zp = self.get_zonepath_from_zonecfg_export()
            except ex.excError:
                zp = "/etc/system/%s" % self.name
        return zp

    @lazy
    def brand(self):
        return self.zone_data.get("brand")

    @lazy
    def zone_data(self):
        """
        Refresh Zone object attributes:
                state
                zonepath
                brand
        from zoneadm -z zonename list -p
        zoneid:zonename:state:zonepath:uuid:brand:ip-type
        """
        out, err, ret = justcall(["zoneadm", "-z", self.name, "list", "-p"])
        if ret != 0:
            return None
        out = out.strip()
        l = out.split(":")
        n_fields = len(l)
        if n_fields == 9:
            (zoneid, zonename, state, zonepath, uuid, brand, iptype, rw, macp) = l
        elif n_fields == 10:
            (zoneid, zonename, state, zonepath, uuid, brand, iptype, rw, macp, dummy) = l
        elif n_fields == 7:
            (zoneid, zonename, state, zonepath, uuid, brand, iptype) = l
        else:
            raise ex.excError("Unexpected zoneadm list output: %s"%out)
        if zonename != self.name:
            return None
        return dict(state=state, zonepath=zonepath, brand=brand)

    def is_running(self):
        """
        Return True if zone is running else False"
        """
        return self.state == "running"

    def is_up(self):
        """
        Return self.is_running status
        """
        return self.is_running()

    def operational(self):
        """
        Return status of: zlogin zone pwd
        """
        cmd = self.runmethod + ["pwd"]
        if qcall(cmd) == 0:
            return True
        return False

    def boot_and_wait_reboot(self):
        """
        Boot zone, then wait for automatic zone reboot
            boot zone
            wait for zone init process end
            wait for zone running
            wait for zone operational
        """
        self.log.info("wait for zone boot and reboot...")
        self.zone_boot()
        if self.is_running is False:
            raise ex.excError("zone is not running")
        cmd = [PGREP, "-z", self.name, "-f", INIT]
        out, err, st = justcall(cmd)
        if st != 0:
            raise ex.excError("fail to detect zone init process")
        pids = " ".join(out.split("\n")).rstrip()
        cmd = [PWAIT, pids]
        self.log.info("wait for zone init process %s termination" % (pids))
        if qcall(cmd) != 0:
            raise ex.excError("failed " + " ".join(cmd))
        self.log.info("wait for zone running again")
        self.wait_for_fn(self.is_up, self.start_timeout, 2)
        self.log.info("wait for zone operational")
        self.wait_for_fn(self.operational, self.start_timeout, 2)

    def umount_fs_in_zonepath(self):
        """
        Zone boot will fail if some fs linger under the zonepath.
        those fs might be datasets automounted upon zpool import.
        umount them.
        If they are needed, them still may be mounted by opensvc
        if declared as zoned fs or encap fs.
        """
        if self.zonepath == "/":
            # sanity check
            return

        m = __import__("rcMounts" + rcEnv.sysname)
        mounts = m.Mounts()
        mounts.sort(reverse=True)
        mntpts = []
        for resource in self.svc.get_resources("fs"):
            mntpts.append(resource.mount_point)
        for mount in mounts.mounts:
            # don't unmount zonepath itself
            if mount.mnt == self.zonepath:
                continue
            if not mount.mnt.startswith(self.zonepath):
                continue
            # don't umount fs not handled by the service
            if mount.mnt not in mntpts:
                continue
            self.vcall(["umount", mount.mnt])
            self.vcall(["rmdir", mount.mnt])
            if mount.type == "zfs":
                zfs_setprop(mount.dev, "canmount", "noauto", log=self.log)

    def start(self):
        if not "noaction" in self.tags:
            self.attach()
            self.ready()
        self.svc.sub_set_action("ip", "start", tags=set([self.name]))
        if not "noaction" in self.tags:
            self.zone_boot()
        self.svc.sub_set_action([
            "disk.scsireserv",
            "disk.zpool",
            "disk.raw",
            "fs"], "start", tags=set([self.name]))

    def _stop(self):
        if not "noaction" in self.tags:
            self.halt()
            self.detach()
            self.delete()

    def stop(self):
        self.export_zone_cfg()
        self.svc.sub_set_action([
            "fs",
            "disk.raw",
            "disk.zpool",
            "disk.scsireserv",
            "ip"], "stop", tags=set([self.name]))
        self._stop()

    def provision(self):
        if not "noaction" in self.tags:
            resContainer.Container.provision(self)
        self.svc.sub_set_action([
            #"ip",
            "disk.scsireserv",
            "disk.zpool",
            "disk.raw",
            "fs"], "provision", tags=set([self.name]))

    def unprovision(self):
        self.svc.sub_set_action([
            "fs",
            "disk.raw",
            "disk.zpool",
            "disk.scsireserv",
            "ip"], "unprovision", tags=set([self.name]))
        if not "noaction" in self.tags:
            resContainer.Container.unprovision(self)

    def presync(self):
        self.export_zone_cfg()

    def files_to_sync(self):
        return [self.zone_cfg_path()]

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def get_container_info(self):
        vcpus = "0"
        vmem = "0"
        cmd = [ZONECFG, "-z", self.name, "info", "rctl", "name=zone.cpu-cap"]
        out, err, status = justcall(cmd)
        if status == 0:
            lines = out.split("\n")
            for line in lines:
                if "value:" not in line:
                    continue
                l = line.split("limit=")
                if len(l) == 2:
                    vcpus = l[-1][:l[-1].index(",")]
                    vcpus = str(float(vcpus)/100)
                    break

        cmd = [ZONECFG, "-z", self.name, "info", "capped-memory"]
        out, err, status = justcall(cmd)
        if status == 0:
            lines = out.split("\n")
            for line in lines:
                if "physical:" not in line:
                    continue
                l = line.split(": ")
                if len(l) == 2:
                    vmem = l[-1].strip()
                    if vmem.endswith("T"):
                        vmem = str(float(vmem[:-1])*1024*1024)
                    elif vmem.endswith("G"):
                        vmem = str(float(vmem[:-1])*1024)
                    elif vmem.endswith("M"):
                        vmem = vmem[:-1]
                    elif vmem.endswith("K"):
                        vmem = str(float(vmem[:-1])/1024)
                    break

        return {"vcpus": vcpus, "vmem": vmem}
