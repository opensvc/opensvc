import os
import stat
import time
from datetime import datetime

import core.status
import core.exceptions as ex
import utilities.subsystems.zone
import utilities.lock
import utilities.os.sunos
from env import Env
from utilities.lazy import lazy
from utilities.subsystems.zfs import zfs_setprop, Dataset
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.mounts import Mounts
from utilities.proc import justcall, qcall, which
from .. import \
    BaseContainer, \
    KW_SNAP, \
    KW_SNAPOF, \
    KW_START_TIMEOUT, \
    KW_STOP_TIMEOUT, \
    KW_NO_PREEMPT_ABORT, \
    KW_NAME, \
    KW_HOSTNAME, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV

SYSIDCFG="/etc/sysidcfg"
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

DRIVER_GROUP = "container"
DRIVER_BASENAME = "zone"
KEYWORDS = [
    {
        "keyword": "delete_on_stop",
        "at": True,
        "candidates": (True, False),
        "text": "If set to ``true``, the zone configuration is deleted after a resource stop. The agent maintains an export of the configuration for the next start. This export is replicated to the other nodes and drp nodes so they can take over the zone even if it is completely hosted on a shared disk.",
        "default": False,
        "convert": "boolean",
    },
    {
        "keyword": "zonepath",
        "at": True,
        "text": "The zone path used to provision the container.",
        "provisioning": True,
    },
    {
        "keyword": "container_origin",
        "text": "The origin container having the reference container disk files.",
        "provisioning": True
    },
    {
        "keyword": "rootfs",
        "text": "Sets the root fs directory of the container",
        "required": False,
        "provisioning": True
    },
    KW_SNAP,
    KW_SNAPOF,
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    data = []
    if which("zoneadm"):
        data.append("container.zone")
    return data

class ContainerZone(BaseContainer):
    """
    Zone container resource driver.
    """
    def __init__(self,
                 delete_on_stop=False,
                 zonepath=None,
                 container_origin=None,
                 rootfs=None,
                 snap=None,
                 snapof=None,
                 **kwargs):
        super(ContainerZone, self).__init__(type="container.zone", **kwargs)
        self.delete_on_stop = delete_on_stop
        self.delayed_noaction = True
        self.container_origin = container_origin
        self.snapof = snapof
        self.snap = snap
        self.kw_zonepath = zonepath

    @lazy
    def clone(self):
        if not self.snap:
            return "rpool/zones/" + self.name
        return self.snap

    @lazy
    def runmethod(self):
        return ["/usr/sbin/zlogin", self.name]

    @lazy
    def zone_cf(self):
        return "/etc/zones/%s.xml" % self.name

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
            raise ex.Error(err)

    def get_zonepath_from_zonecfg_cmd(self):
        cmd = [ZONECFG, "-z", self.name, "info", "zonepath"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("unable to determine zonepath using %s" % " ".join(cmd))
        zp = out.replace("zonepath: ", "").strip()
        return zp

    def get_zonepath_from_zonecfg_export(self):
        fpath = self.zone_cfg_path()
        if not os.path.exists(fpath):
            raise ex.Error("zone config export file %s not found. "
                              "unable to determine zonepath" % fpath)
        with open(fpath, "r") as f:
            buff = f.read()
        for line in buff.split("\n"):
            if "set zonepath" in line:
                return line.split("=")[-1].strip()
        raise ex.Error("set zonepath command not found in %s" % fpath)

    def zonecfg(self, zonecfg_args=None):
        zonecfg_args = zonecfg_args or []
        cmd = [ZONECFG, "-z", self.name] + zonecfg_args
        ret, out, err = self.vcall(cmd, err_to_info=True)
        if ret != 0:
            msg = "%s failed status: %i\n%s" % (" ".join(cmd), ret, out)
            self.log.error(msg)
            raise ex.Error(msg)
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
            raise ex.Error("%s failed in %s - ret %i" % (" ".join(cmd), duration, ret))
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
            raise ex.Error("'%s' execution error:\n%s"%(" ".join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        dst = os.path.realpath(self.zonepath + "/root/" + dst)
        cmd = ["cp", src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(" ".join(cmd), err))
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
                raise ex.Error
        options = []
        if utilities.os.sunos.get_solaris_version() >= 11.3:
            options += ["-x", "deny-zbe-clone"]
        try:
            self.umount_fs_in_zonepath()
            self.zoneadm("attach", options)
        except ex.Error:
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
            raise ex.Error
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
            raise ex.Error("zone should be running")
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
            return core.status.UP
        return core.status.DOWN

    def zone_refresh(self):
        self.unset_lazy("zone_data")
        self.unset_lazy("state")
        self.unset_lazy("brand")
        self.unset_lazy("zonepath")

    @lazy
    def state(self):
        if self.zone_data is None:
            raise ex.Error("zone %s does not exist" % self.name)
        else:
            return self.zone_data.get("state")

    @lazy
    def zonepath(self):
        zp = self.zone_data.get("zonepath")
        if zp:
            return zp
        try:
            zp = self.get_zonepath_from_zonecfg_cmd()
        except ex.Error:
            try:
                zp = self.get_zonepath_from_zonecfg_export()
            except ex.Error:
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
            raise ex.Error("Unexpected zoneadm list output: %s"%out)
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
            raise ex.Error("zone is not running")
        cmd = [PGREP, "-z", self.name, "-f", INIT]
        out, err, st = justcall(cmd)
        if st != 0:
            raise ex.Error("fail to detect zone init process")
        pids = " ".join(out.split("\n")).rstrip()
        cmd = [PWAIT, pids]
        self.log.info("wait for zone init process %s termination" % (pids))
        if qcall(cmd) != 0:
            raise ex.Error("failed " + " ".join(cmd))
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

        mounts = Mounts()
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
            super(ContainerZone, self).provision()
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
            super(ContainerZone, self).unprovision()

    def presync(self):
        self.export_zone_cfg()

    def files_to_sync(self):
        return [self.zone_cfg_path()]

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

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


    def pre_unprovision_stop(self):
        self._stop()

    def post_provision_start(self):
        pass

    def sysid_network(self):
        """
         network_interface=l226z1 {primary
          hostname=zone1-32
          ip_address=172.30.5.232
          netmask=255.255.255.0
          protocol_ipv6=no
          default_route=172.30.5.1}
        """
        cf = os.path.join(Env.paths.pathetc, self.svc.name+'.conf')
        s = ""

        for r in self.svc.get_resources(["ip"]):
            # Add mandatory tags for sol11 zones
            r.tags.add("noaction")
            r.tags.add("noalias")
            r.tags.add("exclusive")
            r.tags.remove("preboot")
            r.tags.remove("postboot")

            try:
                default_route = self.svc.conf_get(r.rid, "gateway")
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                # Add nonrouted tag if no gateway provisioning keyword is passed
                self.tags.add("nonrouted")
                continue

            try:
                netmask = self.svc.oget(r.rid, "netmask")
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                continue

            if s == "":
                s += "network_interface=%s {primary\n"%r.ipdev
                s += " hostname=%s\n"%r.ipname
                s += " ip_address=%s\n"%r.addr
                s += " netmask=%s\n"%netmask
                s += " protocol_ipv6=no\n"
                s += " default_route=%s}\n"%default_route

            # save new service env file
        self.svc.set_multi(["%s.tags=%s" % (r.rid, ' '.join(r.tags))])
        return s

    def get_tz(self):
        if "TZ" not in os.environ:
            return "MET"
        tz = os.environ["TZ"]
        tzp = os.path.join(os.sep, "etc", tz)
        if os.path.exists(tzp) and self.osver >= 11:
            p = os.path.realpath(tzp)
            l = p.split('zoneinfo/')
            if len(l) != 2:
                return "MET"
            return l[-1]
        else:
            return tz

    def get_ns(self):
        "return (domain, nameservers) detected from resolv.conf"
        p = os.path.join(os.sep, 'etc', 'resolv.conf')
        domain = None
        search = []
        nameservers = []
        with open(p) as f:
            for line in f.readlines():
                if line.strip().startswith('search'):
                    l = line.split()
                    if len(l) > 1:
                        search = l[1:]
                if line.strip().startswith('domain'):
                    l = line.split()
                    if len(l) > 1:
                        domain = l[1]
                if line.strip().startswith('nameserver'):
                    l = line.split()
                    if len(l) > 1 and l[1] not in nameservers:
                        nameservers.append(l[1])
        return (domain, nameservers, search)

    def create_sysidcfg(self, zone=None):
        self.log.info("creating zone sysidcfg file")
        if self.osver >= 11.0:
            self._create_sysidcfg_11(zone)
        else:
            self._create_sysidcfg_10(zone)

    def _create_sysidcfg_11(self, zone=None):
        try:
            domain, nameservers, search = self.get_ns()
            if domain is None and len(search) > 0:
                domain = search[0]
            if domain is None or len(nameservers) == 0:
                name_service="name_service=none"
            else:
                name_service = "name_service=DNS {domain_name=%s name_server=%s search=%s}\n" % (
                  domain,
                  ",".join(nameservers),
                  ",".join(search)
                )

            sysidcfg_dir = os.path.join(self.var_d)
            sysidcfg_filename = os.path.join(sysidcfg_dir, 'sysidcfg')
            contents = ""
            contents += "keyboard=US-English\n"
            contents += "system_locale=C\n"
            contents += "timezone=%s\n"%self.get_tz()
            contents += "terminal=vt100\n"
            contents += "timeserver=localhost\n"
            contents += self.sysid_network()
            contents += "root_password=NP\n"
            contents += "security_policy=NONE\n"
            contents += name_service

            try:
                os.makedirs(sysidcfg_dir)
            except:
                pass
            with open(sysidcfg_filename, "w") as sysidcfg_file:
                sysidcfg_file.write(contents)
            os.chdir(sysidcfg_dir)
            self.zonecfg_xml = os.path.join(sysidcfg_dir, "sc_profile.xml")
            try:
                os.unlink(self.zonecfg_xml)
            except:
                pass
            cmd = ['/usr/sbin/js2ai', '-s']
            out, err, ret = justcall(cmd)
            if not os.path.exists(self.zonecfg_xml):
                raise ex.Error("js2ai conversion error")
        except Exception as e:
            self.svc.save_exc()
            raise ex.Error("exception from %s: %s during create_sysidcfg file" % (e.__class__.__name__, e.__str__()))

    def _create_sysidcfg_10(self, zone=None):
        try:
            name_service = "name_service=NONE\n"

            sysidcfg_filename = zone.zonepath + "/root" + SYSIDCFG
            sysidcfg_file = open(sysidcfg_filename, "w" )
            contents = ""
            contents += "system_locale=C\n"
            contents += "timezone=MET\n"
            contents += "terminal=vt100\n"
            contents += "timeserver=localhost\n"
            contents += "security_policy=NONE\n"
            contents += "root_password=NP\n"
            contents += "nfs4_domain=dynamic\n"
            contents += "network_interface=NONE {hostname=%(zonename)s}\n" % {"zonename":zone.name}
            contents += name_service

            sysidcfg_file.write(contents)
            sysidcfg_file.close()
        except Exception as exc:
            raise ex.Error("exception from %s: %s during create_sysidcfg file" % (exc.__class__.__name__, exc.__str__()))

    def test_net_interface(self, intf):
        cmd = ['dladm', 'show-link', intf]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        return False

    def zone_configure_net(self, zone=None):
        if zone is None:
            zone = self
        cmds = []
        for r in self.svc.get_resources(["ip"]):
            if not self.test_net_interface(r.ipdev):
                raise ex.Error("Missing interface: %s" % r.ipdev)
            cmds.append("add net ; set physical=%s ; end" % r.ipdev)
        for cmd in cmds:
            zone.zonecfg([cmd])

    def zone_configure(self, zone=None):
        """
            configure zone, if zone is None, configure self
        """
        if zone is None:
            zone = self

        if self.osver >= 11.0 and self.container_origin:
            cmd = "create -t " + self.container_origin
        else:
            cmd = "create"

        cmd += "; set zonepath=" + zone.zonepath

        if zone.state is None:
            zone.zonecfg([cmd])
            if zone.state != "configured":
                raise ex.Error("zone %s is not configured" % zone.name)

        if self.osver >= 11.0:
            try:
                self.zone_configure_net(zone)
            except:
                zone.zonecfg(["delete", "-F"])
                raise

    def create_zone2clone(self):
        if os.path.exists(self.kw_zonepath):
            try:
                os.chmod(self.kw_zonepath, 0o0700)
            except:
                pass
        if self.osver >= 11.0:
            self._create_zone2clone_11()
        else:
            self._create_zone2clone_10()

    def _create_zone2clone_11(self):
        zonename = self.container_origin
        zone2clone = ContainerZone(rid="container#skelzone", name=zonename)
        zone2clone.log = self.log
        if zone2clone.state == "installed":
            return
        self.zone_configure(zone=zone2clone)
        if zone2clone.state != "configured":
            raise(ex.Error("zone %s is not configured" % (zonename)))
        self.create_sysidcfg(zone2clone)
        #zone2clone.zoneadm("clone", ['-c', self.zonecfg_xml, self.container_origin])
        zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(ex.Error("zone %s is not installed" % (zonename)))
        brand = zone2clone.brand
        if brand == "native":
            zone2clone.boot_and_wait_reboot()
        elif brand == "ipkg":
            zone2clone.zone_boot()
        else:
            raise(ex.Error("zone brand: %s not yet implemented" % (brand)))
        zone2clone.wait_multi_user()
        zone2clone.stop()
        if zone2clone.state != "installed":
            raise(ex.Error("zone %s is not installed" % (zonename)))

    def _create_zone2clone_10(self):
        """verify if self.container_origin zone is installed
        else configure container_origin if required
        then install container_origin if required
        """
        zonename = self.container_origin
        zone2clone = ContainerZone(rid="container#skelzone", name=zonename)
        zone2clone.log = self.log
        if zone2clone.state == "installed":
            return
        self.zone_configure(zone=zone2clone)
        if zone2clone.state != "configured":
            raise(ex.Error("zone %s is not configured" % (zonename)))
        zone2clone.zoneadm("install")
        if zone2clone.state != "installed":
            raise(ex.Error("zone %s is not installed" % (zonename)))
        self.create_sysidcfg(zone2clone)
        brand = zone2clone.brand
        if brand == "native":
            zone2clone.boot_and_wait_reboot()
        elif brand == "ipkg":
            zone2clone.zone_boot()
        else:
            raise(ex.Error("zone brand: %s not yet implemented" % (brand)))
        zone2clone.wait_multi_user()
        zone2clone.stop()
        if zone2clone.state != "installed":
            raise(ex.Error("zone %s is not installed" % (zonename)))

    def create_cloned_zone(self):
        zone = self
        if zone.state == "running":
            self.log.info("zone %s already running"%zone.name)
            return
        if zone.state == "configured":
            if self.osver >= 11.0:
                self._create_cloned_zone_11(zone)
            else:
                self._create_cloned_zone_10(zone)
        if zone.state != "installed":
            raise(ex.Error("zone %s is not installed" % (zone.name)))

    def _create_cloned_zone_11(self, zone):
        zone.zoneadm("clone", ['-c', self.zonecfg_xml, self.container_origin])

    def _create_cloned_zone_10(self, zone):
        zone.zoneadm("clone", [self.container_origin])

    def create_zonepath(self):
        """create zonepath dataset from clone of snapshot of self.snapof
        snapshot for self.snapof will be created
        then cloned to self.clone
        """
        zonename = self.name
        source_ds = Dataset(self.snapof)
        if source_ds.exists(type="filesystem") is False:
            raise(ex.Error("source dataset doesn't exist " + self.snapof))
        snapshot = source_ds.snapshot(zonename)
        snapshot.clone(self.clone, ['-o', 'mountpoint=' + self.kw_zonepath])

    def provisioner(self, need_boot=True):
        """provision zone
        - configure zone
        - if snapof and zone brand is native
           then create zonepath from snapshot of snapof
           then attach zone
        - if snapof and zone brand is ipkg
           then try to detect zone associated with snapof
           then define container_origin
        - if container_origin
           then clone  container_origin
        - create sysidcfg
        - if need_boot boot and wait multiuser
        """
        self.osver = utilities.os.sunos.get_solaris_version()
        self.zone_configure()

        if self.osver >= 11:
            self.create_sysidcfg(self)
        else:
            if self.snapof is not None and self.brand == 'native':
                self.create_zonepath()
                self.zoneadm("attach", ["-F"])
            elif self.snapof is not None and self.brand == 'ipkg':
                zones = utilities.subsystems.zone.Zones()
                src_dataset = Dataset(self.snapof)
                zonepath = src_dataset.getprop('mountpoint')
                self.container_origin = zones.zonename_from_zonepath(zonepath).zonename
                self.log.info("source zone is %s (detected from snapof %s)" % (self.container_origin, self.snapof))

        if self.container_origin is not None:
            lockname='create_zone2clone-' + self.container_origin
            lockfile = os.path.join(Env.paths.pathlock, lockname)
            self.log.info("wait get lock %s"%(lockname))
            try:
                lockfd = utilities.lock.lock(timeout=1200, delay=5, lockfile=lockfile)
            except:
                raise(ex.Error("failure in get lock %s"%(lockname)))
            try:
                self.create_zone2clone()
            except:
                utilities.lock.unlock(lockfd)
                raise
            utilities.lock.unlock(lockfd)
            self.create_cloned_zone()

        if self.osver < 11:
            self.create_sysidcfg(self)

        if need_boot is True:
            self.zone_boot()
            self.wait_multi_user()

        self.log.info("provisioned")
        return True
