import os
import stat
import time
from datetime import datetime
from shutil import copy

import core.status
import core.exceptions as ex
import utilities.subsystems.zone
import utilities.lock
import utilities.os.sunos
from env import Env
from utilities.lazy import lazy
from utilities.net.converters import to_cidr, cidr_to_dotted
from utilities.net.getaddr import getaddr
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
ZLOGIN = "/usr/sbin/zlogin"
PGREP = "/usr/bin/pgrep"
PWAIT = "/usr/bin/pwait"
INIT = "/sbin/init"
SVCS = "/usr/bin/svcs"
SYS_UNCONFIG = "/usr/sbin/sys-unconfig"
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
    {
        "keyword": "sc_profile",
        "text": "The system configuration profile xml file for container provisioning. If not set, a configuration profile will be automatically created.",
        "required": False,
        "provisioning": True
    },
    {
        "keyword": "ai_manifest",
        "text": "The Automated Installer manifest xml file for container provisioning. If not set, default manifest will be used.",
        "required": False,
        "provisioning": True
    },
    {
        "keyword": "brand",
        "text": "The zone brand. If not set default brand will be used",
        "candidates": ["solaris", "solaris10", "native"],
        "required": False,
        "provisioning": True
    },
    {
        "keyword": "install_archive",
        "text": "The install archive to use during zonedm install '-a <archive>'. If both container_origin and install_archive are set, but container_origin is not yet provisioned, container_origin will be created from <install_archive>.",
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


PROVISIONED_STATES = ['installed', 'running']


def driver_capabilities(node=None):
    data = []
    if which("zoneadm"):
        data.append("container.zone")
    if os.path.exists('/etc/zones/SYSsolaris.xml'):
        data.append("container.zone.brand-solaris")
    elif os.path.exists('/etc/zones/SUNWdefault.xml'):
        data.append("container.zone.brand-native")
    if os.path.exists('/etc/zones/SYSsolaris10.xml'):
        data.append("container.zone.brand-solaris10")
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
                 sc_profile=None,
                 ai_manifest=None,
                 provision_net_type="anet",
                 brand=None,
                 install_archive=None,
                 **kwargs):
        super(ContainerZone, self).__init__(type="container.zone", **kwargs)
        self.sysidcfg = None
        self.delete_on_stop = delete_on_stop
        self.delayed_noaction = True
        self.container_origin = container_origin
        self.snapof = snapof
        self.snap = snap
        self.kw_zonepath = zonepath
        self.sc_profile = sc_profile
        self.ai_manifest = ai_manifest
        self.provision_net_type = provision_net_type
        if self.has_capability("container.zone.brand-solaris"):
            self.default_brand = 'solaris'
        elif self.has_capability("container.zone.brand-native"):
            self.default_brand = 'native'
        else:
            self.default_brand = None
        self.boot_config_file = None
        self.kw_brand = brand
        self.install_archive = install_archive

    @lazy
    def clone(self):
        if not self.snap:
            return "rpool/zones/" + self.name
        return self.snap

    @lazy
    def runmethod(self):
        return [ZLOGIN, self.name]

    @lazy
    def zone_cf(self):
        return "/etc/zones/%s.xml" % self.name

    @lazy
    def osver(self):
        return utilities.os.sunos.get_solaris_version()

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
        return self.zone_unconfigure()

    def zone_unconfigure(self):
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
        self.zone_refresh()

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
        if self.state is None:
            self.log.info("skip zone %s halt, no such zone", self.name)
            return 0
        elif self.state == "running":
            ret, out, err = self.vcall([ZLOGIN, self.name, "/sbin/init", "0"])
        elif self.state not in ["shutting_down", "down", "installed"]:
            self.log.warning("unable to halt zone %s state %s", self.name, self.state)
            return 0

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
            self.log.info("zone %s does not exist" % self.name)
            return
        else:
            return self.zone_data.get("state")

    @lazy
    def zonepath(self):
        """
        method that returns zonepath
        from zoneadm output
        or zonecfg info
        or zonecfg exported info
        or from resource kw_zonepath value
        else return None (when creating zone2clone, we don't know zonepath before
        zonecfg create command launched)
        """
        if self.zone_data is not None:
            zp = self.zone_data.get("zonepath")
            if zp:
                return zp
        try:
            return self.get_zonepath_from_zonecfg_cmd()
        except ex.Error:
            pass
        try:
            return self.get_zonepath_from_zonecfg_export()
        except ex.Error:
            pass
        if self.kw_zonepath:
            return self.kw_zonepath
        else:
            return None

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
        out, err, ret = justcall([ZONEADM, "-z", self.name, "list", "-p"])
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
        Boot freshly installed zones, then wait for automatic zone reboot
            boot zone
            ensure for zone init process is running
            wait for 2 'system boot' (this is only usable on freshly installed zones)
            wait for zone running
            wait for zone operational

        We wait for 2 'system boot', this is only usable on freshly installed zones
        """
        def wait_boot_count(count, max_retries=240):
            retries = 0
            self.log.info('wait for %s boot count is %s (max retries %s)', self.name, count, max_retries)
            cmd = [ZLOGIN, self.name, 'last', 'reboot']
            while retries < max_retries:
                out, err, st = justcall(cmd)
                if st == 0:
                    reboots = len([line for line in out.split('\n') if 'system boot' in line])
                    self.log.info('%s boot count: %s', self.name, reboots)
                    if reboots >= count:
                        return True
                time.sleep(1)
                retries += 1

        self.log.info("wait for zone %s boot and reboot...", self.name)
        self.zone_boot()
        if self.is_running is False:
            raise ex.Error("zone is not running")
        cmd = [PGREP, "-z", self.name, "-f", INIT]
        out, err, st = justcall(cmd)
        if st != 0:
            raise ex.Error("fail to detect zone init process")
        wait_boot_count(2)
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

    def update_ip_tags(self):
        if self.osver < 11.0:
            return
        ip_kws = []
        need_save = False
        if self.brand in ['native', 'solaris10']:
            # pickup only first rid because of sysidcfg only setup 1st nic
            rids = self.get_encap_ip_rids()[:1]
        else:
            rids = self.get_encap_ip_rids()
        for rid in rids:
            # Add mandatory tags for sol11 zones
            tags = self.get_encap_conf(rid, 'tags')
            for tag in ['noaction', 'noalias', 'exclusive']:
                if tag not in tags:
                    tags.add(tag)
                    need_save = True
            for tag in ['preboot', 'postboot']:
                if tag in tags:
                    tags.remove(tag)
                    need_save = True

            try:
                self.get_encap_conf(rid, 'gateway')
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                # Add nonrouted tag if no gateway provisioning keyword is passed
                tags.add("nonrouted")
                need_save = True

            ip_kws += ["%s.tags=%s" % (rid, ' '.join(tags))]
        if need_save and ip_kws:
            # update service env file
            self.svc.set_multi(ip_kws)

    def get_encap_ip_rids(self):
        return [rid for rid in self.svc.conf_sections(cat='ip')
                if 'encap' in self.get_encap_conf(rid, 'tags')]

    def get_encap_conf(self, rid, kw):
        return self.svc.oget(rid, kw, impersonate=self.name)

    def get_install_ipv4_interfaces(self):
        """
         returns list of InstallIpv4Interface tha will be used to create sc_profile
        """
        from .configuration_profile import InstallIpv4Interface
        ipv4_interfaces = []
        for rid in self.get_encap_ip_rids():
            try:
                ipdevext = self.get_encap_conf(rid, 'ipdevext')
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                ipdevext = 'v4'
            ipdev = self.get_encap_conf(rid, 'ipdev')
            ipv4_name = '%s/%s' % (ipdev, ipdevext)

            try:
                default_route = self.get_encap_conf(rid, 'gateway')
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                default_route = None

            try:
                netmask = to_cidr(self.get_encap_conf(rid, 'netmask'))
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                continue
            ipname = self.get_encap_conf(rid, 'ipname')
            addr = getaddr(ipname, True)
            ipv4_interface = InstallIpv4Interface(ipv4_name,
                                                  static_address='%s/%s' % (addr, netmask),
                                                  address_type='static',
                                                  default_route=default_route,
                                                  id=len(ipv4_interfaces))
            ipv4_interfaces.append(ipv4_interface)
        return ipv4_interfaces

    def get_sysidcfg_network_interfaces(self):
        network_interfaces = []
        for rid in self.get_encap_ip_rids():
            ipdev = self.get_encap_conf(rid, 'ipdev')
            try:
                default_route = self.get_encap_conf(rid, 'gateway')
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                default_route = None

            try:
                netmask_conf = self.get_encap_conf(rid, 'netmask')
            except (ex.RequiredOptNotFound, ex.OptNotFound):
                netmask_conf = None
            if netmask_conf is None:
                netmask = None
            else:
                netmask = cidr_to_dotted(to_cidr(netmask_conf))
            ipname = self.get_encap_conf(rid, 'ipname')
            addr = getaddr(ipname, True)
            if len(network_interfaces) == 0:
                network_interface = "network_interface=PRIMARY {primary hostname=%s\n" % self.name
                network_interface += "    default_route=%s\n" % default_route
            else:
                network_interface = "network_interface=%s {\n" % ipdev
            network_interface += "    ip_address=%s\n" % addr
            if netmask:
                network_interface += "    netmask=%s\n" % netmask
            network_interface += "    protocol_ipv6=no}\n"
            network_interfaces.append(network_interface)
        return network_interfaces

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
        "return (domain, nameservers, search) detected from resolv.conf"
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

    def prepare_boot_config(self):
        if self.brand in ['solaris']:
            self.create_sc_profile()
            self.boot_config_file = self.sc_profile
        elif self.brand in ['solaris10']:
            self.create_sysidcfg()
            self.boot_config_file = self.sysidcfg
        elif self.brand in ['native']:
            self.create_sysidcfg()

    def install_boot_config(self):
        if self.brand in ['native'] and self.sysidcfg:
            sysidcfg_filename = self.zonepath + "/root" + SYSIDCFG
            self.log.info('create %s', sysidcfg_filename)
            copy(self.sysidcfg, sysidcfg_filename)
            os.chmod(sysidcfg_filename, 0o0600)

    def create_sc_profile(self):
        if self.sc_profile:
            if not os.path.exists(self.sc_profile):
                message = 'sc_profile %s does not exists' % self.sc_profile
                self.log.warning(message)
                raise ex.Error(message)
            self.log.info('using provided sc_profile %s', self.sc_profile)
            return
        else:
            self.sc_profile = os.path.join(self.var_d, 'sc_profile.xml')
            self.log.info('creating sc_profile %s', self.sc_profile)
        try:
            from .configuration_profile import ScProfile, InstallIpv4Interface
            domain, nameservers, searchs = self.get_ns()
            sc_profile = ScProfile(sc_profile_file=self.sc_profile)
            sc_profile.set_nodename(self.name)
            sc_profile.set_localtime('Europe/Paris')
            for ipv4_interface in self.get_install_ipv4_interfaces():
                sc_profile.add_ipv4_interface(ipv4_interface)
            sc_profile.set_environment({'LANG': 'C'})
            if searchs or nameservers:
                sc_profile.set_dns_client(searches=searchs, nameservers=nameservers)
                sc_profile.set_name_service_switch({'host': 'files dns'})
            sc_profile.write()
        except Exception as e:
            self.svc.save_exc()
            raise ex.Error("exception from %s: %s during create_sc_profile" % (e.__class__.__name__, e.__str__()))

    def set_sysidcfg_unconfig(self):
        self.sysidcfg = os.path.join(self.var_d, 'sysidcfg.unconfig')
        self.log.info('creating sysidcfg %s', self.sysidcfg)
        contents = "system_locale=C\n"
        contents += "timezone=MET\n"
        contents += "terminal=vt100\n"
        contents += "timeserver=localhost\n"
        contents += "security_policy=NONE\n"
        contents += "root_password=NP\n"
        contents += "auto_reg=disable\n"
        contents += "nfs4_domain=dynamic\n"
        contents += "network_interface=NONE {hostname=%s}\n" % self.name
        contents += "name_service=NONE\n"
        with open(self.sysidcfg, "w") as sysidcfg_file:
            sysidcfg_file.write(contents)

    def create_sysidcfg(self):
        if self.sysidcfg:
            if not os.path.exists(self.sysidcfg):
                message = 'sysidcfg %s does not exists' % self.sysidcfg
                self.log.warning(message)
                raise ex.Error(message)
            self.log.info('using provided sysidcfg %s', self.sc_profile)
            return
        else:
            sysidcfg_network_interfaces = self.get_sysidcfg_network_interfaces()
            if not sysidcfg_network_interfaces:
                self.log.info('no network interface found, use sysidcfg unconfig')
                self.set_sysidcfg_unconfig()
                return
            self.sysidcfg = os.path.join(self.var_d, 'sysidcfg')
            self.log.info('creating sysidcfg %s', self.sysidcfg)
        try:
            contents = ""
            contents += "system_locale=C\n"
            contents += "timezone=MET\n"
            contents += "terminal=vt100\n"
            contents += "timeserver=localhost\n"
            contents += "security_policy=NONE\n"
            contents += "root_password=NP\n"
            contents += "auto_reg=disable\n"
            contents += "nfs4_domain=dynamic\n"
            for network_interface in sysidcfg_network_interfaces[:1]:
                contents += network_interface + '\n'
            domain, nameservers, searchs = self.get_ns()
            if not nameservers or not domain:
                name_service = "name_service=NONE"
            else:
                name_service = "name_service=DNS {domain_name=%s" % domain
                name_service += "\n    name_server=%s" % ','.join(nameservers)
                if searchs:
                    name_service += "\n    search=%s" % ','.join(searchs)
                name_service += "\n    }\n"
            contents += name_service

            with open(self.sysidcfg, "w") as sysidcfg_file:
                sysidcfg_file.write(contents)
        except Exception as exc:
            raise ex.Error("exception from %s: %s during create_sysidcfg file" % (exc.__class__.__name__, exc.__str__()))

    def test_net_interface(self, intf):
        cmd = ['dladm', 'show-link', intf]
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        return False

    def zone_configure_net(self):
        if not self.provision_net_type:
            return
        if self.provision_net_type in ['no-anet', 'no-net']:
            self.zonecfg(['remove -F %s' % self.provision_net_type.replace('no-', '')])
            return

        for rid in self.svc.conf_sections(cat='ip'):
            ipdev = self.svc.oget(rid, 'ipdev', impersonate=self.name)
            if not self.test_net_interface(ipdev):
                raise ex.Error("Missing interface: %s" % ipdev)
            if self.provision_net_type == 'anet':
                cmd = [ZONECFG, "-z", self.name, 'select anet lower-link=%s linkname=%s; info;end' % (ipdev, ipdev)]
                out, err, ret = justcall(cmd)
                if ret != 0:
                    self.zonecfg(['add anet; set lower-link=%s; set linkname=%s; end' % (ipdev, ipdev)])
            elif self.provision_net_type == 'net':
                cmd = [ZONECFG, "-z", self.name, 'select net physical=%s; info; end' % ipdev]
                out, err, ret = justcall(cmd)
                if ret != 0:
                    self.zonecfg(['add net; set physical=%s; end' % ipdev])

    def _brand_to_create(self):
        return self.kw_brand or self.default_brand

    def zone_configure(self):
        "Ensure zone is at least configured"
        if self.state is None:
            if self.kw_brand == 'native' and not self.has_capability("container.zone.brand-native"):
                raise ex.Error("node has no capability to create brand %s zone" % self.kw_brand)
            if self.container_origin:
                cmd = "create -t " + self.container_origin
            elif self.kw_brand and self.kw_brand != self.default_brand and self.kw_brand not in ['native']:
                cmd = "create -t SYS"+ self.kw_brand
            else:
                cmd = "create"

            if self.zonepath:
                cmd += "; set zonepath=" + self.zonepath

            self.zonecfg([cmd])
            self.zone_refresh()
            if self.state != "configured":
                raise ex.Error("zone %s is not configured" % self.name)

        if self.brand in ['solaris', 'solaris10']:
            try:
                self.zone_configure_net()
            except:
                raise ex.Error('Error during zone_configure_net')

    def install_origin(self):
        """
        verify if self.container_origin zone is installed
        else configure container_origin if required
        then install container_origin if required
        """
        if self.state == "installed":
            return
        self.provision_zone()
        if self.brand == "native":
            self.boot_and_wait_reboot()
        else:
            self.zone_boot()
        self.wait_multi_user()
        if self.brand == "native":
            self.log.info('call in zone %s %s' % (self.name, SYS_UNCONFIG))
            justcall([ZLOGIN, self.name, SYS_UNCONFIG], input='y\n')
        self.halt()
        if self.state != "installed":
            raise(ex.Error("zone %s is not installed" % (self.name)))

    def create_cloned_zone(self):
        if self.state == "configured":
            if self.boot_config_file:
                self.zoneadm("clone", ['-c', self.boot_config_file, self.container_origin])
            else:
                self.zoneadm("clone", [self.container_origin])
            self.update_ip_tags()
        self.zone_refresh()
        if self.state != "installed":
            raise(ex.Error("zone %s is not installed" % self.name))

    def create_snaped_zone(self):
        self.create_zonepath()
        self.zoneadm("attach", ["-F"])
        self.zone_refresh()

    def install_zone(self):
        zonepath = self.zonepath
        if zonepath and os.path.exists(zonepath):
            try:
                os.chmod(zonepath, 0o0700)
            except:
                pass
        args = []
        if self.boot_config_file:
            args = ['-c', self.boot_config_file]
        if self.ai_manifest and self.brand in ['solaris']:
            args += ['-m', self.ai_manifest]
        if self.install_archive:
            args += ['-a', self.install_archive, '-u']
        self.zoneadm("install", args)
        self.zone_refresh()

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

    def create_container_origin(self):
        lockfile = os.path.join(self.var_d, 'create_zone2clone-' + self.container_origin)
        try:
            with utilities.lock.cmlock(timeout=1200, delay=4, lockfile=lockfile):
                container = self.origin_factory()
                container.install_origin()
        except utilities.lock.LOCK_EXCEPTIONS as exc:
            raise ex.AbortAction(str(exc))

    def origin_factory(self):
        name = self.container_origin
        kwargs = {'brand': self._brand_to_create()}
        if self._brand_to_create() == 'solaris':
            kwargs['provision_net_type'] = 'no-anet'
            kwargs['sc_profile'] = '/usr/share/auto_install/sc_profiles/unconfig.xml'
            if self.ai_manifest:
                kwargs['ai_manifest'] = self.ai_manifest
        if self._brand_to_create() in ['solaris10']:
            kwargs['provision_net_type'] = 'no-anet'
        if self._brand_to_create() in ['native']:
            kwargs['provision_net_type'] = 'no-net'
            kwargs['zonepath'] = '/zones/%s' % name
        if self.install_archive:
            kwargs['install_archive'] = self.install_archive
        origin = ContainerZone(rid="container#skelzone", name=name, **kwargs)
        origin.svc = self.svc
        if self._brand_to_create() in ['native', 'solaris10']:
            origin.set_sysidcfg_unconfig()
        return origin

    def provisioner(self, need_boot=True):
        """provision zone
        - if snapof and zone brand is native
           configure zone
           then create zonepath from snapshot of snapof
           then attach zone
        - else if container_origin
           create clone container_origin if not yet created
           configure zone
           clone container origin to zone

        - if need_boot boot and wait multiuser
        """
        state = self.state
        if state in PROVISIONED_STATES:
            self.log.info('zone already provisioned: state is %s', state)
            return True
        self.log.info('provisioner start')

        # failfast setting for provisioning
        if self.snapof and self.container_origin:
            self.log.error('provision error: container_origin is not compatible with snapof')
            return False
        elif self.snapof and self._brand_to_create() != 'native':
            msg = 'provision error: snapof is only available with native zone, try container_origin instead'
            self.log.error(msg)
            return False

        if self.container_origin:
            self.create_container_origin()
        self.provision_zone()
        self.can_rollback = True

        if need_boot is True:
            self.zone_boot()
            self.wait_multi_user()

        self.log.info("provisioned")
        return True

    def provision_zone(self):
        if self.state in PROVISIONED_STATES:
            self.log.info("zone %s already in state %s" % (self.name, self.state))
            return
        self.zone_configure()
        self.prepare_boot_config()
        self.make_zone_installed()
        self.install_boot_config()

    def make_zone_installed(self):
        if self.snapof:
            # we are on brand native
            self.create_snaped_zone()
        elif self.container_origin:
            self.create_cloned_zone()
        else:
            self.install_zone()

    def unprovisioner(self):
        self.log.info('unprovisioner start')
        state = self.state
        if state == 'configured':
            self.zone_unconfigure()
        elif state:
            msg = 'unable to unprovision zone in state %s' % state
            self.log.error(msg)
            raise ex.Error(msg)
        if os.path.exists(self.zone_cfg_path()):
            os.remove(self.zone_cfg_path())
