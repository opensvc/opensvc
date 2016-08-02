import re
import os
import glob
import time
import rcExceptions as ex
import resDisk
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilitiesLinux import major, get_blockdev_sd_slaves, \
                             devs_to_disks
from rcUtilities import which, justcall

class Disk(resDisk.Disk):
    startup_timeout = 10

    def __init__(self,
                 rid=None,
                 uuid=None,
                 shared=False,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        self.label = "md " + uuid
        self.uuid = uuid
        self.shared = shared
        self.mdadm = "/sbin/mdadm"
        resDisk.Disk.__init__(self,
                          rid=rid,
                          name=uuid,
                          type='disk.md',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

    def md_config_file_name(self):
        return os.path.join(rcEnv.pathvar, 'md_' + self.md_devname() + '.disklist')

    def md_config_import(self):
        p = self.md_config_file_name()
        if not os.path.exists(p):
            return set()
        with open(p, "r") as f:
            buff = f.read()
        disks = set(buff.split("\n"))
        disks -= set([""])
        return disks

    def md_config_export(self):
        from rcDevTreeLinux import DevTree
        dt = DevTree()
        dt.load()
        disks = self.devlist()
        disk_ids = set()
        for disk in disks:
            treedev = dt.get_dev_by_devpath(disk)
            if not treedev:
                continue
            disk_ids.add(treedev.alias)
        with open(self.md_config_file_name(), "w") as f:
             f.write("\n".join(disk_ids))

    def postsync(self):
        pass

    def down_state_alerts(self):
        if not self.shared:
            return rcStatus.NA
        devnames = self.md_config_import()
        devnames = set([d for d in devnames if not d.startswith("md")])
        if len(devnames) == 0:
            return rcStatus.NA

        from rcDevTreeLinux import DevTree
        dt = DevTree()
        dt.load()
        aliases = set([d.alias for d in dt.dev.values()])
        not_found = devnames - aliases
        if len(not_found) > 0:
            self.status_log("md member missing: %s" % ", ".join(sorted(list(not_found))))
            return rcStatus.WARN
        return rcStatus.NA

    def presync(self):
        if not self.shared:
            return
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "hb"]))
        if self.svc.force or s['overall'].status == rcStatus.UP:
            self.md_config_export()

    def files_to_sync(self):
        if not self.shared:
            return []
        return [self.md_config_file_name()]

    def md_devname(self):
        #devname = self.svc.svcname+"."+self.rid.replace("#", "")
        return self.uuid

    def md_devpath(self):
        devname = self.md_devname()
        l = glob.glob("/dev/disk/by-id/md-uuid-"+self.uuid) + glob.glob("/dev/md/"+devname)
        if len(l) == 0:
            raise ex.excError("unable to find a devpath for md")
        return l[0]

    def devpath(self):
        return "/dev/md/"+self.uuid

    def assemble(self):
        cmd = [self.mdadm, "--assemble", self.devpath(), "-u", self.uuid]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        if ret == 2:
            self.log.info("no changes were made to the array")
        elif ret != 0:
            raise ex.excError 
        else:
            self.wait_for_fn(self.has_it, self.startup_timeout, 1, errmsg="waited too long for devpath creation")

    def manage_stop(self):
        cmd = [self.mdadm, "--manage", self.md_devpath(), "--stop"]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.excError 

    def detail(self):
        try:
            devpath = self.md_devpath()
        except ex.excError as e:
            return "State : " + str(e)
        if not os.path.exists(devpath):
            return "State : devpath does not exist"
        cmd = [self.mdadm, "--detail", devpath]
        out, err, ret = justcall(cmd)
        if "cannot open /dev" in err:
            return "State : devpath does not exist"
        if ret != 0:
            if "does not appear to be active" in err:
                return "State : md does not appear to be active"
            raise ex.excError(err) 
        return out

    def detail_status(self):
        buff = self.detail()
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("State :"):
                return line.split(" : ")[-1]
        return "unknown"

    def has_it(self):
        state = self.detail_status().split(", ")[0]
        states = (
          "clean",
          "active",
        )
        if state in states:
            return True
        return False

    def is_up(self):
        if not self.has_it():
            return False
        state = self.detail_status()
        if state in ("clean", "active"):
            return True
        if state != "devpath does not exist":
            self.status_log(state)
        return False

    def _status(self, verbose=False):
        s = resDisk.Disk._status(self, verbose=verbose)
        if s in (rcStatus.STDBY_DOWN, rcStatus.DOWN):
             if self.down_state_alerts() == rcStatus.WARN:
                 return rcStatus.WARN
        if len(self.status_log_str) > 0:
            return rcStatus.WARN
        return s

    def do_start(self):
        if self.has_it():
            self.log.info("md %s is already assembled" % self.uuid)
            return 0
        self.can_rollback = True
        self.assemble()
        self._create_static_name()

    def do_stop(self):
        if not self.has_it():
            self.log.info("md %s is already down" % self.uuid)
            return
        self.manage_stop()

    def _create_static_name(self):
        self.create_static_name(self.md_devpath())

    def devlist(self):
        if self.devs != set():
            return self.devs

        try:
            devpath = self.md_devpath()
        except ex.excError as e:
            return self.devlist_inactive()
        if os.path.exists(devpath):
            self.devs = self.devlist_active()
        else:
            self.devs = self.devlist_inactive()
        return self.devs
            
    def devlist_inactive(self):
        devs = set()

        cmd = [self.mdadm, "-E", "--scan", "-v"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return devs
        lines = out.split("\n")

        if len(lines) < 2:
            return set()
        inblock = False
        for line in lines:
            if "UUID="+self.uuid in line:
                inblock = True
                continue
            if inblock and "devices=" in line:
                l = line.split("devices=")[-1].split(",")
                l = map(lambda x: os.path.realpath(x), l)
                devs |= set(l)
                break

        self.log.debug("found devs %s held by md %s" % (devs, self.uuid))
        return devs

    def devlist_active(self):
        devs = set()

        try:
            lines = self.detail().split("\n")
        except ex.excError as e:
            return set()

        if len(lines) < 2:
            return set()
        for line in lines[1:]:
            if "/dev/" not in line:
                continue
            devpath = line.split()[-1]
            devpath = os.path.realpath(devpath)
            devs.add(devpath)

        self.log.debug("found devs %s held by md %s" % (devs, self.uuid))
        return devs

    def disklist(self):
        if self.disks != set():
            return self.disks

        self.disks = set()

        members = self.devlist()
        self.disks = devs_to_disks(self, members)
        self.log.debug("found disks %s held by md %s" % (self.disks, self.uuid))
        return self.disks

    def provision(self):
        m = __import__("provDiskMdLinux")
        prov = getattr(m, "ProvisioningDisk")(self)
        prov.provisioner()

