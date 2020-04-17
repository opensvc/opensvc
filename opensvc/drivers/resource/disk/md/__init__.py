import json
import os

import core.exceptions as ex
import core.status
import utilities.devices.linux
from os.path import exists as path_exists
from .. import BaseDisk, BASE_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS
from utilities.cache import cache
from utilities.converters import convert_size
from utilities.fcache import fcache
from utilities.lazy import lazy
from utilities.proc import justcall, which

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "md"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "uuid",
        "at": True,
        "text": "The md uuid to use with mdadm assemble commands"
    },
    {
        "keyword": "devs",
        "at": True,
        "default": [],
        "convert": "list",
        "provisioning": True,
        "example": "/dev/rbd0 /dev/rbd1",
        "text": "The md member devices to use with mdadm create command"
    },
    {
        "keyword": "level",
        "at": True,
        "provisioning": True,
        "example": "raid1",
        "text": "The md raid level to use with mdadm create command (see mdadm man for values)"
    },
    {
        "keyword": "layout",
        "at": True,
        "provisioning": True,
        "text": "The md raid layout to use with mdadm create command (see mdadm man for values)"
    },
    {
        "keyword": "chunk",
        "at": True,
        "provisioning": True,
        "example": "128k",
        "text": "The md chunk size to use with mdadm create command. Values are converted to kb and rounded to 4."
    },
    {
        "keyword": "spares",
        "at": True,
        "provisioning": True,
        "convert": "integer",
        "example": "0",
        "default": 0,
        "text": "The md number of spare devices to use with mdadm create command"
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mdadm"):
        return ["disk.md"]
    return []

def justcall_mdadm_detail(*args, **kwargs):
    return justcall(*args, **kwargs)


def justcall_mdadm_scan(*args, **kwargs):
    return justcall(*args, **kwargs)


def justcall_md_create(*args, **kwargs):
    return justcall(*args, **kwargs)


class DiskMd(BaseDisk):
    startup_timeout = 10

    def __init__(self,
                 name=None,
                 uuid=None,
                 level=None,
                 devs=None,
                 spares=None,
                 chunk=None,
                 layout=None,
                 **kwargs):
        self.uuid = uuid
        self.level = level
        self.devs = devs or []
        self.spares = spares
        self.chunk = chunk
        self.layout = layout
        self.mdadm = "/sbin/mdadm"
        super(DiskMd, self).__init__(name=uuid, type='disk.md', **kwargs)
        if uuid:
            self.label = "md " + uuid
        else:
            self.label = "md"

    @lazy
    def mdadm_cf(self):
        if os.path.exists("/etc/mdadm"):
            return "/etc/mdadm/mdadm.conf"
        else:
            return "/etc/mdadm.conf"

    @lazy
    def is_shared(self):
        if self.shared is not None:
            return self.shared
        if len(self.svc.nodes|self.svc.drpnodes) < 2:
            self.log.debug("shared param defaults to 'false' due to single "
                           "node configuration")
            return False
        l = [option for option in self.svc.cd[self.rid] if \
             option.startswith("uuid@")]
        if len(l) > 0:
            self.log.debug("shared param defaults to 'false' due to scoped "
                           "configuration")
            return False
        else:
            self.log.debug("shared param defaults to 'true' due to unscoped "
                           "configuration")
            return True

    def provisioned(self):
        if which("mdadm") is None:
            return
        return self.has_it()

    def provisioner(self):
        self._create_md()
        self.can_rollback = True
        self._set_real_uuid()
        self._set_svc_rid_uuid()
        self.svc.node.unset_lazy("devtree")

    def unprovisioner(self):
        if self.uuid == "" or self.uuid is None:
            return
        for dev in self.sub_devs():
            self.vcall([self.mdadm, "--brief", "--zero-superblock", dev])
        self._unset_svc_rid_uuid()
        self.svc.node.unset_lazy("devtree")
        self.clear_cache("mdadm.scan.v")

    def _info(self):
        data = [
          ["uuid", self.uuid],
        ]
        return data

    def md_config_file_name(self):
        return os.path.join(self.var_d, 'disks')

    def md_config_import(self):
        p = self.md_config_file_name()
        if not os.path.exists(p):
            return set()
        with open(p, "r") as ofile:
            return json.load(ofile)

    def md_config_export(self):
        devs = self.sub_devs()
        disk_ids = set()
        for dev in devs:
            treedev = self.svc.node.devtree.get_dev_by_devpath(dev)
            if not treedev:
                continue
            disk_ids.add(treedev.alias)
        with open(self.md_config_file_name(), "w") as ofile:
             json.dump(list(disk_ids), ofile)

    def postsync(self):
        self.auto_assemble_disable()

    def down_state_alerts(self):
        if not self.is_shared:
            return
        devnames = self.md_config_import()
        devnames = set([d for d in devnames if not d.startswith("md")])
        if len(devnames) == 0:
            return

        dt = self.svc.node.devtree
        aliases = set([d.alias for d in dt.dev.values()])
        not_found = devnames - aliases
        if len(not_found) > 0:
            self.status_log("md member missing: %s" % ", ".join(sorted(list(not_found))))

    def presync(self):
        if self.uuid is None:
            return
        if not self.is_shared:
            return
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if self.svc.options.force or s['avail'].status == core.status.UP:
            self.md_config_export()

    def files_to_sync(self):
        if self.uuid is None:
            return
        if not self.is_shared:
            return []
        return [self.md_config_file_name()]

    def md_devpath(self):
        devpath = self.devpath()
        if path_exists(devpath):
            return devpath
        out, err, ret = self.mdadm_scan_v()
        devname = self.devname()
        for line in out.splitlines():
            if self._mdadm_scan_match(line, uuid=self.uuid, devname=devname):
                devname = line.split()[1]
                if path_exists(devname):
                    return devname
        raise ex.Error("unable to find a devpath for md")

    def devname(self):
        if self.svc.namespace:
            return "/dev/md/"+self.svc.namespace.lower()+"."+self.svc.name.split(".")[0]+"."+self.rid.replace("#", ".")
        else:
            return "/dev/md/"+self.svc.name.split(".")[0]+"."+self.rid.replace("#", ".")

    def devpath(self):
        return "/dev/disk/by-id/md-uuid-"+str(self.uuid)

    def exposed_devs(self):
        if self.uuid == "" or self.uuid is None:
            return set()
        try:
            return set([os.path.realpath(self.md_devpath())])
        except:
            return set()

    def assemble(self):
        cmd = [self.mdadm, "--assemble", self.devname(), "-u", self.uuid]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        self.clear_cache("mdadm.scan.v")
        if ret == 2:
            self.log.info("no changes were made to the array")
        elif ret != 0:
            raise ex.Error
        else:
            self.wait_for_fn(self.has_it, self.startup_timeout, 1, errmsg="waited too long for devpath creation")

    def manage_stop(self):
        cmd = [self.mdadm, "--stop", self.md_devpath()]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        self.clear_cache("mdadm.scan.v")
        if ret != 0:
            raise ex.Error

    def detail(self, devname=None):
        try:
            devpath = devname or self.md_devpath()
        except ex.Error as e:
            return "State : " + str(e)
        if not path_exists(devpath):
            return "State : devpath does not exist"
        out, err, ret = justcall_mdadm_detail(argv=[self.mdadm, '--detail', devpath])
        if "cannot open /dev" in err:
            return "State : devpath does not exist"
        if ret != 0:
            if "does not appear to be active" in err:
                return "State : md does not appear to be active"
            raise ex.Error(err)
        return out

    def detail_status(self):
        buff = self.detail()
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("State :"):
                return line.split(" : ")[-1]
        return "unknown"

    def _mdadm_scan_match(self, output, uuid=None, devname=None):
        words = output.split()
        if uuid and "UUID=" + uuid in words:
            return True
        elif devname and devname in words:
            return True
        else:
            return False

    def has_it(self):
        if self.uuid == "" or self.uuid is None:
            return False
        return self._mdadm_scan_match(self.mdadm_scan_v()[0], uuid=self.uuid)

    def is_up(self):
        if not self.has_it():
            return False
        buff = self.detail_status()
        states = buff.split(", ")
        if len(states) > 1:
            self.status_log(buff)
        if len(states) == 0:
            return False
        false_states = [
            "Not Started",
            "devpath does not exist",
            "unable to find a devpath for md",
            "unknown"
        ]
        for state in false_states:
            if state in states:
                self.status_log(buff)
                return False
        return True

    def auto_assemble_disabled(self):
        if self.uuid == "" or self.uuid is None:
            return True
        if not os.path.exists(self.mdadm_cf):
            self.status_log("auto-assemble is not disabled")
            return False
        with open(self.mdadm_cf, "r") as ofile:
            for line in ofile.readlines():
                words = line.strip().split()
                if len(words) < 2:
                    continue
                if words[0] == "AUTO" and "-all" in words:
                    return True
                if words[0] == "ARRAY" and words[1] == "<ignore>" and \
                   "UUID="+self.uuid in words:
                    return True
        self.status_log("auto-assemble is not disabled")
        return False

    def auto_assemble_disable(self):
        if self.uuid == "" or self.uuid is None:
            return
        if self.auto_assemble_disabled():
            return
        self.log.info("disable auto-assemble in %s" % self.mdadm_cf)
        with open(self.mdadm_cf, "a+") as ofile:
            ofile.write("ARRAY <ignore> UUID=%s\n" % self.uuid)

    def _status(self, verbose=False):
        invalid_devname_message = self._invalid_devname()
        if invalid_devname_message:
            self.status_log(invalid_devname_message)
        if self.uuid is None:
            return core.status.NA
        self.auto_assemble_disabled()
        s = super(DiskMd, self)._status(verbose=verbose)
        if s == core.status.DOWN:
             self.down_state_alerts()
        return s

    def do_start(self):
        if self.uuid is None:
            raise ex.Error("uuid is not set")
        self.auto_assemble_disable()
        if self.is_up():
            self.log.info("md %s is already assembled" % self.uuid)
            return 0
        self.can_rollback = True
        self.assemble()
        self._create_static_name()

    def do_stop(self):
        if self.uuid is None:
            self.log.warning("uuid is not set: skip")
            return
        self.auto_assemble_disable()
        if not self.is_up():
            self.log.info("md %s is already down" % self.uuid)
            return
        self.manage_stop()

    def _create_static_name(self):
        self.create_static_name(self.md_devpath())

    @fcache
    def sub_devs(self):
        if self.uuid == "" or self.uuid is None:
            # try to get the info from the config so pr co-resource can reserv
            # during provision
            try:
                self.devs = self.oget("devs")
                devs = self.devs
                if devs is None:
                    return set()
                return set([os.path.realpath(dev) for dev in devs])
            except ex.OptNotFound:
                return set()
        try:
            devpath = self.md_devpath()
        except ex.Error as e:
            return self.sub_devs_inactive()
        if path_exists(devpath):
            return self.sub_devs_active()
        else:
            return self.sub_devs_inactive()

    @cache("mdadm.scan.v")
    def mdadm_scan_v(self):
        return justcall_mdadm_scan(argv=[self.mdadm, "-E", "--scan", "-v"])

    def sub_devs_inactive(self):
        devs = set()
        out, err, ret = self.mdadm_scan_v()
        if ret != 0:
            return devs
        lines = out.split("\n")

        if len(lines) < 2:
            return set()
        inblock = False
        paths = set()
        for line in lines:
            if self._mdadm_scan_match(line, uuid=self.uuid):
                inblock = True
                continue
            if inblock and "devices=" in line:
                l = line.split("devices=")[-1].split(",")
                l = map(lambda x: os.path.realpath(x), l)
                for dev in l:
                    _paths = set(utilities.devices.linux.dev_to_paths(dev))
                    if set([dev]) != _paths:
                        paths |= _paths
                    devs.add(dev)
                break
        # discard paths from the list (mdadm shows both mpaths and paths)
        devs -= paths

        self.log.debug("found devs %s held by md %s" % (devs, self.uuid))
        return devs

    def sub_devs_active(self):
        devs = set()

        try:
            lines = self.detail().split("\n")
        except ex.Error as e:
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

    def sync_resync(self):
        faultydev = None
        buff = self.detail()
        added = 0
        removed = buff.count("removed")
        if removed == 0:
            self.log.info("skip: no removed device")
            return
        if "Raid Level : raid1" not in buff:
            self.log.info("skip: non-raid1 md")
            return
        if not self.is_up():
            self.log.info("skip: non-up md")
            return
        devpath = self.devpath()
        for line in buff.split("\n"):
            line = line.strip()
            if "faulty" in line:
                faultydev = line.split()[-1]
                cmd = [self.mdadm, "--re-add", devpath, faultydev]
                ret, out, err = self.vcall(cmd, warn_to_info=True)
                self.clear_cache("mdadm.scan.v")
                if ret != 0:
                    raise ex.Error("failed to re-add %s to %s"%(faultydev, devpath))
                added += 1
        if removed > added:
            self.log.error("no faulty device found to re-add to %s remaining "
                           "%d removed legs"% (devpath, removed - added))

    def _set_svc_rid_uuid(self):
        if self.shared:
            self.log.info("set %s.uuid = %s", self.rid, self.uuid)
            self.svc._set(self.rid, "uuid", self.uuid)
        else:
            self.log.info("set %s.uuid@%s = %s", self.rid, Env.nodename, self.uuid)
            self.svc._set(self.rid, "uuid@" + Env.nodename, self.uuid)

    def _unset_svc_rid_uuid(self):
        if self.shared:
            self.log.info("reset %s.uuid", self.rid)
            self.svc._set(self.rid, "uuid", "")
        else:
            self.log.info("reset %s.uuid@%s", self.rid, Env.nodename)
            self.svc._set(self.rid, "uuid@" + Env.nodename, "")

    def _create_md(self):
        if which("mdadm") is None:
            raise ex.Error("mdadm is not installed")
        argv = self._md_create_argv()
        self.log.info(" ".join(argv))
        out, err, return_code = justcall_md_create(argv=argv, input=b'no\n')
        self.clear_cache("mdadm.scan.v")
        self.log.info(out)
        if return_code != 0:
            raise ex.Error(err)
        if len(out) > 0:
            self.log.info(out)
        if len(err) > 0:
            self.log.error(err)

    def _md_create_argv(self):
        level = self.level
        devs = self.devs or self.oget("devs")
        spares = self.spares
        chunk = self.chunk
        layout = self.layout
        number_devs = len(devs) - (spares or 0)
        if number_devs < 1:
            raise ex.Error("at least 1 device must be set in the 'devs' provisioning")
        invalid_devname_message = self._invalid_devname()
        if invalid_devname_message:
            raise ex.Error(invalid_devname_message)
        name = self.devname()
        argv = [self.mdadm, '--create', name, '--force', '--quiet',
               '--metadata=default']
        argv += ['-n', str(number_devs)]
        if level:
            argv += ["-l", level]
        if spares:
            argv += ["-x", str(spares)]
        if chunk:
            argv += ["-c", str(convert_size(chunk, _to="k", _round=4))]
        if layout:
            argv += ["-p", layout]
        argv += devs
        return argv

    def _set_real_uuid(self):
        buff = self.detail(devname=self.devname())
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("UUID :"):
                self.uuid = line.split(" : ")[-1]
                return
        raise ex.Error("unable to determine md uuid")

    def _invalid_devname(self):
        md_name = os.path.basename(self.devname())
        if len(md_name) >= 32:
            return "device md name is too long, 32 chars max (name is %s)" % md_name
