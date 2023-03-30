import datetime
import os
import subprocess

import core.status
import utilities.subsystems.btrfs
import core.exceptions as ex
from .. import Sync, notify
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import justcall
from utilities.files import makedirs

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "btrfssnap"
KEYWORDS = [
    {
        "keyword": "name",
        "at": True,
        "example": "weekly",
        "text": "A name included in the snapshot name to avoid retention conflicts between multiple btrfs snapshot resources. A full snapshot name is formatted as ``<subvol>.<name>.snap.<datetime>``. Example: data.weekly.snap.2016-03-09.10:09:52"
    },
    {
        "keyword": "subvol",
        "convert": "list",
        "at": True,
        "required": True,
        "example": "svc1fs:data svc1fs:log",
        "text": "A whitespace separated list of ``<label>:<subvol>`` to snapshot."
    },
    {
        "keyword": "keep",
        "at": True,
        "default": 3,
        "convert": "integer",
        "example": "3",
        "text": "The maximum number of snapshots to retain."
    },
    {
        "keyword": "recursive",
        "at": True,
        "default": False,
        "convert": "boolean",
        "candidates": [True, False],
        "text": "Also replicate subvolumes in the src tree."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

TIMEFMT = "%Y-%m-%dT%H:%M:%S.%fZ"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("btrfs"):
        return ["sync.btrfssnap"]
    return []


class SyncBtrfssnap(Sync):
    def __init__(self,
                 name=None,
                 subvol=None,
                 keep=1,
                 recursive=False,
                 **kwargs):
        super(SyncBtrfssnap, self).__init__(type="sync.btrfssnap", **kwargs)

        if subvol is None:
            subvol = []
        if name:
            self.label = "btrfs '%s' snapshot %s" % (name, ", ".join(subvol))
        else:
            self.label = "btrfs snapshot %s" % ", ".join(subvol)
        self.subvol = subvol
        self.keep = keep
        self.name = name
        self.recursive = recursive
        self.btrfs = {}

    def on_add(self):
        pass

    def __str__(self):
        return "%s subvol=%s keep=%s" % (
            super(SyncBtrfssnap, self).__str__(),
            self.subvol,
            self.keep
        )

    def test_btrfs(self, label):
        cmd = [Env.syspaths.blkid, "-L", label]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return ret
        dev = out.strip()
        dev = os.path.realpath(dev)
        import glob
        holders = glob.glob("/sys/block/%s/holders/*" % os.path.basename(dev))
        if len(holders) == 1:
            hdev = "/dev/%s" % os.path.basename(holders[0])
        else:
            hdev = None
        if hdev and os.path.exists(hdev):
            dev = hdev
        cmd = ["btrfs", "fi", "show", dev]
        out, err, ret = justcall(cmd)
        return ret

    def stop(self):
        for s in self.subvol:
            try:
                label, subvol = s.split(":")
            except:
                self.log.error("misformatted subvol entry %s (expected <label>:<subvol>)" % s)
                continue
            btrfs = self.get_btrfs(label)
            cmd = ["umount", btrfs.rootdir]
            self.vcall(cmd)

    def get_btrfs(self, label):
        if label in self.btrfs:
            return self.btrfs[label]
        try:
            self.btrfs[label] = utilities.subsystems.btrfs.Btrfs(label=label, resource=self)
        except utilities.subsystems.btrfs.ExecError as e:
            raise ex.Error(str(e))
        return self.btrfs[label]

    def src(self, label, path):
        return os.path.join(self.btrfs[label].rootdir, path)

    def subvols(self, label, path):
        """
        sort by path so the subvols are sorted by path depth
        """
        btr = self.get_btrfs(label)
        src = self.src(label, path)
        if not src:
            return []
        if not self.recursive:
            sub = btr.get_subvol(src)
            if not sub:
                return []
            return [sub]
        subvols = []
        for subvol in btr.get_subvols().values():
            if subvol["path"] == path:
                subvols.append(subvol)
            elif subvol["path"].startswith(path + "/"):
                subvols.append(subvol)
        subvols = sorted(subvols, key=lambda x: x["path"])
        return subvols

    def create_snaps(self, label, subvol):
        cmds = []
        for sv in self.subvols(label, subvol):
            if "/.snap/" in sv["path"]:
                continue
            cmds += self.create_snap(label, sv["path"])
        return cmds

    def create_snap(self, label, subvol):
        btrfs = self.get_btrfs(label)
        orig = os.path.join(btrfs.rootdir, subvol)
        snap = os.path.join(btrfs.rootdir, subvol)
        snap += "/.snap/"
        snap += datetime.datetime.utcnow().isoformat("T")+"Z"
        if self.name:
            snap += "," + self.name
        try:
            makedirs(os.path.dirname(snap))
        except OSError as e:
            self.log.debug("skip %s snap: readonly", subvol)
            return []
        cmd = btrfs.snapshot_cmd(orig, snap, readonly=True)
        if not cmd:
            return []
        return [subprocess.list2cmdline(cmd)]

    def remove_snaps(self, label, subvol):
        cmds = []
        for sv in self.subvols(label, subvol):
            if "/.snap/" in sv["path"]:
                continue
            cmds += self._remove_snaps(label, sv["path"])
        return cmds

    def _remove_snaps(self, label, subvol):
        btrfs = self.get_btrfs(label)
        snaps = {
            datetime.datetime.utcnow().strftime(TIMEFMT): {"path": ""},
        }
        # does not contain the one we created due to cache
        for sv in btrfs.get_subvols().values():
            if not sv["path"].startswith(subvol+"/.snap/"):
                continue
            if not self.match_snap_name(sv["path"]):
                continue
            ds = sv["path"].replace(subvol+"/.snap/", "")
            ds = ds.split(",")[0] # discard optional name
            try:
                d = datetime.datetime.strptime(ds, TIMEFMT)
                snaps[ds] = sv["path"]
            except Exception as e:
                pass
        if len(snaps) <= self.keep:
            return []
        sorted_snaps = []
        for ds in sorted(snaps.keys(), reverse=True):
            sorted_snaps.append(snaps[ds])
        cmds = []
        for path in sorted_snaps[self.keep:]:
            cmd = btrfs.subvol_delete_cmd(os.path.join(btrfs.rootdir, path))
            if cmd:
                cmds.append(subprocess.list2cmdline(cmd))
        return cmds

    def match_snap_name(self, path):
        if self.name:
            if not path.endswith(","+self.name):
                return False
        else:
            if not path.endswith("Z"):
                return False
        return True

    def _status_one(self, label, subvol):
        if self.test_btrfs(label) != 0:
            self.status_log("snap of %s suspended: not writable"%label, "info")
            return
        try:
            btrfs = self.get_btrfs(label)
        except Exception as e:
            self.status_log("%s:%s %s" % (label, subvol, str(e)))
            return
        snaps = []
        for sv in btrfs.get_subvols().values():
            if not sv["path"].startswith(subvol+"/.snap/"):
                continue
            if not self.match_snap_name(sv["path"]):
                continue
            ds = sv["path"].replace(subvol+"/.snap/", "")
            ds = ds.split(",")[0] # discard optional name
            try:
                d = datetime.datetime.strptime(ds, TIMEFMT)
                snaps.append(d)
            except Exception as e:
                pass
        if len(snaps) == 0:
            self.status_log("%s:%s has no snap" % (label, subvol))
            return
        if len(snaps) > self.keep:
            self.status_log("%s:%s has %d/%d snaps" % (label, subvol, len(snaps), self.keep))
        last = sorted(snaps, reverse=True)[0]
        limit = datetime.datetime.now() - datetime.timedelta(seconds=self.sync_max_delay)
        if last < limit:
            self.status_log("%s:%s last snap is too old (%s)" % (label, subvol, last.strftime(TIMEFMT)))

    def _status(self, verbose=False):
        not_found = []
        for s in self.subvol:
            try:
                label, subvol = s.split(":")
            except:
                self.status_log("misformatted subvol entry %s (expected <label>:<subvol>)" % s)
                continue
            try:
                subvols = self.subvols(label, subvol)
            except Exception as e:
                if "mount" in str(e):
                    self.status_log("%s not found" % subvol, "info")
                    not_found.append(subvol)
                    continue
                else:
                    self.status_log(str(e), "error")
                    continue
            for sv in subvols:
                if "/.snap/" in sv["path"]:
                    continue
                self._status_one(label, sv["path"])
        messages = set(self.status_logs_get(["warn"])) - set([''])
        not_writable = set([r for r in messages if "not writable" in r or "not found" in r])
        issues = messages - not_writable

        if len(not_writable) > 0 and len(not_writable) == len(messages):
            return core.status.NA
        if len(not_found) == len(self.subvol):
            return core.status.NA
        if len(not_found) > 0:
            return core.status.WARN
        if len(issues) == 0:
            return core.status.UP
        return core.status.WARN

    def _sync_update(self, s):
        try:
            label, subvol = s.split(":")
        except:
            self.log.error("misformatted subvol entry %s (expected <label>:<subvol>)" % s)
            return
        if self.test_btrfs(label) != 0:
            self.log.info("skip snap of %s while the btrfs is no writable"%label)
            return
        cmds = []
        cmds += self.create_snaps(label, subvol)
        cmds += self.remove_snaps(label, subvol)
        if not cmds:
            return
        self.do_cmds(label, cmds)

    def do_cmds(self, label, cmds):
        o = self.get_btrfs(label)
        ret, out, err = o.vcall(" && ".join(cmds), shell=True)
        if ret != 0:
            raise ex.Error

    @notify
    def sync_update(self):
        for subvol in self.subvol:
            self._sync_update(subvol)

