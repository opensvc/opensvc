import os

from rcGlobalEnv import rcEnv
import rcExceptions as ex
import rcStatus
import time
import datetime
import resSync
import rcZfs
from rcUtilities import justcall

class syncZfsSnap(resSync.Sync):
    def __init__(self,
                 rid=None,
                 name=None,
                 dataset=[],
                 keep=1,
                 recursive=True,
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 internal=False):
        resSync.Sync.__init__(self,
                              rid=rid, type="sync.zfssnap",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        if name:
            self.label = "zfs '%s' snapshot %s" % (name, ", ".join(dataset))
        else:
            self.label = "zfs snapshot %s" % ", ".join(dataset)
        self.dataset = dataset
        self.recursive = recursive
        self.keep = keep
        self.name = name
        self.zfs = {}

    def info(self):
        data = [
          ["dataset", " ".join(self.dataset)],
          ["name", self.name if self.name else ""],
          ["keep", str(self.keep)],
          ["recursive", str(self.recursive).lower()],
          ["sync_max_delay", str(self.sync_max_delay) if self.sync_max_delay else ""],
          ["schedule", self.schedule if self.schedule else ""],
        ]
        return self.fmt_info(data)

    def on_add(self):
        pass

    def create_snap(self, dataset):
        ds = rcZfs.Dataset(dataset, log=self.log)
        snap = ""
        if self.name:
            suffix = self.name
        else:
            suffix = ""
        suffix += ".snap.%Y-%m-%d.%H:%M:%S"
        snap += datetime.datetime.now().strftime(suffix)
        try:
            ds.snapshot(snapname=snap, recursive=self.recursive)
        except Exception as e:
            raise ex.excError(str(e))

    def list_snaps(self, dataset):
        cmd = ["zfs", "list", "-H", "-t", "snapshot", "-o", "name"]
        out, err, ret = justcall(cmd)
        snaps = []
        for line in out.splitlines():
            if line.startswith(dataset+"@"):
                snaps.append(line)
        return snaps

    def remove_snap(self, dataset):
        cursnaps = self.list_snaps(dataset)
        snaps = {}
        for sv in cursnaps:
            s = sv.replace(dataset+"@", "")
            l = s.split('.')
            if len(l) < 2:
                continue
            if l[0] != self.name or l[1] != "snap":
                continue
            try:
                ds = sv.split(".snap.")[-1]
                d = datetime.datetime.strptime(ds, "%Y-%m-%d.%H:%M:%S")
                snaps[ds] = sv
            except Exception as e:
                pass
        if len(snaps) <= self.keep:
            return
        sorted_snaps = []
        for ds in sorted(snaps.keys(), reverse=True):
            sorted_snaps.append(snaps[ds])
        for path in sorted_snaps[self.keep:]:
            try:
                ds = rcZfs.Dataset(path, log=self.log)
                if self.recursive:
                    options = ["-r"]
                else:
                    options = []
                ds.destroy(options=options)
            except Exception as e:
                raise ex.excError(str(e))

    def _status_one(self, dataset):
        try:
            ds = rcZfs.Dataset(dataset, log=self.log)
        except Exception as e:
            self.status_log("%s %s" % (dataset, str(e)))
            return
        snaps = []
        for sv in self.list_snaps(dataset):
            s = sv.replace(dataset+"@", "")
            l = s.split('.')
            if len(l) < 2:
                continue
            if l[0] != self.name or l[1] != "snap":
                continue
            try:
                ds = sv.split(".snap.")[-1]
                d = datetime.datetime.strptime(ds, "%Y-%m-%d.%H:%M:%S")
                snaps.append(d)
            except Exception as e:
                pass
        if len(snaps) == 0:
            self.status_log("%s has no snap" % dataset)
            return
        if len(snaps) > self.keep:
            self.status_log("%s has %d too many snaps" % (dataset, len(snaps)-self.keep))
        last = sorted(snaps, reverse=True)[0]
        limit = datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay)
        if last < limit:
            self.status_log("%s last snap is too old (%s)" % (dataset, last.strftime("%Y-%m-%d %H:%M:%S")))

    def _status(self, verbose=False):
        for dataset in self.dataset:
            if dataset.count("/") < 1:
                self.status_log("misformatted dataset entry %s (expected <pool>/<ds>)" % dataset)
                continue
            self._status_one(dataset)
        issues = set(self.status_logs_get(["warn"])) - set([''])
        if len(issues) == 0:
            return rcStatus.UP
        return rcStatus.WARN

    def _sync_update(self, dataset):
        self.create_snap(dataset)
        self.remove_snap(dataset)

    def sync_update(self):
        for dataset in self.dataset:
            self._sync_update(dataset)

    def __str__(self):
        return "%s dataset=%s keep=%s" % (resSync.Sync.__str__(self), str(self.dataset), str(self.keep))

