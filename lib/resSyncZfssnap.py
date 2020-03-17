import datetime
import os
import time

import rcExceptions as ex
import rcStatus
import rcZfs
import resSync

from rcGlobalEnv import rcEnv
from rcUtilities import justcall, cache, clear_cache
from svcBuilder import sync_kwargs


def adder(svc, s):
    kwargs = {}
    kwargs["name"] = svc.oget(s, "name")
    kwargs["keep"] = svc.oget(s, "keep")
    kwargs["recursive"] = svc.oget(s, "recursive")
    kwargs["dataset"] = svc.oget(s, "dataset")
    kwargs.update(sync_kwargs(svc, s))
    r = SyncZfssnap(**kwargs)
    svc += r


class SyncZfssnap(resSync.Sync):
    def __init__(self,
                 rid=None,
                 name=None,
                 dataset=[],
                 keep=1,
                 recursive=True,
                 **kwargs):
        resSync.Sync.__init__(self,
                              rid=rid, type="sync.zfssnap",
                              **kwargs)

        try:
            self.dataset = [ds.strip("/") for ds in dataset]
        except Exception:
            self.dataset = dataset
        if name:
            self.label = "zfs '%s' snapshot %s" % (name, ", ".join(self.dataset))
        else:
            self.label = "zfs snapshot %s" % ", ".join(self.dataset)
        self.recursive = recursive
        self.keep = keep
        self.name = name
        self.zfs = {}

    def _info(self):
        data = [
          ["dataset", " ".join(self.dataset)],
          ["name", self.name if self.name else ""],
          ["keep", str(self.keep)],
          ["recursive", str(self.recursive).lower()],
          ["sync_max_delay", str(self.sync_max_delay) if self.sync_max_delay else ""],
          ["schedule", self.schedule if self.schedule else ""],
        ]
        return data

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
            clear_cache("zfs.list.snapshots.name")
        except Exception as e:
            raise ex.excError(str(e))

    @cache("zfs.list.snapshots.name")
    def list_snaps(self):
        cmd = ["zfs", "list", "-r", "-H", "-t", "snapshot", "-o", "name"]
        out, err, ret = justcall(cmd)
        return out.splitlines()

    def remove_snap(self, dataset):
        cursnaps = self.list_snaps()
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
                clear_cache("zfs.list.snapshots.name")
            except Exception as e:
                raise ex.excError(str(e))

    def get_snaps(self, dataset):
        snaps = []
        for sv in self.list_snaps():
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
        return snaps

    def last_snap_date(self, snaps):
        try:
            return sorted(snaps, reverse=True)[0]
        except IndexError:
            return

    def _status_one(self, dataset):
        if not self.has_pool(dataset):
            return
        try:
            ds = rcZfs.Dataset(dataset, log=self.log)
        except Exception as e:
            self.status_log("%s %s" % (dataset, str(e)))
            return
        snaps = self.get_snaps(dataset)
        if len(snaps) == 0:
            self.status_log("%s has no snap" % dataset)
            return
        if len(snaps) > self.keep + 1:
            self.status_log("%s has %d too many snaps" % (dataset, len(snaps)-self.keep))
        last = self.last_snap_date(snaps)
        limit = datetime.datetime.now() - datetime.timedelta(seconds=self.sync_max_delay)
        if last < limit:
            self.status_log("%s last snap is too old (%s)" % (dataset, last.strftime("%Y-%m-%d %H:%M:%S")))

    def has_pool(self, dataset):
        cmd = ["zfs", "list", "-H", "-o", "name", dataset.split("/")[0]]
        _, _, ret = justcall(cmd)
        return ret == 0

    def sync_status(self, verbose=False):
        self.remove_snaps()
        for dataset in self.dataset:
            self._status_one(dataset)
        issues = set(self.status_logs_get(["warn"])) - set([''])
        if len(issues) == 0:
            return rcStatus.UP
        return rcStatus.WARN

    def can_update(self, dataset):
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if not self.svc.options.force and \
           s['avail'].status not in [rcStatus.UP, rcStatus.NA]:
            if not self.svc.options.cron:
                self.log.info("skip snapshot creation on instance not up")
            return False
        rids = self.svc.options.rid
        if rids is None:
            rids = []
        if self.svc.options.cron or self.rid not in rids:
            snaps = self.get_snaps(dataset)
            last = self.last_snap_date(snaps)
            if self.svc.sched.skip_action_schedule(self.rid, "sync_schedule", last=last):
                self.log.info("skip: not in allowed schedule")
                return False
        return True

    def _sync_update(self, dataset):
        if self.can_update(dataset):
            self.create_snap(dataset)
        self.remove_snap(dataset)

    @resSync.notify
    def sync_update(self):
        pass

    def pre_action(self, action):
        """
        Do the snaps in pre_action so the zfs send/recv can replicate them asap
        """
        if not hasattr(self, action):
            return

        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        for resource in sorted(resources):
            for dataset in resource.dataset:
                resource._sync_update(dataset)

    def remove_snaps(self):
        for dataset in self.dataset:
            self.remove_snap(dataset)

    def __str__(self):
        return "%s dataset=%s keep=%s" % (resSync.Sync.__str__(self), str(self.dataset), str(self.keep))
