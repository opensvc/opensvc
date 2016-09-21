import os

from rcGlobalEnv import rcEnv
import rcExceptions as ex
import rcStatus
from rcUtilities import justcall
import resSync
import datetime
import rcHp3par as rc

class syncHp3parSnap(resSync.Sync):
    def __init__(self,
                 rid=None,
                 array=None,
                 vv_names=[],
                 depends=[],
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.hp3parsnap",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)
        self.array = array
        self.vv_names = vv_names
        self.depends = depends
        self.label = "hp3parsnap %s" % ", ".join(self.vv_names)
        if len(self.label) > 50:
            self.label = self.label[:47] + "..."
        try:
            arrays = rc.Hp3pars(objects=[self.array])
        except Exception as e:
            raise ex.excInitError(str(e))
        if len(arrays.arrays) == 1:
            self.array_obj = arrays.arrays[0]
        else:
            self.array_obj = None

    def __str__(self):
        return self.label

    def on_add(self):
        if self.array_obj is None:
            self.log.error("no 3par array object")
            return
        self.array_obj.svcname = self.svc.svcname

    def can_sync(self, target=None, s=None):
        data = self.array_obj.showvv(vvprov="snp", vvnames=self.vv_names, cols=["Name", "CreationTime"])
        last = self.lastsync_s_to_datetime(data[0]['CreationTime'])
        if self.skip_sync(datetime.datetime.utcnow()-last):
            return False
        try:
            self.check_depends()
        except ex.excError:
            return False
        return True

    def check_depends(self):
        if len(self.depends) == 0:
            return
        for rid in self.depends:
            self._check_depends(rid)

    def _check_depends(self, rid):
        if rid not in self.svc.resources_by_id:
            self.log.warning("ignore depends on %s: resource not found" % rid)
            return
        r = self.svc.resources_by_id[rid]
        rs = r.status()
        if r.status() != rcStatus.UP:
            raise ex.excError("depends on resource %s, in state %s" % (rid, rcStatus.status_str(rs)))

    def updatevv(self):
        try:
            self.check_depends()
        except ex.excError as e:
            self.log.error(e)
            raise ex.excError()
        self.array_obj.updatevv(vvnames=self.vv_names, log=self.log)

    def sync_update(self):
        self.updatevv()

    def lastsync_s_to_datetime(self, s):
        out, err, ret = justcall(["date", "--utc", "--date=%s" % s, '+%Y-%m-%d %H:%M:%S'])
        d = datetime.datetime.strptime(out.strip(), "%Y-%m-%d %H:%M:%S")
        return d

    def _status(self, verbose=False):
        if self.array_obj is None:
            self.status_log("array %s is not accessible" % self.array)
            return rcStatus.WARN
        if not self.array_obj.has_virtualcopy():
            self.status_log("array %s has no virtual copy license" % self.array)
            return rcStatus.WARN

        try:
            data = self.array_obj.showvv(vvprov="snp", vvnames=self.vv_names, cols=["Name", "CreationTime"])
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

        elapsed = datetime.datetime.utcnow() - datetime.timedelta(minutes=self.sync_max_delay)
        r = None
        for vv in data:
            if self.lastsync_s_to_datetime(vv['CreationTime']) < elapsed:
                self.status_log("vv %s last sync too old (%s)"%(vv['Name'], vv['CreationTime']))
                r = rcStatus.WARN

        if r is not None:
            return r

        return rcStatus.UP

if __name__ == "__main__":
    o = syncHp3parSnap(rid="sync#1", array="baie-pra", rcg="RCG.SVCTEST1")
    print(o)
