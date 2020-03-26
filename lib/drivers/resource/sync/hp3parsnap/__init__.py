import datetime
import os

import rcExceptions as ex
import drivers.array.hp3par as array_driver
import rcStatus

from .. import Sync, notify
from rcGlobalEnv import rcEnv
from svcBuilder import sync_kwargs
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "hp3parsnap"
KEYWORDS = [
    {
        "keyword": "array",
        "required": True,
        "at": True,
        "text": "Name of the HP 3par array to send commands to."
    },
    {
        "keyword": "vv_names",
        "convert": "list",
        "required": True,
        "at": True,
        "text": "The names of snapshot VV or sets of VV to update."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = {}

    kwargs["array"] = svc.oget(s, "array")
    vv_names = svc.oget(s, "vv_names")

    if len(vv_names) == 0:
        svc.log.error("config file section %s must have at least one vv_name set" % s)
        return

    kwargs["vv_names"] = vv_names

    kwargs.update(sync_kwargs(svc, s))
    r = SyncHp3parsnap(**kwargs)
    svc += r


class SyncHp3parsnap(Sync):
    def __init__(self, array=None, vv_names=[], **kwargs):
        super(SyncHp3parsnap, self).__init__(type="sync.hp3parsnap", **kwargs)
        self.array = array
        self.vv_names = vv_names
        self.label = "hp3parsnap %s" % ", ".join(self.vv_names)
        if len(self.label) > 50:
            self.label = self.label[:47] + "..."
        self.default_schedule = "@0"

    def __str__(self):
        return self.label

    def on_add(self):
        try:
            arrays = array_driver.Hp3pars(objects=[self.array], log=self.log, node=self.svc.node)
        except Exception as e:
            raise ex.excInitError(str(e))
        if len(arrays.arrays) == 1:
            self.array_obj = arrays.arrays[0]
        else:
            self.array_obj = None

        if self.array_obj is None:
            self.log.error("no 3par array object")
            return
        self.array_obj.path = self.svc.path

    def can_sync(self, target=None, s=None):
        data = self.showvv()
        if len(data) < len(self.vv_names):
            return False
        last = self.lastsync_s_to_datetime(data[0]['CreationTime'])
        if self.skip_sync(datetime.datetime.utcnow()-last):
            return False
        try:
            self.check_requires("sync_update")
        except ex.excError:
            return False
        return True

    def updatevv(self):
        self.array_obj.updatevv(vvnames=self.vv_names, log=self.log)

    @notify
    def sync_update(self):
        self.updatevv()
        self.array_obj.clear_caches()

    def lastsync_s_to_datetime(self, s):
        out, err, ret = justcall(["date", "--utc", "--date=%s" % s, '+%Y-%m-%d %H:%M:%S'])
        d = datetime.datetime.strptime(out.strip(), "%Y-%m-%d %H:%M:%S")
        return d

    def showvv(self):
        return self.array_obj.showvv(vvprov="snp", vvnames=self.vv_names, cols=["Name", "CreationTime"])

    def _status(self, verbose=False):
        if self.array_obj is None:
            self.status_log("array %s is not accessible" % self.array)
            return rcStatus.WARN
        if not self.array_obj.has_virtualcopy():
            self.status_log("array %s has no virtual copy license" % self.array)
            return rcStatus.WARN

        try:
            data = self.showvv()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

        r = None
        if len(data) < len(self.vv_names):
            missing = set(self.vv_names) - set([d["Name"] for d in data])
            for m in missing:
                self.status_log("missing vv: %s" % m)
            r = rcStatus.WARN

        elapsed = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.sync_max_delay)
        for vv in data:
            if self.lastsync_s_to_datetime(vv['CreationTime']) < elapsed:
                self.status_log("vv %s last sync too old (%s)"%(vv['Name'], vv['CreationTime']))
                r = rcStatus.WARN

        if r is not None:
            return r

        return rcStatus.UP

