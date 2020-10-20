import datetime
import time

import core.exceptions as ex
import core.status
import drivers.array.necism as array_driver
from .. import Sync, notify
from core.objects.svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "necismsnap"
KEYWORDS = [
    {
        "keyword": "array",
        "required": True,
        "text": "Name of the NEC ISM array to send commands to."
    },
    {
        "keyword": "devs",
        "required": True,
        "text": "A whitespace-separated list of ``SV:LD``."
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
    data = []
    if which("iSMsc_query"):
        data.append("sync.necismsnap")
    return data


class SyncNecismsnap(Sync):
    def __init__(self,
                 array_name=None,
                 devs=None,
                 **kwargs):
        super(SyncNecismsnap, self).__init__(type="sync.necismsnap", **kwargs)

        self.devs = devs or []
        self.array = array_driver.NecIsm(array_name)
        self.parse_devs(devs)
        self.default_schedule = "@0"
        self.label = "NecIsm snapshot %s" % array_name

    def on_add(self):
        self.array.log = self.log

    def parse_devs(self, devs):
        self.svld = []
        self.sv = {}
        self.ld = {}
        devs = devs.replace(' ', ',')
        for e in devs.split(','):
            try:
                sv, ld = e.split(':')
            except:
                raise ex.InitError("%s: malformed devs" % self.rid)
            if sv in self.sv:
                raise ex.InitError("%s: duplicate sv %s in devs" % (self.rid, sv))
            if ld in self.ld:
                raise ex.InitError("%s: duplicate ld %s in devs" % (self.rid, ld))
            self.sv[sv] = sv
            self.ld[ld] = ld
            self.svld.append((sv, ld))

    def wait_for_devs_ready(self):
        pass

    def get_sv_ts(self, sv):
        try:
            data = self.array.sc_query_ld(sv)
        except ex.Error:
            self.status_log("unable to query SV:%s" % sv)
            return
        if len(data["sv"]) == 0:
            self.status_log("SV:%s not found" % sv)
            return
        svinfo = data['sv'][0]
        try:
            begin = svinfo.index('[')+1
        except:
            self.status_log("unable to get timestamp for SV:%s" % sv)
            return
        end = svinfo.index(']')
        ts_s = svinfo[begin:end]
        ts = datetime.datetime.strptime(ts_s, "%Y/%m/%d %H:%M:%S")
        return ts

    def can_sync(self, target=None):
        return True

    def get_oldest_ts(self):
        ts = None
        for sv, ld in self.svld:
            _ts = self.get_sv_ts(sv)
            if _ts is None:
                return
            if ts is None or _ts < ts:
                ts = _ts
        return ts

    def _status(self, verbose=False, skip_prereq=False):
        ret = 0
        ret += self._status_ts()
        ret += self._status_link()
        if ret > 0:
            return core.status.WARN
        return core.status.UP

    def _status_ts(self):
        ts = self.get_oldest_ts()
        if ts is None:
            return 1
        now = datetime.datetime.now()
        limit = now - datetime.timedelta(minutes=self.sync_max_delay)
        if ts < limit:
            self.status_log("snap too old (%s)" % ts.strftime("%Y-%m-%d %H:%M"))
            return core.status.WARN
        self.status_log("snap at %s" % ts.strftime("%Y-%m-%d %H:%M"))
        return 0

    def _status_link(self):
        r = 0
        for sv, ld in self.svld:
            r += self.__status_link(sv, ld)
        return r

    def __status_link(self, sv, ld):
        try:
            li = self.array.sc_linkinfo_ld(sv)
        except ex.Error:
            self.status_log("unable to get SV:%s linkinfo" % sv)
            return 1
        l = [ dst for dst in li['dst'] if ld in dst.split() and 'link' in dst.split()]
        if len(l) != 1:
            self.status_log("LD:%s is not linked to SV:%s" % (ld, sv))
            return 1
        return 0

    def sync_resync(self):
        self.unlink()
        self.create()
        self.link()

    @notify
    def sync_update(self):
        self.unlink()
        self.create()
        self.link()

    def unlink(self):
        for sv, ld in self.svld:
            if self.__status_link(sv, ld) == 1:
                self.log.info("SV:%s is already unlinked from LD:%s" % (sv, ld))
            else:
                self.array.sc_unlink_ld(ld)

    def get_changing_snap(self, src):
        bv_detail = self.array.sc_query_bv_detail(src)
        l = []
        for sv, data in bv_detail['sv'].items():
            if data['Snap State'].endswith('ing'):
                l.append(':'.join((sv, data['Snap State'])))
        return l

    def wait_for_changing_snap(self, src):
        retry = 40
        while retry > 0:
            changing_snap = self.get_changing_snap(src)
            if len(changing_snap) == 0:
                return
            self.log.info("SV are changing states : %s. Wait 30sec before retry" % ', '.join(changing_snap))
            retry -= 1
            try:
                time.sleep(30)
            except:
                return

    def create(self):
        for sv, ld in self.svld:
            try:
                src = self.array.sc_query_ld(sv)['LD Name']
            except:
                raise ex.Error("can not determine source LD for SV:%s" % sv)
            self.wait_for_changing_snap(src)
            self.array.sc_create_ld(src, sv)

    def link(self):
        for sv, ld in self.svld:
            if self.__status_link(sv, ld) == 0:
                self.log.info("SV:%s is already linked to LD:%s" % (sv, ld))
            else:
                self.array.sc_link_ld(sv, ld)

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

