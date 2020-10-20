import datetime

import core.exceptions as ex
import core.status
import drivers.array.hp3par as array_driver
from .. import Sync, notify
from env import Env
from core.objects.svcdict import KEYS
from utilities.lazy import lazy

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "hp3par"
KEYWORDS = [
    {
        "keyword": "array",
        "required": True,
        "at": True,
        "text": "Name of the HP 3par array to send commands to."
    },
    {
        "keyword": "method",
        "candidates": ["ssh", "cli"],
        "default": "ssh",
        "at": True,
        "text": "The method to use to submit commands to the arrays."
    },
    {
        "keyword": "mode",
        "required": True,
        "candidates": ["async", "sync"],
        "text": "Replication mode: Synchronous or Asynchronous"
    },
    {
        "keyword": "rcg",
        "required": True,
        "at": True,
        "text": "Name of the HP 3par remote copy group. The scoping syntax must be used to fully describe the replication topology."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class SyncHp3par(Sync):
    def __init__(self,
                 array=None,
                 method=None,
                 mode=None,
                 rcg=None,
                 **kwargs):
        super(SyncHp3par, self).__init__(type="sync.hp3par", **kwargs)
        self.pausable = False
        self.array = array
        self.rcg = rcg
        self.mode = mode
        self.method = method
        self.label = "hp3par %s %s"%(mode, self.rcg)
        self.array_obj = None
        self.remote_array_obj = None

    def __str__(self):
        return "%s array=%s method=%s mode=%s rcg=%s" % (
            super(SyncHp3par, self).__str__(),
            self.array,
            self.method,
            self.mode,
            self.rcg
        )

    def on_add(self):
        try:
            arrays = array_driver.Hp3pars(objects=[self.array], log=self.log, node=self.svc.node)
        except Exception as e:
            raise ex.Error(str(e))
        if len(arrays.arrays) == 1:
            self.array_obj = arrays.arrays[0]
        if self.array_obj is None:
            raise ex.Error("array %s is not accessible" % self.array)
        self.array_obj.path = self.svc.path

    @lazy
    def rcg_names(self):
        data = {}
        for node in self.svc.nodes | self.svc.drpnodes:
            array = self.oget("array", impersonate=node)
            rcg = self.oget("rcg", impersonate=node)
            data[array] = rcg
        return data

    def get_array_obj(self, target=None, log=False):
        if target is None:
            array_name = self.array
            return self.array_obj
        else:
            array_name = target
            if self.remote_array_obj is None:
                try:
                    self.remote_array_obj = array_driver.Hp3pars(objects=[target], log=self.log, node=self.svc.node).arrays[0]
                    if self.remote_array_obj is None:
                        raise ex.Error("array %s is not accessible" % array_name)
                    self.remote_array_obj.path = self.svc.path
                    return self.remote_array_obj
                except Exception as e:
                    raise ex.Error(str(e))

    def _cmd(self, cmd, target=None, log=False):
        array_obj = self.get_array_obj(target=target, log=self.log)
        if log:
            if target is not None:
                suffix = " (on " + target + ")"
            else:
                suffix = ""
            self.log.info(cmd+suffix)

        if log:
            out, err = array_obj.rcmd(cmd, log=log)
        else:
            out, err = array_obj.rcmd(cmd)

        if not log:
            return out, err
        if len(out) > 0:
            self.log.info(out)
        if len(err) > 0:
            self.log.error(err)
            raise ex.Error()
        return out, err

    def can_sync(self, target=None, s=None):
        return True

    def sync_resync(self):
        self.sync_update()

    def sync_swap(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary':
            self.log.error("rcopy group %s role is Primary. refuse to swap")
            raise ex.Error()
        self.stoprcopygroup()
        self.setrcopygroup_reverse()
        self.startrcopygroup()

    @notify
    def sync_update(self):
        self.syncrcopygroup()

    def sync_revert(self):
        self.setrcopygroup_revert()

    def sync_resume(self):
        self.startrcopygroup()

    def sync_quiesce(self):
        self.stoprcopygroup()

    def sync_break(self):
        self.stoprcopygroup()

    def start(self):
        data = self.showrcopy()
        target = data['rcg']['Target']
        if self.is_splitted(target):
            self.log.info("we are split from %s array" % target)
            self.start_splitted()
        else:
            self.log.info("we are joined with %s array" % target)
            self.start_joined()

    def start_joined(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary':
            self.log.info("rcopy group %s role is already Primary. skip" % self.rcg)
            return
        self.stoprcopygroup()
        self.setrcopygroup_reverse()
        if Env.nodename in self.svc.nodes:
            self.sync_resume()

    def start_splitted(self):
        self.setrcopygroup_failover()

    def stop(self):
        pass

    def setrcopygroup_revert(self):
        data = self.showrcopy()
        if data['rcg']['Role'] != 'Primary-Rev':
           self.log.error("rcopy group %s role is not Primary-Rev. refuse to setrcopygroup revert" % self.rcg)
           return
        self._cmd("setrcopygroup reverse -f -waittask -stopgroups -local -current %s" % self.rcg, log=True)
        self.clear_caches()

    def setrcopygroup_failover(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary-Rev':
           self.log.info("rcopy group %s role is already Primary-Rev. skip setrcopygroup failover" % self.rcg)
           return
        self._cmd("setrcopygroup failover -f -waittask %s" % self.rcg, log=True)
        self.clear_caches()

    def setrcopygroup_reverse(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary':
            self.log.info("rcopy group %s role is already Primary. skip setrcopygroup reverse" % self.rcg)
            return
        self._cmd("setrcopygroup reverse -f -waittask %s" % self.rcg, log=True)
        self.clear_caches()

    def syncrcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Role'] != 'Primary':
            self.log.info("rcopy group %s role is not Primary. skip sync" % self.rcg)
            return
        if data['rcg']['Mode'] == 'Periodic' and self.svc.options.cron:
            self.log.info("skip syncrcopy as group %s is in periodic mode" % self.rcg)
            return
        self._cmd("syncrcopy -w %s" % self.rcg, log=True)
        self.clear_caches()

    def startrcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Status'] == "Started":
            self.log.info("rcopy group %s is already started. skip startrcopygroup" % self.rcg)
            return
        if data['rcg']['Role'] != 'Primary':
            self.log.error("rcopy group %s role is not Primary. refuse to start rcopy" % self.rcg)
            raise ex.Error()
        self._cmd("startrcopygroup %s" % self.rcg, log=True)
        self.clear_caches()

    def stoprcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Status'] == "Stopped":
            self.log.info("rcopy group %s is already stopped. skip stoprcopygroup" % self.rcg)
            return
        if data['rcg']['Role'] == "Primary":
            self._cmd("stoprcopygroup -f %s" % self.rcg, log=True)
        else:
            target = data['rcg']['Target']
            if target not in self.rcg_names:
                raise ex.Error("target %s not found in rcg names (%s)" % (target, ", ".join([name for name in self.rcg_names])))
            self._cmd("stoprcopygroup -f %s" % self.rcg_names[target], target=target, log=True)
        self.clear_caches()

    def is_splitted(self, target):
        data = self.showrcopy_links()
        for h in data:
            if h['Target'] != target:
                continue
            if h['Status'] == "Up":
                return False
        return True

    def showrcopy_links(self):
        """
        Target,Node,Address,Status,Options
        baie-pra,0:2:4,20240002AC00992B,Down,
        baie-pra,1:2:3,21230002AC00992B,Down,
        receive,0:2:4,20240002AC00992B,Up,
        receive,1:2:3,21230002AC00992B,Up,
        """
        out, err = self._cmd("showrcopy links")
        cols = ["Target", "Node", "Address", "Status", "Options"]
        lines = out.split('\n')
        data = []
        for line in lines:
            v = line.strip().split(",")
            if len(v) != len(cols):
                continue
            h = {}
            for a, b in zip(cols, v):
                h[a] = b
            data.append(h)
        return data

    def clear_caches(self):
        self.array_obj.clear_showrcopy_cache()

    def showrcopy(self):
        return self.array_obj.showrcopy(self.rcg)

    def sync_status(self, verbose=False):
        if self.array_obj is None:
            self.status_log("array %s is not accessible" % self.array)
            return core.status.WARN

        try:
            data = self.showrcopy()
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN

        elapsed = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.sync_max_delay)
        r = None
        if data['rcg']['Status'] != "Started":
            self.status_log("rcopy group status is not Started (%s)"%data['rcg']['Status'])
            r = core.status.WARN
        if self.mode == "async" and data['rcg']['Mode'] != "Periodic":
            self.status_log("rcopy group mode is not Periodic (%s)"%data['rcg']['Mode'])
            r = core.status.WARN
        if self.mode == "sync" and data['rcg']['Mode'] != "Sync":
            self.status_log("rcopy group mode is not Sync (%s)"%data['rcg']['Mode'])
            r = core.status.WARN
        if self.mode == "async":
            l = [o for o in data['rcg']['Options'] if o.startswith('Period ')]
            if len(l) == 0:
                self.status_log("rcopy group period option is not set")
                r = core.status.WARN
        if 'auto_recover' not in data['rcg']['Options']:
            self.status_log("rcopy group auto_recover option is not set")
            r = core.status.WARN
        for vv in data['vv']:
            if vv['SyncStatus'] != 'Synced':
                self.status_log("vv %s SyncStatus is not Synced (%s)"%(vv['LocalVV'], vv['SyncStatus']))
                r = core.status.WARN
            if vv['LastSyncTime'] < elapsed:
                self.status_log("vv %s last sync too old (%s)"%(vv['LocalVV'], vv['LastSyncTime'].strftime("%Y-%m-%d %H:%M")))
                r = core.status.WARN

        if r is not None:
            return r

        return core.status.UP

