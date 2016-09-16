import os

from rcGlobalEnv import rcEnv
import rcExceptions as ex
import rcStatus
import time
import resSync
import datetime
import rcHp3par as rc

class syncHp3par(resSync.Sync):
    def __init__(self,
                 rid=None,
                 array=None,
                 method=None,
                 mode=None,
                 rcg_names={},
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.hp3par",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)
        self.array = array
        self.rcg_names = rcg_names
        self.rcg = rcg_names[array]
        self.mode = mode
        self.method = method
        self.label = "hp3par %s %s"%(mode, self.rcg)
        try:
            arrays = rc.Hp3pars(objects=[self.array])
        except Exception as e:
            raise ex.excInitError(str(e))
        if len(arrays.arrays) == 1:
            self.array_obj = arrays.arrays[0]
        else:
            self.array_obj = None

    def __str__(self):
        return "%s array=%s method=%s mode=%s rcg=%s" % (
                resSync.Sync.__str__(self),
                self.array,
                self.method,
                self.mode,
                self.rcg)

    def on_add(self):
        if self.array_obj is None:
            raise ex.excError("no 3par array object")
        self.array_obj.svcname = self.svc.svcname

    def _cmd(self, cmd, target=None, log=False):
        if target is None:
            array_name = self.array
            array_obj = self.array_obj
        else:
            array_name = target
            if not hasattr(self, 'remote_array_obj'):
                try:
                    self.remote_array_obj = rc.Hp3pars(objects=[target]).arrays[0]
                except Exception as e:
                    raise ex.excError(str(e))
            array_obj = self.remote_array_obj

        if array_obj is None:
            raise ex.excError("array %s is not accessible" % array_name)
        if log:
            if target is not None:
                suffix = " (on " + target + ")"
            else:
                suffix = ""
            self.log.info(cmd+suffix)
        out, err = array_obj.rcmd(cmd, log=self.log)
        if not log:
            return out, err
        if len(out) > 0:
            self.log.info(out)
        if len(err) > 0:
            self.log.error(err)
            raise ex.excError()
        return out, err

    def can_sync(self, target=None, s=None):
        data = self.showrcopy()
        last = data['vv'][0]['LastSyncTime']
        if self.skip_sync(datetime.datetime.now()-last):
            return False
        return True

    def sync_resync(self):
        self.sync_update()

    def syncswap(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary':
            self.log.error("rcopy group %s role is Primary. refuse to swap")
            raise ex.excError()
        self.stoprcopygroup()
        self.setrcopygroup_reverse()
        self.startrcopygroup()

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

    def setrcopygroup_failover(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary-Rev':
           self.log.info("rcopy group %s role is already Primary-Rev. skip setrcopygroup failover" % self.rcg)
           return
        self._cmd("setrcopygroup failover -f -waittask %s" % self.rcg, log=True)

    def setrcopygroup_reverse(self):
        data = self.showrcopy()
        if data['rcg']['Role'] == 'Primary':
            self.log.info("rcopy group %s role is already Primary. skip setrcopygroup reverse" % self.rcg)
            return
        self._cmd("setrcopygroup reverse -f -waittask %s" % self.rcg, log=True)

    def syncrcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Role'] != 'Primary':
            self.log.error("rcopy group %s role is not Primary. refuse to sync" % self.rcg)
            raise ex.excError()
        self._cmd("syncrcopy -w %s" % self.rcg, log=True)

    def startrcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Status'] == "Started":
            self.log.info("rcopy group %s is already started. skip startrcopygroup" % self.rcg)
            return
        if data['rcg']['Role'] != 'Primary':
            self.log.error("rcopy group %s role is not Primary. refuse to start rcopy" % self.rcg)
            raise ex.excError()
        self._cmd("startrcopygroup %s" % self.rcg, log=True)

    def stoprcopygroup(self):
        data = self.showrcopy()
        if data['rcg']['Status'] == "Stopped":
            self.log.info("rcopy group %s is already stopped. skip stoprcopygroup" % self.rcg)
            return
        if data['rcg']['Role'] == "Primary":
            self._cmd("stoprcopygroup -f %s" % self.rcg, log=True)
        else:
            target = data['rcg']['Target']
            self._cmd("stoprcopygroup -f %s" % self.rcg_names[target], target=target, log=True)

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
        baie-pra.cgr.fr,0:2:4,20240002AC00992B,Down,
        baie-pra.cgr.fr,1:2:3,21230002AC00992B,Down,
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

    def showrcopy(self, refresh=False):
        """
	Remote Copy System Information
	Status: Started, Normal

	Group Information

	Name        ,Target    ,Status  ,Role      ,Mode    ,Options
	RCG.SVCTEST1,baie-pra.cgr.fr,Started,Primary,Periodic,"Last-Sync 2014-03-05 10:19:42 CET , Period 5m, auto_recover,over_per_alert"
	 ,LocalVV     ,ID  ,RemoteVV    ,ID  ,SyncStatus   ,LastSyncTime
	 ,LXC.SVCTEST1.DATA01,2706,LXC.SVCTEST1.DATA01,2718,Synced,2014-03-05 10:19:42 CET
	 ,LXC.SVCTEST1.DATA02,2707,LXC.SVCTEST1.DATA02,2719,Synced,2014-03-05 10:19:42 CET

        """
        if not refresh and hasattr(self, "showrcopy_cache"):
            return self.showrcopy_cache

        out, err = self._cmd("showrcopy groups %s" % self.rcg)
        cols_rcg = ["Name", "Target", "Status", "Role", "Mode"]
        cols_vv = ["LocalVV", "ID", "RemoteVV", "ID", "SyncStatus", "LastSyncTime"]
        lines = out.split('\n')

        if "does not exist" in err:
            raise ex.excError("rcg does not exist")

        if len(out) == 0:
            raise ex.excError("unable to fetch rcg status")

        # RCG status
        rcg_s = lines[0]
        options_start = rcg_s.index('"')
        rcg_options = rcg_s[options_start+1:-1].split(",")
        rcg_options = map(lambda x: x.strip(), rcg_options)
        rcg_v = rcg_s[:options_start].split(",")
        rcg_data = {}
        for a, b in zip(cols_rcg, rcg_v):
            rcg_data[a] = b
        rcg_data["Options"] = rcg_options

        # VV status
        vv_l = []
        for line in lines[1:]:
            v = line.strip().strip(",").split(",")
            if len(v) != len(cols_vv):
                continue
            vv_data = {}
            for a, b in zip(cols_vv, v):
                vv_data[a] = b
            vv_data['LastSyncTime'] = self.lastsync_s_to_datetime(vv_data['LastSyncTime'])
            vv_l.append(vv_data)
        data = {'rcg': rcg_data, 'vv': vv_l}
        self.showrcopy_cache = data
        return self.showrcopy_cache

    def lastsync_s_to_datetime(self, s):
        try:
            d = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S %Z")
        except ValueError:
            # workaround hp-ux python 2.6
            s = s.replace("CET", "MET")
            d = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S %Z")
        return d

    def _status(self, verbose=False):
        if self.array_obj is None:
            self.status_log("array %s is not accessible" % self.array)
            return rcStatus.WARN

        try:
            data = self.showrcopy(refresh=True)
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.UNDEF

        elapsed = datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay)
        r = None
        if data['rcg']['Status'] != "Started":
            self.status_log("rcopy group status is not Started (%s)"%data['rcg']['Status'])
            r = rcStatus.WARN
        if self.mode == "async" and data['rcg']['Mode'] != "Periodic":
            self.status_log("rcopy group mode is not Periodic (%s)"%data['rcg']['Mode'])
            r = rcStatus.WARN
        if self.mode == "sync" and data['rcg']['Mode'] != "Sync":
            self.status_log("rcopy group mode is not Sync (%s)"%data['rcg']['Mode'])
            r = rcStatus.WARN
        if self.mode == "async":
            l = [o for o in data['rcg']['Options'] if o.startswith('Period ')]
            if len(l) == 0:
                self.status_log("rcopy group period option is not set")
                r = rcStatus.WARN
        if 'auto_recover' not in data['rcg']['Options']:
            self.status_log("rcopy group auto_recover option is not set")
            r = rcStatus.WARN
        for vv in data['vv']:
            if vv['SyncStatus'] != 'Synced':
                self.status_log("vv %s SyncStatus is not Synced (%s)"%(vv['LocalVV'], vv['SyncStatus']))
                r = rcStatus.WARN
            if vv['LastSyncTime'] < elapsed:
                self.status_log("vv %s last sync too old (%s)"%(vv['LocalVV'], vv['LastSyncTime'].strftime("%Y-%m-%d %H:%M")))
                r = rcStatus.WARN

        if r is not None:
            return r

        return rcStatus.UP

if __name__ == "__main__":
    o = syncHp3par(rid="sync#1", mode="async", array="baie-pra", rcg="RCG.SVCTEST1")
    print(o)
