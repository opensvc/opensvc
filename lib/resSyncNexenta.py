import datetime
import os

import rcExceptions as ex
import rcStatus
import resSync

from rcGlobalEnv import rcEnv
from rcNexenta import Nexenta
from svcBuilder import sync_kwargs
from svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "nexenta"
KEYWORDS = [
    {
        "keyword": "name",
        "at": True,
        "required": True,
        "text": "The name of the Nexenta autosync configuration."
    },
    {
        "keyword": "filer",
        "at": True,
        "required": True,
        "text": "The name of the Nexenta local head. Must be set for each node using the scoping syntax."
    },
    {
        "keyword": "path",
        "at": True,
        "required": True,
        "text": "The path of the zfs to synchronize, as seen by the Nexenta heads."
    },
    {
        "keyword": "reversible",
        "at": True,
        "candidates": [True, False],
        "required": True,
        "text": "Defines if the replication link can be reversed. Set to ``false`` for prd to drp replications to protect production data."
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
    kwargs["name"] = svc.oget(s, "name")
    kwargs["path"] = svc.oget(s, "path")
    kwargs["reversible"] = svc.oget(s, "reversible")
    filers = {}
    for n in svc.nodes | svc.drpnodes:
        filers[n] = svc.oget(s, "filer", impersonate=n)
    kwargs["filers"] = filers
    kwargs.update(sync_kwargs(svc, s))
    r = SyncNexenta(**kwargs)
    svc += r


class SyncNexenta(resSync.Sync):
    def can_sync(self, target=None):
        try:
            self.get_endpoints()
        except ex.excError as e:
            self.log.error(str(e))
            raise ex.excError

        if self.ts is None:
            self.get_props()
        if self.skip_sync(self.ts):
            return False
        return True

    def sync_swap(self):
        # only available from CLI ?
        pass

    @resSync.notify
    def sync_update(self):
        try:
            self.get_endpoints()
        except ex.excError as e:
            self.log.error(str(e))
            raise ex.excError

        if not self.can_sync() and not self.svc.options.force:
            return
        s = self.master.autosync_get_state(self.autosync)
        if s == "disabled":
            self.log.error("update not applicable: disabled")
            return
        if s == "running":
            self.log.info("update not applicable: transfer in progress")
            return
        if s != "online":
            self.log.error("update not applicable: %s state"%s)
            return
        self.master.autosync_execute(self.autosync)
        self.log.info("autosync runner execution submitted")

    def bind(self):
        b = self.local.ssh_list_bindings()
        found = False
        for k in b:
            user, hostport = k.split('@')
            if hostport == self.remote.head:
                found = True
                break
        if found:
            self.log.info("%s head already bound"%self.remote.head)
        else:
            self.local.ssh_bind(self.remote.username, self.remote.head, self.remote.password)
            self.log.info("%s head bound"%self.remote.head)

    def unbind(self):
        b = self.local.ssh_list_bindings()
        done = False
        for k in b:
            user, hostport = k.split('@')
            if hostport != self.remote.head:
                continue
            self.local.ssh_unbind(user, hostport, '1')
            self.log.info("%s head unbound"%hostport)
            done = True
        if not done:
            self.log.info("%s head already unbound"%self.remote.head)

    def sync_resync(self):
        try:
            self.get_endpoints()
            self.bind()
            self.master.autosync_enable(self.autosync)
            self.log.info("autosync enable submitted")
        except ex.excError as e:
            self.log.error(str(e))
            raise ex.excError

    def sync_break(self):
        try:
            self.get_endpoints()
            self.unbind()
            self.master.autosync_disable(self.autosync)
            self.log.info("autosync disable submitted")
            self.wait_break()
        except ex.excError as e:
            self.log.error(str(e))
            raise ex.excError

    def wait_break(self):
        import time
        timeout = 5
        for i in range(timeout, 0, -1):
            s = self.master.autosync_get_state(self.autosync)
            if s == "disabled":
                return
            if i > 1:
                time.sleep(2)
        self.log.error("timed out waiting for disable to finish")
        raise ex.excError

    def start(self):
        try:
            self.get_endpoints()
            self.local.set_can_mount(self.path)
            self.log.info("set 'canmount = on' on %s"%self.path)
        except ex.excError as e:
            self.log.error(str(e))
            raise ex.excError

    def stop(self):
        pass

    def get_props(self):
        self.props = self.master.autosync_get_props(self.autosync)

        # timestamp format : 15:34:09,May27
        now = datetime.datetime.now()
        try:
            self.ts = datetime.datetime.strptime(str(now.year)+' '+self.props['zfs/time_started'], "%Y %H:%M:%S,%b%d")
            if now < self.ts:
                self.ts = datetime.datetime.strptime(str(now.year-1)+' '+self.props['zfs/time_started'], "%Y %H:%M:%S,%b%d")
        except ValueError:
            raise ex.excError("can not parse last sync date: %s"%self.props['zfs/time_started'])
        self.age = now - self.ts

    def _status(self, verbose=False):
        ret = rcStatus.UP
        try:
            self.get_endpoints()
            self.status_log("master head is %s"%self.master.head)
            self.get_props()
        except ex.excError as e:
            if 'message' in e.value:
                msg = e.value['message']
            else:
                msg = str(e)
            self.status_log(msg)
            return rcStatus.WARN
        except:
            self.status_log("unexpected error")
            self.save_exc()
            return rcStatus.WARN

        limit = datetime.timedelta(seconds=self.sync_max_delay)
        if self.age > limit:
            self.status_log("last sync too old: %s ago"%str(self.age))
            ret = rcStatus.WARN
        s = self.master.autosync_get_state(self.autosync)
        if s not in ['online', 'running']:
            self.status_log("runner in '%s' state"%s)
            ret = rcStatus.WARN
        if ret == rcStatus.UP:
            self.status_log("last sync %s ago"%str(self.age))
        return ret

    def get_endpoints(self):
        """ determine which head is the replication master and
            which is replication slave.
        """
        if self.local is not None and self.remote is not None:
            return

        heads = list(set(self.filers.values()) - set([self.filer]))
        if len(heads) != 1:
            raise ex.excError("two heads need to be setup")

        self.local = Nexenta(self.filer, self.log)
        self.remote = Nexenta(heads[0], self.log)

        prop = 'zfs/to-host'
        try:
            localdown = False
            props = self.local.autosync_get_props(self.autosync)
            if prop in props and props[prop] == self.filer:
                self.slave = self.local
                self.master = self.remote
            else:
                self.slave = self.remote
                self.master = self.local
            return
        except ex.excError as e:
            if 'does not exist' in str(e):
                path_props = self.local.get_props(self.path)
                if path_props is None:
                    raise ex.excError("path '%s' not found on local head '%s'"%(self.path, self.filer))
                self.slave = self.local
                self.master = self.remote
            else:
                # local head is down
                localdown = True

        try:
            props = self.remote.autosync_get_props(self.autosync)
            if prop in props and props[prop] == self.filer:
                self.slave = self.local
                self.master = self.remote
            else:
                self.slave = self.remote
                self.master = self.local
            return
        except ex.excError as e:
            if 'does not exist' in str(e):
                path_props = self.remote.get_props(self.path)
                if path_props is None:
                    raise ex.excError("path '%s' not found on remote head '%s'"%(self.path, self.filer))
                self.slave = self.remote
                self.master = self.local
            elif localdown:
                raise ex.excError("both heads unreachable")

    def __init__(self,
                 rid=None,
                 name=None,
                 path=None,
                 filers={},
                 reversible=False,
                 **kwargs):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.nexenta",
                              **kwargs)
        self.pausable = False
        self.label = "nexenta autosync %s"%name
        self.autosync = name
        self.filers = filers
        self.path = path
        self.reversible = reversible
        self.filer = filers[rcEnv.nodename]
        self.master = None
        self.slave = None
        self.ts = None
        self.age = None
        self.props = None
        self.local = None
        self.remote = None

    def __str__(self):
        return "%s autosync=%s" % (resSync.Sync.__str__(self),\
                self.autosync)

