import datetime

import core.exceptions as ex
import core.status
import drivers.array.nexenta as array_driver

from .. import Sync, notify
from env import Env
from core.objects.svcdict import KEYS
from utilities.lazy import lazy

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


class SyncNexenta(Sync):
    def __init__(self,
                 name=None,
                 path=None,
                 filer=None,
                 reversible=False,
                 **kwargs):
        super(SyncNexenta, self).__init__(type="sync.nexenta", **kwargs)
        self.pausable = False
        self.label = "nexenta autosync %s" % name
        self.autosync = name
        self.path = path
        self.reversible = reversible
        self.filer = filer
        self.master = None
        self.slave = None
        self.ts = None
        self.age = None
        self.props = None
        self.local = None
        self.remote = None

    def __str__(self):
        return "%s autosync=%s" % (
            super(SyncNexenta, self).__str__(),
            self.autosync
        )

    @lazy
    def filers(self):
        data = {}
        for n in self.svc.nodes | self.svc.drpnodes:
            data[n] = self.oget("filer", impersonate=n)
        return data

    def can_sync(self, target=None):
        try:
            self.get_endpoints()
        except ex.Error as e:
            self.log.error(str(e))
            raise ex.Error

        if self.ts is None:
            self.get_props()
        return True

    def sync_swap(self):
        # only available from CLI ?
        pass

    @notify
    def sync_update(self):
        try:
            self.get_endpoints()
        except ex.Error as e:
            self.log.error(str(e))
            raise ex.Error

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
        except ex.Error as e:
            self.log.error(str(e))
            raise ex.Error

    def sync_break(self):
        try:
            self.get_endpoints()
            self.unbind()
            self.master.autosync_disable(self.autosync)
            self.log.info("autosync disable submitted")
            self.wait_break()
        except ex.Error as e:
            self.log.error(str(e))
            raise ex.Error

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
        raise ex.Error

    def start(self):
        try:
            self.get_endpoints()
            self.local.set_can_mount(self.path)
            self.log.info("set 'canmount = on' on %s"%self.path)
        except ex.Error as e:
            self.log.error(str(e))
            raise ex.Error

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
            raise ex.Error("can not parse last sync date: %s"%self.props['zfs/time_started'])
        self.age = now - self.ts

    def _status(self, verbose=False):
        ret = core.status.UP
        try:
            self.get_endpoints()
            self.status_log("master head is %s"%self.master.head)
            self.get_props()
        except ex.Error as e:
            if 'message' in e.value:
                msg = e.value['message']
            else:
                msg = str(e)
            self.status_log(msg)
            return core.status.WARN
        except:
            self.status_log("unexpected error")
            self.save_exc()
            return core.status.WARN

        limit = datetime.timedelta(seconds=self.sync_max_delay)
        if self.age > limit:
            self.status_log("last sync too old: %s ago"%str(self.age))
            ret = core.status.WARN
        s = self.master.autosync_get_state(self.autosync)
        if s not in ['online', 'running']:
            self.status_log("runner in '%s' state"%s)
            ret = core.status.WARN
        if ret == core.status.UP:
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
            raise ex.Error("two heads need to be setup")

        self.local = array_driver.Nexenta(self.filer, self.log)
        self.remote = array_driver.Nexenta(heads[0], self.log)

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
        except ex.Error as e:
            if 'does not exist' in str(e):
                path_props = self.local.get_props(self.path)
                if path_props is None:
                    raise ex.Error("path '%s' not found on local head '%s'"%(self.path, self.filer))
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
        except ex.Error as e:
            if 'does not exist' in str(e):
                path_props = self.remote.get_props(self.path)
                if path_props is None:
                    raise ex.Error("path '%s' not found on remote head '%s'"%(self.path, self.filer))
                self.slave = self.remote
                self.master = self.local
            elif localdown:
                raise ex.Error("both heads unreachable")

