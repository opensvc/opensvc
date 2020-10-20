import datetime
import os
import time
import xml.etree.ElementTree as ElementTree

import core.exceptions as ex
import core.status
from .. import Sync, notify
from utilities.converters import print_duration
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symclone"
KEYWORDS = [
    {
        "keyword": "precopy",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "Use :opt:`-precopy` on recreate."
    },
    {
        "keyword": "recreate_timeout",
        "at": True,
        "default": 300,
        "convert": "duration",
        "text": "Maximum wait time for the clone to reach the created state."
    },
    {
        "keyword": "restore_timeout",
        "at": True,
        "default": 300,
        "convert": "duration",
        "text": "Maximum wait time for the clone to reach the restored state."
    },
    {
        "keyword": "symid",
        "required": True,
        "text": "Identifier of the symmetrix array hosting the source and target devices pairs pointed by :kw:`pairs`."
    },
    {
        "keyword": "pairs",
        "convert": "list",
        "required": True,
        "at": True,
        "text": "Whitespace-separated list of devices ``<src>:<dst>`` devid pairs to drive with this resource.",
        "example": "00B60:00B61 00B62:00B63",
    },
    {
        "keyword": "consistent",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "Use :opt:`-consistent` in symclone commands.",
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
    if which("symdg"):
        data.append("sync.symclone")
    return data


class SyncSymclone(Sync):
    def __init__(self,
                 type="sync.symclone",
                 symid=None,
                 pairs=None,
                 precopy=True,
                 consistent=True,
                 restore_timeout=None,
                 recreate_timeout=None,
                 **kwargs):
        super(SyncSymclone, self).__init__(type=type, **kwargs)

        if pairs is None:
            pairs = []
        if self.type == "sync.symclone":
            self.active_states = ["copied", "copyinprog"]
            self.activable_states = ["recreated", "precopy"]
        elif self.type == "sync.symsnap":
            self.active_states = ["copyonwrite"]
            self.activable_states = ["recreated", "created"]
        else:
            raise ex.InitError("unsupported symclone driver type %s", self.type)
        self.activate_timeout = 20
        self.recreate_timeout = recreate_timeout
        self.restore_timeout = restore_timeout
        self.precopy = precopy
        self.pairs_written = {}
        self.label = "symclone symid %s pairs %s" % (symid, " ".join(pairs))
        if len(self.label) > 80:
            self.label = self.label[:76] + "..."
        self.symid = symid
        self.pairs = pairs
        self.consistent = consistent
        self.svcstatus = {}
        self.default_schedule = "@0"
        if restore_timeout is None:
            self.restore_timeout = 300
        else:
            self.restore_timeout = restore_timeout

    def __str__(self):
        return "%s symid=%s pairs=%s" % (
            super(SyncSymclone, self).__str__(),
            self.symid,
            str(self.pairs)
        )

    def wait_for_devs_ready(self):
        pass

    def pairs_file(self, pairs=None):
        if pairs is None:
            suffix = ""
        else:
            suffix = "." + ",".join(pairs)
        return os.path.join(self.var_d, "pairs."+suffix)

    def write_pair_file(self, pairs=None):
        if pairs is None:
            _pairs = self.pairs
            key = "all"
        else:
            _pairs = pairs
            key = ",".join(pairs)
        if key in self.pairs_written:
            return
        pf = self.pairs_file(pairs)
        content = "\n".join(map(lambda x: x.replace(":", " "), _pairs))
        with open(pf, "w") as f:
            f.write(content)
        self.log.debug("wrote content '%s' in file '%s'" % (content, pf))
        self.pairs_written[key] = True

    def symclone_cmd(self, pairs=None):
        self.write_pair_file(pairs)
        return ['/usr/symcli/bin/symclone', '-sid', self.symid, '-f', self.pairs_file(pairs)]

    @lazy
    def active_pairs(self):
        active_pairs = []
        etree = self._showdevs()
        for e in etree.findall("Symmetrix/Device"):
            clone = e.find("CLONE_Device")
            if clone is None:
                dev = e.find("Dev_Info/dev_name").text
                self.status_log("%s has no clone info" % dev)
                continue
            src = e.find("CLONE_Device/SRC/dev_name").text
            dst = e.find("CLONE_Device/TGT/dev_name").text
            state = e.find("CLONE_Device/state").text.lower()
            if state in self.active_states:
                active_pairs.append(src+":"+dst)
        return active_pairs

    def is_active(self):
        self.unset_lazy("active_pairs")
        if len(self.active_pairs) == len(self.pairs):
            return True
        return False

    def is_activable(self):
        for state in self.activable_states:
            cmd = self.symclone_cmd() + ['verify', '-'+state]
            (ret, out, err) = self.call(cmd)
            if ret == 0:
                return True
        return False

    def wait_restored(self):
        interval = 10
        count = self.restore_timeout // interval
        if count == 0:
            count = 1
        cmd = self.symclone_cmd() + ['verify', '-restored', '-i', str(interval), '-c', str(count)]
        ret, out, err = self.vcall(cmd)
        if ret == 0:
           return
        cmd = self.symclone_cmd() + ['-noprompt', 'terminate', '-restored']
        cmd = " ".join(cmd)
        raise ex.Error("timeout waiting for devs to become 'restored'. "
                          "once restored, you can terminate running: " + cmd)

    def wait_for_active(self):
        delay = 10
        ass = " or ".join(self.active_states)
        for i in range(self.activate_timeout//delay+1):
            if self.is_active():
                return
            if i == 0:
                self.log.info("waiting for active state (max %i secs, %s)" % (self.activate_timeout, ass))
            time.sleep(delay)
        self.log.error("timed out waiting for active state (%i secs, %s)" % (self.activate_timeout, ass))
        ina = set(self.pairs) - set(self.active_pairs)
        ina = ", ".join(ina)
        raise ex.Error("%s still not in active state (%s)" % (ina, ass))

    def wait_for_activable(self):
        delay = 10
        ass = " or ".join(self.activable_states)
        for i in range(self.recreate_timeout//delay+1):
            if self.is_activable():
                return
            if i == 0:
                self.log.info("waiting for activable state (max %i secs, %s)" % (self.recreate_timeout, ass))
            time.sleep(delay)
        raise ex.Error("timed out waiting for activable state (%i secs, %s)" % (self.recreate_timeout, ass))

    def activate(self):
        if self.is_active():
            self.log.info("symclone target devices are already active")
            return
        self.wait_for_activable()
        self.unset_lazy("last")
        cmd = self.symclone_cmd() + ['-noprompt', 'activate', '-i', '20', '-c', '30']
        if self.consistent:
            cmd.append("-consistent")
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.Error
        self.wait_for_active()
        self.wait_for_devs_ready()

    def can_sync(self, target=None):
        try:
            self.check_requires("sync_update")
        except ex.Error as e:
            self.log.debug(e)
            return False
        return True

    def recreate(self):
        if self.is_activable():
            self.log.info("symclone are already recreated")
            return
        cmd = self.symclone_cmd() + ['-noprompt', 'recreate', '-i', '20', '-c', '30']
        if self.type == "sync.symclone" and self.precopy:
            cmd.append("-precopy")
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.Error

    def restore(self):
        self.unset_lazy("last")
        cmd = self.symclone_cmd() + ['-noprompt', 'restore']
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.Error(err)

    def terminate_restore(self):
        self.unset_lazy("last")
        cmd = self.symclone_cmd() + ['-noprompt', 'terminate', '-restored']
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.Error(err)

    def _info(self):
        data = [
          ["precopy", str(self.precopy)],
          ["pairs", str(self.pairs)],
          ["symid", str(self.symid)],
          ["consistent", str(self.consistent)],
        ]
        return data

    def split_pair(self, pair):
        l = pair.split(":")
        if len(l) != 2:
            raise ex.Error("pair %s malformed" % pair)
        return l

    def _showdevs(self):
        dst_devs = map(lambda x: x.split(":")[1], self.pairs)
        cmd = ['/usr/symcli/bin/symdev', '-sid', self.symid, 'list', '-v', '-devs', ','.join(dst_devs), '-output', 'xml_e']
        out, err, ret = justcall(cmd)
        etree = ElementTree.fromstring(out)
        return etree

    @lazy
    def showdevs_etree(self):
        data = {}
        etree = self._showdevs()
        for e in etree.findall("Symmetrix/Device"):
            dev_name = e.find("Dev_Info/dev_name").text
            data[dev_name] = e
        return data

    def last_action_dev(self, dev):
        # format: Thu Feb 25 10:20:56 2010
        try:
            s = self.showdevs_etree[dev].find("CLONE_Device/last_action").text
            return datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %Y")
        except AttributeError:
            return

    @lazy
    def last(self):
        self.unset_lazy("showdevs_etree")
        _last = None
        for pair in self.pairs:
            src, dst = self.split_pair(pair)
            dev_last = self.last_action_dev(dst)
            if dev_last is None:
                continue
            if _last is None or dev_last > _last:
                _last = dev_last
        return _last

    def _status(self, verbose=False):
        if self.last is None:
            return core.status.DOWN
        if len(self.active_pairs) not in (len(self.pairs), 0):
            self.status_log("cloneset has %d/%d active devs" % (len(self.active_pairs), len(self.pairs)))
            return core.status.WARN
        elif self.last < datetime.datetime.now() - datetime.timedelta(seconds=self.sync_max_delay):
            self.status_log("Last sync on %s older than %s"%(self.last, print_duration(self.sync_max_delay)))
            return core.status.WARN
        else:
            self.status_log("Last sync on %s" % self.last, "info")
            return core.status.UP

    def sync_break(self):
        self.activate()

    def sync_resync(self):
        self.recreate()

    @notify
    def sync_update(self):
        self.recreate()
        self.activate()

    def sync_restore(self):
        if not self.is_active():
            ina = set(self.pairs) - set(self.active_pairs)
            ina = ", ".join(ina)
            ass = " or ".join(self.active_states)
            raise ex.Error("%s not in active state (%s)" % (ina, ass))
        self.restore()
        self.wait_restored()
        self.terminate_restore()

    def start(self):
        self.activate()
