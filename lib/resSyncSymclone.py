import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which, justcall
import rcExceptions as ex
import rcStatus
import time
import datetime
import resSync
import xml.etree.ElementTree as ElementTree

class syncSymclone(resSync.Sync):
    def wait_for_devs_ready(self):
        pass

    def pairs_file(self, pairs=None):
        if pairs is None:
            suffix = ""
        else:
            suffix = "." + ",".join(pairs)
        return os.path.join(rcEnv.pathvar, self.svc.svcname, "pairs."+self.rid)

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

    def is_active_snap(self):
        for pair in self.pairs:
            if pair in self.active_pairs:
                continue
            cmd = self.symclone_cmd([pair]) + ['verify', '-copyonwrite']
            out, err, ret = justcall(cmd)
            if ret == 0:
                self.active_pairs.append(pair)
                continue
        if len(self.active_pairs) == len(self.pairs):
            return True
        return False

    def is_active_clone(self):
        for pair in self.pairs:
            if pair in self.active_pairs:
                continue
            cmd = self.symclone_cmd([pair]) + ['verify', '-copied']
            out, err, ret = justcall(cmd)
            if ret == 0:
                self.active_pairs.append(pair)
                continue
            cmd = self.symclone_cmd([pair]) + ['verify', '-copyinprog']
            out, err, ret = justcall(cmd)
            if ret == 0:
                self.active_pairs.append(pair)
                continue
        if len(self.active_pairs) == len(self.pairs):
            return True
        return False

    def is_activable_snap(self):
        cmd = self.symclone_cmd() + ['verify', '-recreated']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        cmd = self.symclone_cmd() + ['verify', '-created']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_activable_clone(self):
        cmd = self.symclone_cmd() + ['verify', '-precopy']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def wait_for_active(self):
        delay = 20
        timeout = 300
        self.active_pairs = []
        for i in range(timeout/delay):
            if self.is_active():
                return
            if i == 0:
                self.log.info("waiting for copied or copyinprog state (max %i secs)"%timeout)
            time.sleep(delay)
        self.log.error("timed out waiting for copied or copyinprog state (%i secs)"%timeout)
        ina = set(self.pairs) - set(self.active_pairs)
        ina = map(lambda x: ' '.join(x), ina)
        ina = ", ".join(ina)
        raise ex.excError("%s still not in copied or copyinprod state"%ina)

    def wait_for_activable(self):
        delay = 30
        for i in range(self.precopy_timeout/delay):
            if self.is_activable():
                return
            if i == 0:
                self.log.info("waiting for precopy state (max %i secs)"%self.precopy_timeout)
            time.sleep(delay)
        raise ex.excError("timed out waiting for precopy state (%i secs)"%self.precopy_timeout)

    def activate(self):
        if self.is_active():
            self.log.info("symclone target devices are already active")
            return
        self.wait_for_activable()
        cmd = self.symclone_cmd() + ['-noprompt', 'activate', '-i', '20', '-c', '30']
        if self.consistent:
            cmd.append("-consistent")
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.excError
        self.wait_for_active()
        self.wait_for_devs_ready()

    def can_sync(self, target=None):
        self.get_last()
        if skip_sync(self.last):
            return False
        return True

    def recreate(self):
        self.get_last()
        if self.skip_sync(self.last):
            return
        self.get_svcstatus()
        if self.svcstatus['overall'].status != rcStatus.DOWN:
            self.log.error("the service (sync excluded) is in '%s' state. Must be in 'down' state"%self.svcstatus['overall'])
            raise ex.excError
        if self.is_activable():
            self.log.info("symclone are already recreated")
            return
        cmd = self.symclone_cmd() + ['-noprompt', 'recreate', '-i', '20', '-c', '30']
        if self.type == "sync.symclone":
            cmd.append("-precopy")
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.excError

    def split_pair(self, pair):
        l = pair.split(":")
        if len(l) != 2:
            raise ex.excError("pair %s malformed" % pair)
        return l

    def showdevs(self):
        if len(self.showdevs_etree) > 0:
            return
        dst_devs = map(lambda x: x.split(":")[1], self.pairs)
        cmd = ['/usr/symcli/bin/symdev', '-sid', self.symid, 'list', '-v', '-devs', ','.join(dst_devs), '-output', 'xml_e']
        out, err, ret = justcall(cmd)
        etree = ElementTree.fromstring(out)
        etree = ElementTree.fromstring(out)
        for e in etree.findall("Symmetrix/Device"):
            dev_name = e.find("Dev_Info/dev_name").text
            self.showdevs_etree[dev_name] = e

    def last_action_dev(self, dev):
        # format: Thu Feb 25 10:20:56 2010
        self.showdevs()
        s = self.showdevs_etree[dev].find("CLONE_Device/last_action").text
        return datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %Y")

    def get_last(self):
        if self.last is not None:
            return
        self.showdevs()
        for pair in self.pairs:
            src, dst = self.split_pair(pair)
            last = self.last_action_dev(dst)
            if self.last is None or last > self.last:
                self.last = last

    def _status(self, verbose=False):
        self.get_last()
        if self.last is None:
            return rcStatus.DOWN
        elif self.last < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
            self.status_log("Last sync on %s older than %d minutes"%(self.last, self.sync_max_delay))
            return rcStatus.WARN
        else:
            self.status_log("Last sync on %s" % self.last, "info")
            return rcStatus.UP

    def sync_break(self):
        self.activate()

    def sync_resync(self):
        self.recreate()

    def sync_update(self):
        self.recreate()
        self.activate()

    def start(self):
        self.activate()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync", 'hb']))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __init__(self,
                 rid=None,
                 symid=None,
                 pairs=[],
                 precopy_timeout=300,
                 consistent=True,
                 type="sync.symclone",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type=type,
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        if type == "sync.symclone":
            self.is_active = self.is_active_clone
            self.is_activable = self.is_activable_clone
        elif type == "sync.symsnap":
            self.is_active = self.is_active_snap
            self.is_activable = self.is_activable_snap
        else:
            raise ex.excInitError("unsupported symclone driver type %s", type)
        self.pairs_written = {}
        self.label = "symclone symid %s pairs %s" % (symid, " ".join(pairs))
        if len(self.label) > 80:
            self.label = self.label[:76] + "..."
        self.symid = symid
        self.pairs = pairs
        self.precopy_timeout = precopy_timeout
        self.consistent = consistent
        self.disks = set([])
        self.svcstatus = {}
        self.active_pairs = []
        self.last = None
        self.showdevs_etree = {}
        self.default_schedule = "@0"

    def __str__(self):
        return "%s symid=%s pairs=%s" % (resSync.Sync.__str__(self),\
                self.symid, str(self.pairs))

