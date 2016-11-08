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
        return os.path.join(rcEnv.pathvar, self.svc.svcname, "pairs."+self.rid+suffix)

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

    def is_active(self):
        for pair in self.pairs:
            if pair in self.active_pairs:
                continue
            found = False
            for state in self.active_states:
                cmd = self.symclone_cmd([pair]) + ['verify', '-'+state]
                out, err, ret = justcall(cmd)
                if ret == 0:
                    self.active_pairs.append(pair)
                    break
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

    def wait_for_active(self):
        delay = 10
        self.active_pairs = []
        ass = " or ".join(self.active_states)
        for i in range(self.activate_timeout//delay+1):
            if self.is_active():
                return
            if i == 0:
                self.log.info("waiting for active state (max %i secs, %s)" % (timeout, ass))
            time.sleep(delay)
        self.log.error("timed out waiting for active state (%i secs, %s)" % (timeout, ass))
        ina = set(self.pairs) - set(self.active_pairs)
        ina = map(lambda x: ' '.join(x), ina)
        ina = ", ".join(ina)
        raise ex.excError("%s still not in active state (%s)" % (ina, ass))

    def wait_for_activable(self):
        delay = 10
        ass = " or ".join(self.activable_states)
        for i in range(self.recreate_timeout//delay+1):
            if self.is_activable():
                return
            if i == 0:
                self.log.info("waiting for activable state (max %i secs, %s)" % (self.activate_timeout, ass))
            time.sleep(delay)
        raise ex.excError("timed out waiting for activable state (%i secs, %s)" % (self.activate_timeout, ass))

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
        try:
            self.check_depends("sync_update")
        except ex.excError as e:
            self.log.debug(e)
            return False

        self.get_last()
        if self.skip_sync(self.last):
            return False
        return True

    def recreate(self):
        self.get_last()
        if self.skip_sync(self.last):
            return
        if self.is_activable():
            self.log.info("symclone are already recreated")
            return
        cmd = self.symclone_cmd() + ['-noprompt', 'recreate', '-i', '20', '-c', '30']
        if self.type == "sync.symclone" and self.precopy:
            cmd.append("-precopy")
        (ret, out, err) = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.excError

    def info(self):
        data = [
          ["precopy", str(self.precopy)],
          ["pairs", str(self.pairs)],
          ["symid", str(self.symid)],
          ["consistent", str(self.consistent)],
        ]
        return self.fmt_info(data)

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

    def __init__(self,
                 rid=None,
                 type="sync.symclone",
                 symid=None,
                 pairs=[],
                 precopy=True,
                 consistent=True,
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

        if self.type == "sync.symclone":
            self.active_states = ["copied", "copyinprog"]
            self.activable_states = ["recreated", "precopy"]
        elif self.type == "sync.symsnap":
            self.active_states = ["copyonwrite"]
            self.activable_states = ["recreated", "created"]
        else:
            raise ex.excInitError("unsupported symclone driver type %s", self.type)
        self.activate_timeout = 20
        self.recreate_timeout = 20
        self.precopy = precopy
        self.pairs_written = {}
        self.label = "symclone symid %s pairs %s" % (symid, " ".join(pairs))
        if len(self.label) > 80:
            self.label = self.label[:76] + "..."
        self.symid = symid
        self.pairs = pairs
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

