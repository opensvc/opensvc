import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which, justcall
import rcExceptions as ex
import rcStatus
import time
import datetime
import resSync

class syncSymclone(resSync.Sync):
    def wait_for_devs_ready(self):
        pass

    def get_symdevs(self):
        for symdev in self.symdevs:
            l = symdev.split(':')
            if len(l) != 2:
                self.log.error("symdevs must be in symid:symdev ... format")
                raise ex.excError
            self.symdev[l[0],l[1]] = dict(symid=l[0], symdev=l[1])

    def get_symld(self):
        cmd = ['/usr/symcli/bin/symld', '-g', self.symdg, 'list', '-v']
        out, err, ret = justcall(cmd)
        if ret != 0:
            if len(err) > 0:
                self.status_log(err.strip())
            raise ex.excError
        ld = {}
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            l = line.split(': ')
            if len(l) != 2:
                continue
            if "Device Physical Name" in l[0]:
                """ New logical device
                """
                if ld != {}:
                    self.symld[ld['symid'],ld['symdev']] = ld
                    ld = {}
                ld['pdev'] = l[1].strip()
            elif "  Symmetrix ID" in l[0]:
                ld['symid'] = l[1].strip()
            elif "  Device Symmetrix Name" in l[0]:
                ld['symdev'] = l[1].strip()
            elif "  Device Logical Name" in l[0]:
                ld['symld'] = l[1].strip()
            elif "  Product Revision" in l[0]:
                ld['symrev'] = l[1].strip()
            elif "  Source (SRC) Device Symmetrix Name" in l[0]:
                ld['clone_srcdev'] = l[1].strip()
            elif "  Target (TGT) Device Symmetrix Name" in l[0]:
                ld['clone_tgtdev'] = l[1].strip()
            elif "  State of Session" in l[0]:
                ld['clone_state'] = l[1].strip()
            elif "  Time of Last Clone Action" in l[0]:
                ld['clone_lastaction'] = l[1].strip()
            elif "  Changed Tracks for SRC Device" in l[0]:
                ld['clone_srcchangedtracks'] = l[1].strip()
            elif "  Changed Tracks for TGT Device" in l[0]:
                ld['clone_tgtchangedtracks'] = l[1].strip()
        if ld != {}:
            self.symld[ld['symid'],ld['symdev']] = ld

    def is_active(self):
        for pair in self._pairs:
            if pair in self.active_pairs:
                continue
            cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-copied']+pair
            out, err, ret = justcall(cmd)
            if ret == 0:
                self.active_pairs.append(pair)
                continue
            cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-copyinprog']+pair
            out, err, ret = justcall(cmd)
            if ret == 0:
                self.active_pairs.append(pair)
                continue
        if len(self.active_pairs) == len(self._pairs):
            return True
        return False

    def is_copied(self):
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-copied']+self.pairs
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_activable(self):
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-precopy']+self.pairs
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def get_pairs(self):
        for symid, dev in self.symdev:
            ld = self.symld[symid,dev]
            if dev == ld['clone_tgtdev']:
                tgtld = ld['symld']
                srcld = self.symld[ld['symid'],ld['clone_srcdev']]['symld']
            else:
                self.log.error("device %s not a clone target"%(dev))
                raise ex.excError
            pair = [srcld, 'sym', 'ld', tgtld]
            self.pairs += pair
            self._pairs += [pair]

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
        ina = set(self._pairs) - set(self.active_pairs)
        ina = map(lambda x: ' '.join(x), ina)
        ina = ", ".join(ina)
        self.log.error("%s still not in copied or copyinprod state"%ina)
        raise ex.excError

    def wait_for_activable(self):
        delay = 30
        for i in range(self.precopy_timeout/delay):
            if self.is_activable():
                return
            if i == 0:
                self.log.info("waiting for precopied state (max %i secs)"%self.precopy_timeout)
            time.sleep(delay)
        self.log.error("timed out waiting for precopied state (%i secs)"%self.precopy_timeout)
        raise ex.excError

    def activate(self):
        self.get_syminfo()
        if self.is_active():
            self.log.info("symclone dg %s is already active"%self.symdg)
            return
        self.wait_for_activable()
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, '-noprompt', 'activate', '-i', '20', '-c', '30']+self.pairs
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.wait_for_active()
        self.wait_for_devs_ready()

    def can_sync(self, target=None):
        self.get_syminfo()
        self.get_last()
        if skip_sync(self.last):
            return False
        return True

    def recreate(self):
        self.get_syminfo()
        self.get_last()
        if self.skip_sync(self.last):
            return
        self.get_svcstatus()
        if self.svcstatus['overall'].status != rcStatus.DOWN:
            self.log.error("the service (sync excluded) is in '%s' state. Must be in 'down' state"%self.svcstatus['overall'])
            raise ex.excError
        if not self.is_copied():
            self.log.info("symclone dg %s is not fully copied"%self.symdg)
            return
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, '-noprompt', 'recreate', '-precopy', '-i', '20', '-c', '30']+self.pairs
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def get_syminfo(self):
        self.get_symdevs()
        self.get_symld()
        self.get_pairs()

    def get_last(self):
        if self.last is not None:
            return
        for symid, symdev in self.symdev:
            ld = self.symld[symid,symdev]
            # format: Thu Feb 25 10:20:56 2010
            last = datetime.datetime.strptime(ld['clone_lastaction'], "%a %b %d %H:%M:%S %Y")
            if self.last is None or last > self.last:
                self.last = last

    def _status(self, verbose=False):
        try:
            self.get_syminfo()
            self.get_last()
        except:
            return rcStatus.WARN

        if self.last is None:
            return rcStatus.DOWN
        elif self.last < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
            self.status_log("Last sync on %s older than %d minutes"%(self.last, self.sync_max_delay))
            return rcStatus.WARN
        else:
            return rcStatus.UP

    def sync_break(self):
        self.activate()

    def sync_resync(self):
        self.recreate()

    def start(self):
        self.sync_break()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync", 'hb']))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __init__(self,
                 rid=None,
                 symdg=None,
                 symdevs=[],
                 precopy_timeout=300,
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.symclone",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.label = "clone symdg %s"%(symdg)
        self.symdg = symdg
        self.symdevs = symdevs
        self.precopy_timeout = precopy_timeout
        self.disks = set([])
        self.symdev = {}
        self.pdevs = {}
        self.svcstatus = {}
        self.symld = {}
        self.pairs = []
        self._pairs = []
        self.active_pairs = []
        self.last = None

    def __str__(self):
        return "%s symdg=%s symdevs=%s" % (resSync.Sync.__str__(self),\
                self.symdg, self.symdevs)

