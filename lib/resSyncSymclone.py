#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which
import rcExceptions as ex
import rcStatus
import resources as Res
import time
import datetime

class syncSymclone(Res.Resource):
    def get_symdevs(self):
        for symdev in self.symdevs:
            l = symdev.split(':')
            if len(l) != 2:
                self.log.error("symdevs must be in symid:symdev ... format")
                raise ex.excError
            self.symdev[l[0],l[1]] = dict(symid=l[0], symdev=l[1])

    def get_symld(self):
        cmd = ['/usr/symcli/bin/symld', '-g', self.symdg, 'list', '-v']
        (ret, out) = self.call(cmd)
        if ret != 0:
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
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-copied']+self.pairs
        (ret, out) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_activable(self):
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, 'verify', '-precopy', '-cycled']+self.pairs
        (ret, out) = self.call(cmd)
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
            self.pairs += [srcld, 'sym', 'ld', tgtld]

    def wait_for_copied(self):
        delay = 20
        timeout = 300
        self.log.info("waiting for copied state (max %i secs)"%timeout)
        for i in range(timeout/delay):
            time.sleep(delay)
            if self.is_active():
                return
        self.log.error("timed out waiting for copied state (%i secs)"%timeout)
        raise ex.excError

    def wait_for_precopied(self):
        delay = 30
        self.log.info("waiting for precopied state (max %i secs)"%self.precopy_timeout)
        for i in range(self.precopy_timeout/delay):
            time.sleep(delay)
            if self.is_activable():
                return
        self.log.error("timed out waiting for precopied state (%i secs)"%self.precopy_timeout)
        raise ex.excError

    def activate(self):
        self.get_syminfo()
        if self.is_active():
            self.log.info("symclone dg %s is already active"%self.symdg)
            return
        if not self.is_activable():
            self.log.info("symclone dg %s is not activable (not in precopied+cycle state)"%self.symdg)
            self.wait_for_precopied()
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, '-noprompt', 'activate', '-i', '20', '-c', '30']+self.pairs
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.wait_for_copied()

    def recreate(self):
        self.get_syminfo()
        self.get_svcstatus()
        if self.svcstatus['overall'].status != rcStatus.DOWN:
            self.log.error("the service (sync excluded) is in '%s' state. Must be in 'down' state"%self.svcstatus['overall'])
            raise ex.excError
        if not self.is_active():
            self.log.info("symclone dg %s is already in precopy state"%self.symdg)
            return
        self.get_last()
        if self.last > datetime.datetime.now() - datetime.timedelta(minutes=self.sync_min_delay):
            self.log.info("last symclone resync of %s occured less than %s minutes ago (sync_min_delay)"%(self.symdg, self.sync_min_delay))
            return
        cmd = ['/usr/symcli/bin/symclone', '-g', self.symdg, '-noprompt', 'recreate', '-precopy', '-i', '20', '-c', '30']+self.pairs
        (ret, out) = self.vcall(cmd)
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

    def status(self):
        self.get_syminfo()
        self.get_last()

        if self.last is None:
            return rcStatus.DOWN
        elif self.last < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
            return rcStatus.WARN
        else:
            return rcStatus.UP

    def syncbreak(self):
        self.activate()

    def syncresync(self):
        self.recreate()

    def start(self):
        self.syncbreak()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync"]))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __init__(self, rid=None, symdg=None, symdevs=[], precopy_timeout=300,
                 sync_max_delay=1440, sync_min_delay=30,
                 optional=False, disabled=False, internal=False):
        self.label = "clone symdg %s"%(symdg)
        self.symdg = symdg
        self.symdevs = symdevs
        self.precopy_timeout = precopy_timeout
        self.sync_max_delay = sync_max_delay
        self.sync_min_delay = sync_min_delay
        Res.Resource.__init__(self, rid, "sync.symclone", optional, disabled)
        self.disks = set([])
        self.symdev = {}
        self.svcstatus = {}
        self.symld = {}
        self.pairs = []
        self.last = None

    def __str__(self):
        return "%s symdg=%s dg=%s" % (Res.Resource.__str__(self),\
                self.symdg, self.dg)

