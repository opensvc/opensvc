#
# Copyright (c) 2014 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcExceptions as ex
import rcStatus
import time
import datetime
import resSync
import rcNecIsm

class syncNecIsmSnap(resSync.Sync):
    def __init__(self,
                 rid=None,
                 array_name=None,
                 devs=[],
                 sync_max_delay=None,
                 sync_interval=None,
                 sync_days=None,
                 sync_period=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 internal=False):
        resSync.Sync.__init__(self,
                              rid=rid, type="sync.necismsnap",
                              sync_max_delay=sync_max_delay,
                              sync_interval=sync_interval,
                              sync_days=sync_days,
                              sync_period=sync_period,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.label = "NecIsm snapshot %s"%(rid)
        self.devs = devs
        self.array = rcNecIsm.NecIsm(array_name)
        self.parse_devs(devs)

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
                raise ex.excInitError("%s: malformed devs" % self.rid)
            if sv in self.sv:
                raise ex.excInitError("%s: duplicate sv %s in devs" % (self.rid, sv))
            if ld in self.ld:
                raise ex.excInitError("%s: duplicate ld %s in devs" % (self.rid, ld))
            self.sv[sv] = sv
            self.ld[ld] = ld
            self.svld.append((sv, ld))

    def wait_for_devs_ready(self):
        pass

    def get_sv_ts(self, sv):
        try:
            data = self.array.sc_query_ld(sv)
        except ex.excError:
            self.status_log("unable to query SV:%s" % sv)
            return
        svinfo = data['sv'][0]
        begin = svinfo.index('[')+1
        end = svinfo.index(']')
        ts_s = svinfo[begin:end]
        ts = datetime.datetime.strptime(ts_s, "%Y/%m/%d %H:%M:%S")
        return ts

    def can_sync(self, target=None):
        ts = self.get_oldest_ts()
        return not self.skip_sync(ts)

    def get_oldest_ts(self):
        ts = None
        for sv, ld in self.svld:
            _ts = self.get_sv_ts(sv)
            if ts is None or _ts < ts:
                ts = _ts
        return ts

    def _status(self, verbose=False, skip_prereq=False):
        ret = 0
        ret += self._status_ts()
        ret += self._status_link()
        if ret > 0:
            return rcStatus.WARN
        return rcStatus.UP

    def _status_ts(self):
        ts = self.get_oldest_ts()
        if ts is None:
            return 1
        now = datetime.datetime.now()
        limit = now - datetime.timedelta(minutes=self.sync_max_delay)
        if ts < limit:
            self.status_log("snap too old (%s)" % ts.strftime("%Y-%m-%d %H:%M"))
            return rcStatus.WARN
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
        except ex.excError:
            self.status_log("unable to get SV:%s linkinfo" % sv)
            return 1
        l = [ dst for dst in li['dst'] if ld in dst.split() and 'link' in dst.split()]
        if len(l) != 1:
            self.status_log("LD:%s is not linked to SV:%s" % (ld, sv))
            return 1
        return 0

    def syncresync(self):
        self.unlink()
        self.create()
        self.link()

    def unlink(self):
        for sv, ld in self.svld:
            if self.__status_link(sv, ld) == 1:
                self.log.info("SV:%s is already unlinked from LD:%s" % (sv, ld))
            else:
                self.array.sc_unlink_ld(ld)

    def create(self):
        for sv, ld in self.svld:
            try:
                src = self.array.sc_query_ld(sv)['LD Name']
            except:
                raise ex.excError("can not determine source LD for SV:%s" % sv)
            self.array.sc_create_ld(src, sv)

    def link(self):
        for sv, ld in self.svld:
            if self.__status_link(sv, ld) == 0:
                self.log.info("SV:%s is already linked to LD:%s" % (sv, ld))
            else:
                self.array.sc_link_ld(sv, ld)

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync", 'hb']))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __str__(self):
        return "%s array_name=%s devs=%s" % (resSync.Sync.__str__(self),\
                self.array_name, str(self.devs))

