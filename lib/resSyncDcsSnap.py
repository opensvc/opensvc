#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcExceptions as ex
import rcStatus
import datetime
import resSyncDcs
from rcGlobalEnv import rcEnv
from rcUtilities import justcall

class syncDcsSnap(resSyncDcs.SyncDcs):
    def can_sync(self, target=None):
        ts = None
 
        """ get oldest snap
        """
        for snap in self.snapname:
            info = self.get_snap(snap)
            if info is None:
                self.log.debug("snap %s missing"%snap)
                return True
            _ts = info['TimeStamp']
            if ts is None or _ts < ts:
                ts = _ts
        return not self.skip_sync(ts)

    def update_snap(self):
        cmd = ""
        vars = ""
        for i, snap in enumerate(self.snapname):
            cmd += '$v%d=get-dcssnapshot -snapshot %s;'%(i, snap)
            vars += '$v%d '%i

        cmd += "echo %s|update-dcssnapshot -Y"%vars
        self.dcscmd(cmd, verbose=True)

    def get_snap(self, snap):
        if snap in self._info:
            return self._info[snap]

        cmd = 'get-dcssnapshot -snapshot %s;'%snap
        try:
            ret, out, err = self.dcscmd(cmd)
        except:
            return None

        info = {}
        lines = out.split('\n')
        for line in lines:
            l = line.split(': ')
            if len(l) != 2:
                continue
            var = l[0].strip()
            val = l[1].strip()
            if var == 'TimeStamp':
                info['TimeStamp'] = datetime.datetime.strptime(val, "%d/%m/%Y %H:%M:%S")
            elif var in ['Type', 'State', 'ActiveOperation', 'Failure']:
                info[var] = val
        if len(info) > 0:
            self._info[snap] = info
        return info

    def _status(self, verbose=False, skip_prereq=False):
        err = False
        errlog = []
        log = []
        try:
            self.get_auth()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        for snap in self.snapname:
            info = self.get_snap(snap)
            if info is None:
                errlog.append("snapshot %s does not exists"%snap)
                err |= True
                continue
            if info['State'] not in ['Healthy','Migrated']:
                errlog.append("snapshot %s state is %s"%(snap, info['State']))
                err |= True
            if info['Failure'] not in ['NoFailure']:
                errlog.append("snapshot %s failure state is %s"%(snap, info['Failure']))
                err |= True
            if info['TimeStamp'] < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
                errlog.append("snapshot %s too old"%snap)
                err |= True
            log.append("last update on %s"%info['TimeStamp'])
        if err:
            self.status_log('\n'.join(errlog))
            return rcStatus.WARN
        self.status_log('\n'.join(log))
        return rcStatus.UP

    def syncresync(self):
        self.update_snap()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync", 'hb', 'app']))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __init__(self, rid=None, snapname=set([]), manager=set([]), dcs=set([]),
                 sync_max_delay=None, sync_interval=None, sync_days=None,
                 sync_period=None,
                 optional=False, disabled=False, tags=set([]), internal=False):
        resSyncDcs.SyncDcs.__init__(self, rid=rid, type="sync.dcssnap",
                              manager=manager,
                              dcs=dcs,
                              sync_max_delay=sync_max_delay,
                              sync_interval=sync_interval,
                              sync_days=sync_days,
                              sync_period=sync_period,
                              optional=optional, disabled=disabled, tags=tags)

        self.label = "DCS snapshot %s"%', '.join(snapname)
        self.snapname = snapname
        self._info = {}

    def __str__(self):
        return "%s dcs=%s manager=%s snapname=%s" % (
                 resSync.Sync.__str__(self),
                 ' '.join(self.dcs),
                 ' '.join(self.manager),
                 ' '.join(self.snapname))

