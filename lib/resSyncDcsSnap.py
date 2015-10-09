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
            cmd += '$v%d=get-dcssnapshot -snapshot %s -connection %s;'%(i, snap, self.conn)
            vars += '$v%d '%i

        cmd += "echo %s|update-dcssnapshot -Y -connection %s"%(vars, self.conn)
        self.dcscmd(cmd, verbose=True)

    def get_snaps(self):
        cmd = ""
        for i, snap in enumerate(self.snapname):
            cmd += 'get-dcssnapshot -snapshot %s -connection %s;'%(snap, self.conn)
        try:
            ret, out, err = self.dcscmd(cmd)
        except:
            return

        """
SourceLogicalDiskId      : ef989edf-4a6d-4af6-8b9d-bc6d2070a36c
DestinationLogicalDiskId : 24a6a20f-59f3-4c4d-b4de-9a4f45afff44
Type                     : Full
TimeStamp                : 17/01/2013 14:16:49
ActiveOperation          : NoOperation
State                    : Migrated
Failure                  : NoFailure
SequenceNumber           : 4677814244
Id                       : V.{06C86883-CF53-11E1-9203-441EA14CCCC6}-00000177--V
                           .{06C86883-CF53-11E1-9203-441EA14CCCC6}-000001D7
Caption                  : S64lmwbic6f-22-clone-02
ExtendedCaption          : S64lmwbic6f-22-clone-02 on SDSLMW03
Internal                 : False

SourceLogicalDiskId      : f0450ff7-076f-4dbc-bee0-25f6a586f5b2
DestinationLogicalDiskId : 17e98dbf-1457-41c9-aaad-5b6d7d8cc81c
Type                     : Full
TimeStamp                : 17/01/2013 14:16:49
ActiveOperation          : NoOperation
State                    : Migrated
Failure                  : NoFailure
SequenceNumber           : 4677814247
Id                       : V.{06C86883-CF53-11E1-9203-441EA14CCCC6}-0000017A--V
                           .{06C86883-CF53-11E1-9203-441EA14CCCC6}-000001DA
Caption                  : S64lmwbic6f-25-clone-02
ExtendedCaption          : S64lmwbic6f-25-clone-02 on SDSLMW03
Internal                 : False

"""
        info = {}
        lines = out.split('\n')
        for line in lines:
            l = line.split(': ')
            if len(l) != 2:
                continue
            var = l[0].strip()
            val = l[1].strip()
            if var == 'Internal' and len(info) > 0:
                self._info[info['Caption']] = info
                info = {}
            if var == 'TimeStamp':
                info['TimeStamp'] = datetime.datetime.strptime(val, "%d/%m/%Y %H:%M:%S")
            elif var in ['Type', 'State', 'ActiveOperation', 'Failure', 'Caption']:
                info[var] = val

    def get_snap(self, snap):
        if len(self._info) == 0:
            self.get_snaps()
        if snap not in self._info:
            return None
        return self._info[snap]

    def no_status(self):
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            return True
        s = self.svc.group_status(excluded_groups=set(["sync", "hb", "app"]))
        if s['overall'].status not in [rcStatus.UP, rcStatus.NA]:
            return True
        return False

    def _status(self, verbose=False, skip_prereq=False):
        if self.no_status():
            self.status_log("skip on secondary node")
            return rcStatus.NA

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

    def sync_resync(self):
        self.update_snap()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["sync", 'hb', 'app']))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()

    def __init__(self,
                 rid=None,
                 snapname=set([]),
                 manager=set([]),
                 dcs=set([]),
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 internal=False):
        resSyncDcs.SyncDcs.__init__(self,
                                    rid=rid,
                                    type="sync.dcssnap",
                                    manager=manager,
                                    dcs=dcs,
                                    sync_max_delay=sync_max_delay,
                                    schedule=schedule,
                                    optional=optional,
                                    disabled=disabled,
                                    subset=subset,
                                    tags=tags)

        self.label = "DCS snapshot %s"%', '.join(snapname)
        self.snapname = snapname
        self._info = {}

    def __str__(self):
        return "%s dcs=%s manager=%s snapname=%s" % (
                 resSync.Sync.__str__(self),
                 ' '.join(self.dcs),
                 ' '.join(self.manager),
                 ' '.join(self.snapname))

