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

class syncNetapp(Res.Resource):
    def master(self):
        s = self.local_snapmirror_status()
        return s['master']

    def slave(self):
        s = self.local_snapmirror_status()
        return s['slave']

    def local(self):
        if rcEnv.nodename in self.filers:
            return self.filers[rcEnv.nodename]
        return None

    def _cmd(self, cmd, target, info=False):
        if target == "local":
            filer = self.local()
        elif target == "master":
            filer = self.master()
        elif target == "slave":
            filer = self.slave()
        elif target in self.filers.values():
            filer = target
        else:
            self.log.error("unable to find the %s filer"%target)
            raise ex.excError

        return self.call(rcEnv.rsh.split() + [self.user+'@'+filer] + cmd, info=info)

    def cmd_master(self, cmd, info=False):
        return self._cmd(cmd, "master", info=info)

    def cmd_slave(self, cmd, info=False):
        return self._cmd(cmd, "slave", info=info)

    def cmd_local(self, cmd, info=False):
        return self._cmd(cmd, "local", info=info)

    def lagged(self, lag, max=None):
        if max is None:
            max = self.sync_max_delay
        l = lag.split(":")
        if len(l) != 3:
            self.log.error("unexpected lag format")
            raise ex.excError
        if int(l[0]) * 60 + int(l[1]) > max:
            return True
        return False

    def syncresync(self):
        (ret, buff) = self.cmd_slave(['snapmirror', 'resync', '-f', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def syncswap(self):
        master = self.master()
        slave = self.slave()
        s = self.snapmirror_status(self.local())
        if s['state'] != "Broken-off":
            self.log.error("can not swap: snapmirror is not in state Broken-off")
            raise ex.excError
        src = slave+':'+self.path_short
        dst = master+':'+self.path_short

        (ret, buff) = self._cmd(['snapmirror', 'resync', '-f', '-S', src, dst], master, info=True)
        if ret != 0:
            raise ex.excError
        (ret, buff) = self._cmd(['snapmirror', 'release', self.path_short, src], master, info=True)
        if ret != 0:
            raise ex.excError
        (ret, buff) = self._cmd(['snapmirror', 'status', '-l', dst], slave, info=False)
        if ret != 0:
            raise ex.excError
        snap = ""
        state = ""
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 2:
                continue
            if l[0] == "State:":
                state = l[1]
            if state != "Broken-off":
                continue
            if l[0] == "Base" and l[1] == "Snapshot:":
                snap = l[-1]
                break
        if len(snap) == 0:
            self.log.error("can not determine base snapshot name to remove on %s"%slave)
            raise ex.excError
        import time
        time.sleep(5)
        (ret, buff) = self._cmd(['snap', 'delete', self.path_short, snap], slave, info=True)
        if ret != 0:
            raise ex.excError

    def syncupdate(self):
        s = self.snapmirror_status(self.slave())
        if s['state'] == "Quiesced":
            self.log.error("update not applicable: quiesced")
            return
        if s['state'] == "Snapmirrored" and s['status'] == "Transferring":
            self.log.info("update not applicable: transfer in progress")
            return
        if s['state'] != "Snapmirrored" or s['status'] != "Idle":
            self.log.error("update not applicable: not in snapmirror idle status")
            return
        if not self.svc.force and not self.lagged(s['lag'], self.sync_min_delay):
            self.log.debug("last sync too close")
            return
        (ret, buff) = self.cmd_slave(['snapmirror', 'update', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def syncresume(self):
        s = self.snapmirror_status(self.slave())
        if s['state'] != "Quiesced":
            self.log.info("resume not applicable: not quiesced")
            return
        (ret, buff) = self.cmd_slave(['snapmirror', 'resume', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def syncquiesce(self):
        s = self.snapmirror_status(self.slave())
        if s['state'] == "Quiesced":
            self.log.info("already quiesced")
            return
        elif s['state'] != "Snapmirrored":
            self.log.error("Can not quiesce: volume not in Snapmirrored state")
            raise ex.excError
        if s['status'] == "Pending":
            self.log.error("Can not quiesce: volume in snapmirror Pending status")
            raise ex.excError
        (ret, buff) = self.cmd_slave(['snapmirror', 'quiesce', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError
        self.wait_quiesce()

    def syncbreak(self):
        (ret, buff) = self.cmd_slave(['snapmirror', 'break', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError
        self.wait_break()

    def wait_quiesce(self):
        timeout = 60
        self.log.info("start waiting quiesce to finish (max %s seconds)"%(timeout*5))
        for i in range(timeout):
            s = self.snapmirror_status(self.slave())
            if s['state'] == "Quiesced" and s['status'] == "Idle":
                return
            time.sleep(5)
        self.log.error("timed out waiting for quiesce to finish")
        raise ex.excError

    def wait_break(self):
        timeout = 20
        for i in range(timeout):
            s = self.snapmirror_status(self.slave())
            if s['state'] == "Broken-off" and s['status'] == "Idle":
                return
            time.sleep(5)
        self.log.error("timed out waiting for break to finish")
        raise ex.excError

    def snapmirror_status(self, filer):
        (ret, buff) = self._cmd(['snapmirror', 'status'], filer)
        if ret != 0:
	    self.log.error("can get snapmirror status from %s"%filer)
            raise ex.excError
        key = ':'.join([filer, self.path_short])
        list = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 5:
                continue
            if l[2] == "Uninitialized":
                continue
            if l[0] == key or l[1] == key:
                list.append(l)
        if len(list) == 0:
	    self.log.error("%s not found in snapmirror status"%self.path_short)
            raise ex.excError
        elif len(list) == 1:
            l = list[0]
            master = l[0].split(':')[0]
            slave = l[1].split(':')[0]
            return dict(master=master, slave=slave, state=l[2], lag=l[3], status=l[4])
        else:
            self.log.error("%s is in an unsupported state. Please repair manually."%filer)
            raise ex.excError

    def local_snapmirror_status(self):
        return self.snapmirror_status(self.local())

    def start(self):
        if self.local() == self.master():
            self.log.info("%s is already replication master"%self.local())
            return
        s = self.snapmirror_status(self.slave())
        if s['state'] != "Broken-off":
            try:
                self.syncquiesce()
            except:
                if self.svc.force:
                    self.log.warning("force mode is on. bypass failed quiesce.")
                    pass
                else:
                    self.log.error("set force mode to bypass")
                    raise ex.excError
            self.syncbreak()
        if rcEnv.host_mode == "PRD":
            self.syncswap()

    def stop(self):
        pass

    def status(self):
        try:
            s = self.snapmirror_status(self.slave())
        except:
            return rcStatus.UNDEF
        if s['state'] == "Snapmirrored":
            if "Transferring" in s['status']:
                self.log.debug("snapmirror transfer in progress")
                return rcStatus.WARN
            elif self.lagged(s['lag']):
                self.log.debug("snapmirror lag beyond sync_max_delay")
                return rcStatus.WARN
            else:
                return rcStatus.UP
        return rcStatus.DOWN

    def __init__(self, rid=None, filers={}, path=None, user=None,
                 sync_max_delay=1440, sync_min_delay=30,
                 optional=False, disabled=False, internal=False):
        self.label = "netapp %s on %s"%(path, ', '.join(filers.values()))
        self.filers = filers
        self.path = path
        self.user = user
        self.sync_max_delay = sync_max_delay
        self.sync_min_delay = sync_min_delay
        self.path_short = self.path.replace('/vol/','')
        Res.Resource.__init__(self, rid, "sync.netapp", optional, disabled)

    def __str__(self):
        return "%s filers=%s user=%s path=%s" % (Res.Resource.__str__(self),\
                self.filers, self.user, self.path)

