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
        for filer in set(self.filers.values()):
            s = self.cmd_status(filer)
            if s['state'] == "Source":
                return filer
        self.log.error("unable to find replication master between %s"%self.filers.values())
        raise ex.excError

    def slave(self):
        for filer in set(self.filers.values()):
            s = self.cmd_status(filer)
            if s['state'] in ["Snapmirrored", "Quiesced", "Broken-off"]:
                return filer
        self.log.error("unable to find replication slave between %s"%self.filers.values())
        raise ex.excError

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

    def syncresync(self):
        (ret, buff) = self.cmd_slave(['snapmirror', 'resync', '-f', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def syncquiesce(self):
        (ret, buff) = self._cmd(['snapmirror', 'status'], self.slave())
        if s['state'] == "Quiesced":
            self.log.info("already quiesced")
            return
        elif s['state'] != "Snapmirrored":
            self.log.error("Can not quiesced volume not in Snapmirrored state")
            raise ex.excError
        (ret, buff) = self.cmd_slave(['snapmirror', 'quiesce', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def syncbreak(self):
        (ret, buff) = self.cmd_slave(['snapmirror', 'break', self.slave()+':'+self.path_short], info=True)
        if ret != 0:
            raise ex.excError

    def wait_quiesce(self):
        timeout = 20
        for i in range(timeout):
            s = self.cmd_status(self.slave())
            if s['state'] == "Quiesced" and s['status'] == "Idle":
                return
            time.sleep(5)
        self.log.error("timed out waiting for quiesce to finish")
        raise ex.excError

    def wait_break(self):
        timeout = 20
        for i in range(timeout):
            s = self.cmd_status(self.slave())
            if s['state'] == "Broken-off" and s['status'] == "Idle":
                return
            time.sleep(5)
        self.log.error("timed out waiting for break to finish")
        raise ex.excError

    def cmd_status(self, filer):
        (ret, buff) = self._cmd(['snapmirror', 'status'], filer)
        if ret != 0:
            return rcStatus.UNDEF
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            w = l[1].split(':')
            if len(w) < 2:
                continue
            path = w[1]
            if path != self.path_short:
                continue
            return dict(state=l[2], lag=l[3], status=l[4])
	self.log.error("%s not found in snapmirror status"%self.path_short)
	raise ex.excError

    def start(self):
        if self.local() == self.master():
            self.log.info("%s is already replication master"%self.local())
            return
        self.syncquiesce()
        self.wait_quiesce()
        self.syncbreak()
        self.wait_break()

    def stop(self):
        pass

    def status(self):
        s = self.cmd_status(self.slave())
        if s['state'] == "Snapmirrored":
            if "Transferring" in s['status']:
                self.log.debug("snapmirror transfer in progress")
                return rcStatus.WARN
            else:
                return rcStatus.UP
        return rcStatus.DOWN

    def __init__(self, filers={}, path=None, user=None,
                 optional=False, disabled=False, internal=False):
        self.id = "sync netapp %s %s"%(path, filers.values())
        self.filers = filers
        self.path = path
        self.user = user
        self.path_short = self.path.replace('/vol/','')
        Res.Resource.__init__(self, "sync.netapp", optional, disabled)

    def __str__(self):
        return "%s filers=%s user=%s path=%s" % (Res.Resource.__str__(self),\
                self.filers, self.user, self.path)

