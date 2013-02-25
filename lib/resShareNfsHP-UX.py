#
# Copyright (c) 2013 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

from rcGlobalEnv import rcEnv
from rcUtilities import justcall, which
import rcStatus
import rcExceptions as ex
from resources import Resource

class Share(Resource):
    def get_opts(self):
        if not os.path.exists(self.sharetab):
            return self.data
        with open(self.sharetab, 'r') as f:
            buff = f.read()
        for line in buff.split('\n'):
            words = line.split()
            if len(words) != 4:
                continue
            path = words[0]
            if path != self.path:
                continue
            res = words[1]
            fstype = words[2]
            if fstype != "nfs":
                continue
            opts = words[3]
            return self.parse_opts(opts)
        return ""
        
    def is_up(self):
        self.issues = ""
        opts = self.get_opts()
        if len(opts) == 0:
            return False
        if opts != self.opts:
            self.issues = "%s exported with unexpected options: %s, expected %s"%(self.path, opts, self.opts)
            return False
        return True

    def start(self):
        try:
            up = self.is_up()
        except ex.excError, e:
            self.log.error("skip start because the share is in unknown state")
            return
        if up:
            self.log.info("%s is already up" % self.path)
            return
        if "unexpected options" in self.issues:
            self.log.info("reshare %s because unexpected options were detected"%self.path)
            cmd = [ 'unshare', '-F', 'nfs', self.path ]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.excError(err)
        self.can_rollback = True
        cmd = [ 'share', '-F', 'nfs', '-o', self.opts, self.path ]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)

    def stop(self):
        try:
            up = self.is_up()
        except ex.excError, e:
            self.log.error("continue with stop even if the share is in unknown state")
        if not up:
            self.log.info("%s is already down" % self.path)
            return 0
        cmd = [ 'unshare', '-F', 'nfs', self.path ]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def _status(self, verbose=False):
        try:
            up = self.is_up()
        except ex.excError, e:
            self.status_log(str(e))
            return rcStatus.WARN
        if len(self.issues) > 0:
            self.status_log(self.issues)
            return rcStatus.WARN
        if rcEnv.nodename in self.always_on:
            if up: return rcStatus.STDBY_UP
            else: return rcStatus.STDBY_DOWN
        else:
            if up: return rcStatus.UP
            else: return rcStatus.DOWN

    def parse_opts(self, opts):
        o = sorted(opts.split(','))
        out = []
        for e in o:
            if e.startswith('ro=') or e.startswith('rw=') or e.startswith('access='):
                opt, clients = e.split('=')
                clients = ':'.join(sorted(clients.split(':')))
                out.append('='.join((opt, clients)))
            else:
                out.append(e)
        return ','.join(out)

    def __init__(self, rid, path, opts, always_on=set([]),
                 disabled=False, tags=set([]), optional=False, monitor=False):
        Resource.__init__(self, rid, type="share.nfs", always_on=always_on,
                          disabled=disabled, tags=tags, optional=optional,
                          monitor=monitor)
        self.sharetab = "/etc/dfs/sharetab"
        self.dfstab = "/etc/dfs/dfstab"

        if not which("share"):
            raise ex.excInitError("share is not installed")
        self.label = "nfs:"+path
        self.path = path
        try:
            self.opts = self.parse_opts(opts)
        except ex.excError, e:
            raise ex.excInitError(str(e))


