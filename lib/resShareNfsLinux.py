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
    def get_exports(self):
        self.data = {}
        cmd = [ 'exportfs', '-v' ]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        out = out.replace('\n ', '').replace('\n\t', '')
        for line in out.split('\n'):
            words = line.split()
            if len(words) != 2:
                continue
            path = words[0]
            e = words[1]
            if path not in self.data:
                self.data[path] = {}
            try:
                client, opts = self.parse_entry(e)
            except ex.excError as e:
                continue
            if client == '<world>':
                client = '*'
            self.data[path][client] = opts
        return self.data
        
    def is_up(self):
        self.issues = {}
        exports = self.get_exports()
        if self.path not in exports:
            return False
        for client in self.opts:
            if client not in exports[self.path]:
                self.issues[client] = "%s not exported to client %s"%(self.path, client)
            elif self.opts[client] > exports[self.path][client]:
                self.issues[client] = "%s is exported to client %s with missing options: current '%s', minimum required '%s'"%(self.path, client, ','.join(exports[self.path][client]), ','.join(self.opts[client]))
        return True

    def start(self):
        try:
            up = self.is_up()
        except ex.excError as e:
            self.log.error("skip start because the share is in unknown state")
            return
        if up:
            self.log.info("%s is already up" % self.path)
            return
        self.can_rollback = True
        for client, opts in self.opts.items():
            if client in self.issues:
                cmd = [ 'exportfs', '-u', ':'.join((client, self.path)) ]
                ret, out, err = self.vcall(cmd)

            cmd = [ 'exportfs', '-o', ','.join(opts), ':'.join((client, self.path)) ]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.excError

    def stop(self):
        try:
            up = self.is_up()
        except ex.excError as e:
            self.log.error("continue with stop even if the share is in unknown state")
        if not up:
            self.log.info("%s is already down" % self.path)
            return 0
        for client in self.opts:
            cmd = [ 'exportfs', '-u', ':'.join((client, self.path)) ]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.excError

    def _status(self, verbose=False):
        try:
            up = self.is_up()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if len(self.issues) > 0:
            self.status_log('\n'.join(self.issues.values()))
            return rcStatus.WARN
        if rcEnv.nodename in self.always_on:
            if up: return rcStatus.STDBY_UP
            else: return rcStatus.STDBY_DOWN
        else:
            if up: return rcStatus.UP
            else: return rcStatus.DOWN

    def parse_entry(self, e):
        if '(' not in e or ')' not in e:
            raise ex.excError("malformed share opts: '%s'. must be in client(opts) client(opts) format"%e)
        _l = e.split('(')
        client = _l[0]
        opts = _l[1].strip(')')
        return client, set(opts.split(','))

    def __init__(self, rid, path, opts, always_on=set([]),
                 disabled=False, tags=set([]), optional=False, monitor=False):
        Resource.__init__(self, rid, type="share.nfs", always_on=always_on,
                          disabled=disabled, tags=tags, optional=optional,
                          monitor=monitor)
        if not which("exportfs"):
            raise ex.excInitError("exportfs is not installed")
        self.label = "nfs:"+path
        self.path = path
        l = opts.replace('\\', '').split()
        self.opts = {}
        for e in l:
            try:
                client, opts = self.parse_entry(e)
            except ex.excError as e:
                raise ex.excInitError(str(e))
            self.opts[client] = opts
            

