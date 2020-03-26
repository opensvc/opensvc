import os

import rcStatus
import core.exceptions as ex

from . import BASE_KEYWORDS
from rcGlobalEnv import rcEnv
from rcUtilities import cache, clear_cache
from core.resource import Resource
from svcBuilder import init_kwargs
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which

DRIVER_GROUP = "share"
DRIVER_BASENAME = "nfs"

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=BASE_KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["path"] = svc.oget(s, "path")
    kwargs["opts"] = svc.oget(s, "opts")
    r = NfsShare(**kwargs)
    svc += r


class NfsShare(Resource):
    def __init__(self, path, opts, **kwargs):
        Resource.__init__(self, type="share.nfs", **kwargs)
        if not which("exportfs"):
            raise ex.InitError("exportfs is not installed")
        self.label = "nfs:"+path
        self.path = path
        l = opts.replace('\\', '').split()
        self.opts = {}
        for e in l:
            try:
                client, opts = self.parse_entry(e)
            except ex.Error as e:
                raise ex.InitError(str(e))
            self.opts[client] = opts

    @cache("showmount.e")
    def get_showmount(self):
        self.data = {}
        cmd = ["showmount", "-e", "--no-headers"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("nfs server not operational")
        for line in out.splitlines():
            try:
                idx = line.rindex(" ")
            except IndexError:
                continue
            path = line[0:idx].strip()
            ips = line[idx+1:].split(",")
            if ips == ['(everyone)']:
                ips = '*'
            self.data[path] = ips
        return self.data

    @cache("exportfs.v")
    def get_exports(self):
        self.data = {}
        cmd = ["exportfs", "-v"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        out = out.replace('\n ', '').replace('\n\t', '')
        for line in out.splitlines():
            words = line.split()
            if len(words) != 2:
                continue
            path = words[0]
            e = words[1]
            if path not in self.data:
                self.data[path] = {}
            try:
                client, opts = self.parse_entry(e)
            except ex.Error as e:
                continue
            if client == '<world>':
                client = '*'
            self.data[path][client] = opts
        return self.data

    def is_up(self):
        self.issues = {}
        self.issues_missing_client = []
        self.issues_wrong_opts = []
        self.issues_none = []
        exports = self.get_exports()
        if self.path not in exports:
            return False
        try:
            showmount = self.get_showmount()
        except ex.Error as exc:
            self.status_log(str(exc), "info")
            return False
        if self.path not in showmount:
            self.status_log("%s in userland etab but not in kernel etab" % self.path)
            return False
        for client in self.opts:
            if client not in exports[self.path]:
                self.issues[client] = "%s not exported to client %s"%(self.path, client)
                self.issues_missing_client.append(client)
            elif showmount[self.path] != "*" and client not in showmount[self.path]:
                self.issues[client] = "%s not exported to client %s in kernel etab"%(self.path, client)
                self.issues_missing_client.append(client)
            elif self.opts[client] > exports[self.path][client]:
                self.issues[client] = "%s is exported to client %s with missing options: current '%s', minimum required '%s'"%(self.path, client, ','.join(exports[self.path][client]), ','.join(self.opts[client]))
                self.issues_wrong_opts.append(client)
            else:
                self.issues_none.append(client)
        return True

    def start(self):
        try:
            up = self.is_up()
        except ex.Error as e:
            self.log.error("skip start because the share is in unknown state")
            return

        if up and len(self.issues) == 0:
            self.log.info("%s is already up" % self.path)
            return

        self.can_rollback = True
        for client, opts in self.opts.items():
            if client in self.issues_none:
                continue

            if client in self.issues_wrong_opts:
                cmd = [ 'exportfs', '-u', ':'.join((client, self.path)) ]
                ret, out, err = self.vcall(cmd)

            cmd = [ 'exportfs', '-o', ','.join(opts), ':'.join((client, self.path)) ]
            ret, out, err = self.vcall(cmd)
            clear_cache("exportfs.v")
            clear_cache("showmount.e")
            if ret != 0:
                raise ex.Error

    def stop(self):
        try:
            up = self.is_up()
        except ex.Error as e:
            self.log.error("continue with stop even if the share is in unknown state")
        if not up:
            self.log.info("%s is already down" % self.path)
            return 0
        for client in self.opts:
            cmd = [ 'exportfs', '-u', ':'.join((client, self.path)) ]
            ret, out, err = self.vcall(cmd)
            clear_cache("exportfs.v")
            clear_cache("showmount.e")
            if ret != 0:
                raise ex.Error

    def _status(self, verbose=False):
        try:
            up = self.is_up()
        except ex.Error as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if len(self.issues) > 0:
            self.status_log('\n'.join(self.issues.values()))
            return rcStatus.WARN
        if up:
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def parse_entry(self, e):
        if '(' not in e or ')' not in e:
            raise ex.Error("malformed share opts: '%s'. must be in client(opts) client(opts) format"%e)
        _l = e.split('(')
        client = _l[0]
        opts = _l[1].strip(')')
        return client, set(opts.split(','))

    def post_provision_start(self):
        pass


