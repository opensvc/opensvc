import os

import core.exceptions as ex
import rcStatus

from . import BASE_KEYWORDS
from rcGlobalEnv import rcEnv
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

        if not which("share"):
            raise ex.InitError("share is not installed")
        self.label = "nfs:"+path
        self.path = path
        try:
            self.opts = self.parse_opts(opts)
        except ex.Error as e:
            raise ex.InitError(str(e))

    def get_opts(self):
        cmd = ["share", "-F", "nfs", "-A"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return ""
        for line in out.splitlines():
            words = line.split()
            if len(words) != 3:
                continue
            path = words[1]
            if path != self.path:
                continue
            opts = words[2]
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
        except ex.Error as e:
            self.log.error("skip start because the share is in unknown state")
            return
        if up:
            self.log.info("%s is already up" % self.path)
            return
        if "unexpected options" in self.issues:
            self.log.info("reshare %s because unexpected options were detected"%self.path)
            cmd = ['unshare', '-F', 'nfs', self.path]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error(err)
        self.can_rollback = True
        cmd = ['share', '-F', 'nfs', '-o', self.opts, self.path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error(err)

    def stop(self):
        try:
            up = self.is_up()
        except ex.Error as e:
            self.log.error("continue with stop even if the share is in unknown state")
        if not up:
            self.log.info("%s is already down" % self.path)
            return 0
        cmd = ['unshare', '-F', 'nfs', self.path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def _status(self, verbose=False):
        try:
            up = self.is_up()
        except ex.Error as e:
            self.status_log(str(e))
            return rcStatus.WARN
        if len(self.issues) > 0:
            self.status_log(self.issues)
            return rcStatus.WARN
        if up:
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def parse_opts(self, opts):
        o = sorted(opts.split(','))
        out = []
        for e in o:
            if e.startswith('ro=') or e.startswith('rw=') or e.startswith('access='):
                opt, clients = e.split('=')
                clients = ':'.join(sorted(clients.split(':')))
                if len(clients) == 0:
                    continue
                out.append('='.join((opt, clients)))
            else:
                out.append(e)
        return ','.join(out)

    def post_provision_start(self):
        pass

