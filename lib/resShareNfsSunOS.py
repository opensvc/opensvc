import os

import rcExceptions as ex
import rcStatus
import resShareNfs

from rcGlobalEnv import rcEnv
from rcUtilities import justcall, which
from resources import Resource
from svcBuilder import init_kwargs
from svcdict import KEYS

DRIVER_GROUP = "share"
DRIVER_BASENAME = "nfs"
KEYWORDS = resShareNfs.KEYWORDS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s) 
    kwargs["path"] = svc.oget(s, "path")
    kwargs["opts"] = svc.oget(s, "opts")
    r = Share(**kwargs)
    svc += r


class Share(Resource):

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
        except ex.excError as e:
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
                raise ex.excError(err)
        self.can_rollback = True
        cmd = ['share', '-F', 'nfs', '-o', self.opts, self.path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)

    def stop(self):
        try:
            up = self.is_up()
        except ex.excError as e:
            self.log.error("continue with stop even if the share is in unknown state")
        if not up:
            self.log.info("%s is already down" % self.path)
            return 0
        cmd = ['unshare', '-F', 'nfs', self.path]
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

    def __init__(self, rid, path, opts, **kwargs):
        Resource.__init__(self, rid, type="share.nfs", **kwargs)

        if not which("share"):
            raise ex.excInitError("share is not installed")
        self.label = "nfs:"+path
        self.path = path
        try:
            self.opts = self.parse_opts(opts)
        except ex.excError as e:
            raise ex.excInitError(str(e))


