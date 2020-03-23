import os

import rcStatus

from resources import Resource
from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, justcall


class BaseFsFlag(Resource):
    def __init__(self, **kwargs):
        super().__init__(type="fs.flag", **kwargs)

    @lazy
    def base_flag_d(self):
        "return directory where flag files are created"
        return os.path.join(os.sep, 'tmp', 'opensvc')

    @lazy
    def flag_f(self):
        flag_name = os.path.join(self.svc.kind, self.svc.name, self.rid+".flag")
        if self.svc.namespace:
            flag_name = os.path.join(self.svc.namespace, flag_name)
        return os.path.join(self.base_flag_d, flag_name)

    @lazy
    def flag_d(self):
        return os.path.dirname(self.flag_f)

    def touch(self, fpath):
        if os.path.exists(fpath):
            os.utime(fpath, None)
        else:
            open(fpath, "a").close()

    def has_it(self):
        return os.path.exists(self.flag_f)

    def _status(self, verbose=False):
        return rcStatus.UP if self.has_it() else rcStatus.DOWN

    def start(self):
        if not self.has_it():
            self.log.info("create flag %s", self.flag_f)
            makedirs(self.flag_d, mode=0o700)
            self.touch(self.flag_f)
            self.can_rollback = True

    def stop(self):
        if self.has_it():
            self.log.info("unlink flag %s", self.flag_f)
            os.unlink(self.flag_f)

    def is_provisionned(self):
        return True

    def abort_start(self):
        if self.svc.topology != "failover":
            return
        if self.standby:
            return
        try:
            for node in self.svc.nodes:
                if node == rcEnv.nodename:
                    continue
                cmd = rcEnv.rsh.split() + [node, "test", "-f", self.flag_f]
                out, err, ret = justcall(cmd)
                if ret == 0:
                    self.log.error("already up on %s", node)
                    return True
            return False
        except Exception as exc:
            self.log.exception(exc)
            return True
