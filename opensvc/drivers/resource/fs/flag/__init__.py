import os

import core.status
from core.resource import Resource
from env import Env
from utilities.files import makedirs
from utilities.lazy import lazy
from utilities.proc import justcall


class BaseFsFlag(Resource):
    def __init__(self, type='fs.flag', **kwargs):
        super(BaseFsFlag, self).__init__(type=type, **kwargs)

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
        return core.status.UP if self.has_it() else core.status.DOWN

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

    def abort_start(self):
        if self.svc.topology != "failover":
            return
        if self.is_standby:
            return
        if self.svc.kind == "vol":
            # volumes are slaves of their consumer svc
            return
        try:
            for node in self.svc.nodes:
                if node == Env.nodename:
                    continue
                cmd = Env.rsh.split() + [node, "test", "-f", self.flag_f]
                out, err, ret = justcall(cmd)
                if ret == 0:
                    self.log.error("already up on %s", node)
                    return True
            return False
        except Exception as exc:
            self.log.exception(exc)
            return True

    def provisioner(self):
        pass

    def unprovisioner(self):
        pass

    def provisioned(self):
        flag = self.is_provisioned_flag()
        if flag is None:
            return False
        return flag

