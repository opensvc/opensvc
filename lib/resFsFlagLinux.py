import os
import resources
from rcUtilities import lazy, makedirs
import rcStatus

class Fs(resources.Resource):
    def __init__(self, **kwargs):
        resources.Resource.__init__(self, type="fs.flag", **kwargs)

    @lazy
    def flag_f(self):
        return os.path.join(os.sep, "dev", "shm", "opensvc", self.svc.namespace, self.svc.kind, self.svc.name, self.rid+".flag")

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

    def stop(self):
        if self.has_it():
            self.log.info("unlink flag %s", self.flag_f)
            os.unlink(self.flag_f)

    def is_provisionned(self):
        return True
