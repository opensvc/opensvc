import json
from rcUtilities import which, justcall
import rcExceptions as ex

class Amazon(object):
    def aws(self, cmd, verbose=True):
        if hasattr(self.svc, "aws") and which(self.svc.aws) is not None:
            _cmd = [self.svc.aws]
        else:
            _cmd = ["aws"] 
        _cmd += ["--output=json"]
        if hasattr(self.svc, "aws_profile"):
            _cmd += ["--profile", self.svc.aws_profile]
        _cmd += cmd
        if verbose:
            self.log.info(" ".join(_cmd))
        out, err, ret = justcall(_cmd)
        if ret != 0:
            raise ex.excError(err)
        data = json.loads(out)
        return data

