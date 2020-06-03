import json

import core.exceptions as ex
from utilities.proc import justcall

class AmazonMixin(object):
    instance_id = None
    instance_data = None

    def aws(self, cmd, verbose=True):
        try:
            _cmd = [self.svc.aws]
        except AttributeError:
            _cmd = ["aws"]
        _cmd += ["--output=json"]
        if hasattr(self.svc, "aws_profile"):
            _cmd += ["--profile", self.svc.aws_profile]
        _cmd += cmd
        if verbose:
            self.log.info(" ".join(_cmd))
        out, err, ret = justcall(_cmd)
        if ret != 0:
            raise ex.Error(err)
        data = json.loads(out)
        return data

    def get_instance_id(self):
        if self.instance_id is not None:
            return self.instance_id
        try:
            import httplib
        except ImportError:
            raise ex.Error("the httplib module is required")
        c = httplib.HTTPConnection("instance-data")
        c.request("GET", "/latest/meta-data/instance-id")
        self.instance_id = c.getresponse().read()
        return self.instance_id

    def get_instance_data(self, refresh=False):
        if self.instance_data is not None and not refresh:
            return self.instance_data
        data = self.aws(["ec2", "describe-instances", "--instance-ids", self.get_instance_id()], verbose=False)
        try:
            self.instance_data = data["Reservations"][0]["Instances"][0]
        except Exception as e:
            self.instance_data = None
        return self.instance_data


