import json

from utilities.proc import justcall
from utilities.string import is_string

class GceMixin(object):
    valid_auth = False

    def gce_auth(self):
        cmd = ["gcloud", "auth", "list", "--format", "json"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        self.log.debug(out)
        data = json.loads(out)
        if "active_account" not in data:
            return False
        if not is_string(data["active_account"]):
            return False
        if len(data["active_account"]) == 0:
            return False
        return True
        
    def wait_gce_auth(self):
        if self.valid_auth:
            return
        self.wait_for_fn(self.gce_auth, 120, 1, errmsg="waited 120 seconds for a valid gcloud auth")
        self.valid_auth = True

