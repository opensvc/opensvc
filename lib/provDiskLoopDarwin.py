import provisioning
import os
import rcExceptions as ex
from converters import convert_size

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        try:
            return os.path.exists(self.r.loopFile)
        except Exception:
            return

    def unprovisioner(self):
        try:
            self.path = self.r.loopFile
        except Exception as e:
            raise ex.excError(str(e))

        if not self.is_provisioned():
            return

        self.r.log.info("unlink %s" % self.path)
        os.unlink(self.path)
        self.r.svc.node.unset_lazy("devtree")

    def provisioner(self):
        try:
            self.path = self.r.loopFile
            self.size = self.r.svc.conf_get(self.r.rid, "size")
        except Exception as e:
            raise ex.excError(str(e))

        d = os.path.dirname(self.path)
        try:
            if not os.path.exists(d):
                self.r.log.info("create directory %s"%d)
                os.makedirs(d)
            with open(self.path, 'w') as f:
                self.r.log.info("create file %s, size %s"%(self.path, self.size))
                f.seek(convert_size(self.size, _to='b', _round=512)-1)
                f.write('\0')
        except Exception as e:
            raise ex.excError("failed to create %s: %s"% (self.path, str(e)))
        self.r.svc.node.unset_lazy("devtree")
