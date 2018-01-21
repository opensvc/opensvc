import provisioning
import rcExceptions as ex
from converters import convert_size

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def provisioner(self):
        for image in self.r.images:
            self.provisioner_one(image)
        self.r.log.info("provisioned")
        self.r.start()
        self.r.svc.node.unset_lazy("devtree")

    def provisioner_one(self, image):
        if self.r.exists(image):
            self.r.log.info("%s already provisioned"%image)
            return
        size = self.r.conf_get('size')
        size = convert_size(size, _to="m")

        try:
            image_format = self.r.conf_get('image_format')
        except ex.OptNotFound as exc:
            image_format = exc.default

        cmd = self.r.rbd_rcmd() + ['create', '--size', str(size), image]
        if image_format:
            cmd += ["--image-format", str(image_format)]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.svc.node.unset_lazy("devtree")

