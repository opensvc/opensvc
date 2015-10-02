from provisioning import Provisioning
import rcExceptions as ex

class ProvisioningVg(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provisioner(self):
        for image in self.r.images:
            self.provisioner_one(image)
        self.remove_keywords(["size", "image_format"])
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def provisioner_one(self, image):
        if self.r.exists(image):
            self.r.log.info("%s already provisioned"%image)
            return
        try:
            size = self.r.svc.config.get(self.r.rid, 'size')
        except:
            raise ex.excError("'size' provisioning parameter not set")
        try:
            image_format = self.r.svc.config.get(self.r.rid, 'image_format')
        except:
            image_format = None

        cmd = self.r.rbd_rcmd() + ['create', '--size', str(size), image]
        if image_format:
            cmd += ["--image-format", str(image_format)]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError


