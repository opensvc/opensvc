import provisioning
import rcExceptions as ex

class Prov(provisioning.Prov):
    def provisioner(self):
        prereq = True
        if self.r.image_id is None:
            self.r.log.error("the image keyword is mandatory for the provision action")
            prereq &= False
        if self.r.size_id is None:
            self.r.log.error("the size keyword is mandatory for the provision action")
            prereq &= False
        if self.r.subnet_name is None:
            self.r.log.error("the subnet keyword is mandatory for the provision action")
            prereq &= False
        if self.r.key_name is None:
            self.r.log.error("the key_name keyword is mandatory for the provision action")
            prereq &= False
        if not prereq:
            raise ex.excError()

        c = self.r.get_cloud()
        image = self.r.get_image(self.r.image_id)
        size = self.r.get_size()
        subnet = self.r.get_subnet()
        self.r.log.info("create instance %s, size %s, image %s, key %s, subnet %s"%(self.r.name, size.name, image.name, self.r.key_name, subnet.name))
        c.driver.create_node(name=self.r.name, size=size, image=image, ex_keyname=self.r.key_name, ex_subnet=subnet)
        self.r.log.info("wait for container up status")
        self.r.wait_for_fn(self.r.is_up, self.r.startup_timeout, 5)

