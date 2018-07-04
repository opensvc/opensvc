import provisioning

class Prov(provisioning.Prov):
    def provision(self):
        c = self.r.get_cloud()
        image = self.r.get_template()
        size = self.r.get_size()
        self.r.log.info("create instance %s, size %s, image %s, key %s"%(self.r.name, size.name, image.name, self.r.key_name))
        c.driver.create_node(name=self.r.name, size=size, image=image, ex_keyname=self.r.key_name, ex_shared_ip_group_id=self.r.shared_ip_group)
        #self.r.wait_for_startup()
        self.r.wait_for_fn(self.r.is_up, self.r.start_timeout, 5)

