class Prov(object):
    def __init__(self, r):
        self.r = r

    def start(self):
        self.r.start()

    def stop(self):
        self.r.stop()

    def is_provisioned(self):
        return

    def unprovisioner(self):
        pass

    def provisioner(self):
        pass

    def remove_keywords(self, keywords=[]):
        for kw in keywords:
            self.remove_keyword(kw, write=False)
        self.r.svc.write_config()

    def remove_keyword(self, keyword, write=True):
        for o in self.r.svc.config.options(self.r.rid):
            if o != keyword and not o.startswith(keyword+"@"):
                continue
            self.r.log.info("comment out provisioning keyword: %s" % o)
            val = self.r.svc.config.get(self.r.rid, o)
            self.r.svc.config.remove_option(self.r.rid, o)
            self.r.svc.config.set(self.r.rid, "#"+o, val)

        if write:
            self.r.svc.write_config()
