class Provisioning(object):
    def __init__(self, r):
        self.r = r

    def validate(self):
        return True

    def provisioner(self):
        return True

    def remove_keywords(self, keywords=[]):
        for kw in keywords:
            self.remove_keyword(kw, write=False)
        self.r.svc.write_config()

    def remove_keyword(self, keyword, write=True):
        for o in self.r.svc.config.options(self.r.rid):
            if o != keyword and not o.startswith(keyword+"@"):
                continue
            self.r.log.info("remove provisioning keyword: %s" % o)
            self.r.svc.config.remove_option(self.r.rid, o)

        if write:
            self.r.svc.write_config()
