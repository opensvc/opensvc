from resData import Data

class Vhost(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="vhost.envoy", **kwargs)
        if self.options.domains:
            self.label = "envoy vhost %s" % ", ".join(self.options.domains)
        else:
            self.label = "envoy vhost"

