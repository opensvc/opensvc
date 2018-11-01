from resData import Data

class Expose(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="expose.envoy", **kwargs)
        self.label = "envoy expose %s/%s" % (self.options.port, self.options.protocol) 

