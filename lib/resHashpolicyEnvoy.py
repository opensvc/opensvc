from resData import Data

class Hashpolicy(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="hash_policy.envoy", **kwargs)
        self.label = "envoy hash policy"

