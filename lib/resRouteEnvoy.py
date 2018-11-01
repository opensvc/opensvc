from resData import Data

class Route(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="route.envoy", **kwargs)
        self.label = "envoy route"
