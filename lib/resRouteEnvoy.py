from resData import Data

def adder(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    r = Route(**kwargs)
    svc += r

class Route(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="route.envoy", **kwargs)
        self.label = "envoy route"
