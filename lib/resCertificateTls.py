from resData import Data

def adder(svc, s):
    rtype = svc.oget(s, "type")
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "tls"))
    r = Certificate(**kwargs)
    svc += r

class Certificate(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="certificate.tls", **kwargs)
        self.label = "tls certificate"
