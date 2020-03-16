from resData import Data

def adder(svc, s):
    kwargs = {"rid": s}
    kwargs.update(svc.section_kwargs(s, "envoy"))
    r = Expose(**kwargs)
    svc += r

class Expose(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="expose.envoy", **kwargs)
        self.label = "envoy expose %s/%s via %s:%d" % (
            self.options.port,
            self.options.protocol,
            self.options.listener_addr if self.options.listener_addr else "0.0.0.0",
            self.options.listener_port
        ) 

