import drivers.check
import utilities.subsystems.advfs

class Check(drivers.check.Check):
    chk_type = "fs_u"

    def __init__(self, svcs=None):
        super(Check, self).__init__(self, svcs)
        if svcs is None:
            svcs = []

    def find_svc(self, name):
        for svc in self.svcs:
            for resource in svc.get_resources("pool"):
                if not hasattr(resource, "poolname"):
                    continue
                if resource.poolname == name:
                    return svc.path
        return ""

    def do_check(self):
        o = utilities.subsystems.advfs.Fdmns()
        r = []
        for dom in o.list_fdmns():
            try:
                d = o.get_fdmn(dom)
                r.append({
                          "instance": dom,
                          "value": str(d.used_pct),
                          "path": self.find_svc(dom),
                         })
            except utilities.subsystems.advfs.ExInit:
                pass
        return r
