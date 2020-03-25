import drivers.check
import rcAdvfs

class Check(drivers.check.Check):
    chk_type = "fs_u"

    def __init__(self, svcs=[]):
        checks.check.__init__(self, svcs)

    def find_svc(self, name):
        for svc in self.svcs:
            for resource in svc.get_resources("pool"):
                if not hasattr(resource, "poolname"):
                    continue
                if resource.poolname == name:
                    return svc.path
        return ""

    def do_check(self):
        o = rcAdvfs.Fdmns()
        r = []
        for dom in o.list_fdmns():
            try:
                d = o.get_fdmn(dom)
                r.append({
                          "instance": dom,
                          "value": str(d.used_pct),
                          "path": self.find_svc(dom),
                         })
            except rcAdvfs.ExInit:
                pass
        return r
