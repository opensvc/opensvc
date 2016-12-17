import checks
import glob
import re
import os
from rcUtilities import justcall
import rcAdvfs

class check(checks.check):
    def __init__(self, svcs=[]):
        checks.check.__init__(self, svcs)

    chk_type = "fs_u"

    def find_svc(self, name):
        for svc in self.svcs:
            for resource in svc.get_resources('pool'):
                if resource.poolname == name:
                    return svc.svcname
        return ''

    def do_check(self):
        o = rcAdvfs.Fdmns()
        r = []
        for dom in o.list_fdmns():
            try:
                d = o.get_fdmn(dom)
                r.append({
                          'chk_instance': dom,
                          'chk_value': str(d.used_pct),
                          'chk_svcname': self.find_svc(dom),
                         })
            except rcAdvfs.ExInit:
                pass
        return r
