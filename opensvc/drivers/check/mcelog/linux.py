import datetime
import os

import drivers.check

from env import Env
from utilities.proc import which

class Check(drivers.check.Check):
    chk_type = "mcelog"
    mcelog_p = "/var/log/mcelog"
    marker_p = os.path.join(Env.paths.pathtmp, "checkMceLinunx.marker")

    def get_last_marker(self):
        try:
            f = open(self.marker_p, 'r')
        except:
            return None
        buff = f.read()
        f.close()
        return buff

    def gen_marker(self):
        return "opensvc marker " + str(datetime.datetime.now())+'\n'

    def update_marker(self):
        m = self.gen_marker()
        try:
            f = open(self.mcelog_p, "a")
            f.write(m)
            f.close()
        except:
            return
        try:
            f = open(self.marker_p, "w")
            f.write(m)
            f.close()
        except:
            return

    def do_check(self):
        if not os.path.exists(self.mcelog_p):
            return self.undef

        if not which("mcelog"):
            return self.undef

        try:
            f = open(self.mcelog_p, "r")
        except:
            return self.undef

        marker = self.get_last_marker()
        marker_found = False
        l = 0
        total = 0

        for line in f.readlines():
            total += 1
            if line == marker:
                marker_found = True
                continue
            if not marker_found:
                continue
            l += 1

        if not marker_found:
            l = total

        r = []
        r.append({
                  "instance": "new lines",
                  "value": str(l),
                  "path": "",
                 })

        self.update_marker()
        return r
