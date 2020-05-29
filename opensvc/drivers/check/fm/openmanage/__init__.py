import os

import drivers.check

from env import Env
from utilities.proc import justcall, which

class Check(drivers.check.Check):
    omreport = "/opt/dell/srvadmin/bin/omreport"
    chk_type = "om"
    chk_name = "OpenManage"

    def find_omreport(self):
        if which(self.omreport):
            return self.omreport
        return

    def do_check(self):
        r = self.do_check_system()
        r += self.do_check_chassis()
        return r

    def do_check_chassis(self):
        return self.do_check_gen("chassis")

    def do_check_system(self):
        return self.do_check_gen("system")

    def do_check_gen(self, command):
        omreport = self.find_omreport()
        if omreport is None:
            return self.undef
        cmd = [omreport, command]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        for line in lines:
            l = line.split(" : ")
            if len(l) != 2:
                continue
            inst = l[1].strip().lower()
            state = l[0].strip().lower()
            if state == "severity":
                continue
            elif state == "ok":
                state = 0
            else:
                state = 1
            r.append({
                      "instance": inst,
                      "value": str(state),
                      "path": '',
                     })
        return r
