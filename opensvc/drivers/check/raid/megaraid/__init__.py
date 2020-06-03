import os

import drivers.check

from env import Env
from utilities.proc import justcall, which

class Check(drivers.check.Check):
    prefixes = [os.path.join(os.sep, "usr", "local", "admin"),
                os.path.join(os.sep, "opt", "MegaRAID", "MegaCli")]
    megacli = ["MegaCli64", "MegaCli", "megacli"]
    chk_type = "raid"
    chk_name = "MegaCli"

    def find_megacli(self):
        for prog in self.megacli:
            if which(prog):
                return prog
            for prefix in self.prefixes:
                megacli = os.path.join(prefix, prog)
                if os.path.exists(megacli):
                    return megacli
        return

    def do_check(self):
        r = self.do_check_ldpdinfo()
        r += self.do_check_bbustatus()
        return r

    def do_check_ldpdinfo(self):
        megacli = self.find_megacli()
        if megacli is None:
            return self.undef
        os.chdir(Env.paths.pathtmp)
        logs = [os.path.join(Env.paths.pathtmp, 'MegaSAS.log'),
                os.path.join(Env.paths.pathtmp, 'MegaCli.log'),
                os.path.join(Env.paths.pathtmp, 'MegaRaid.log')]
        for log in logs:
            if not os.path.exists(log):
                continue
            os.unlink(log)
        cmd = [megacli, '-LdPdInfo', '-aALL']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        slot = ""
        errs = 0
        for line in lines:
            if line.startswith('Adapter'):
                l = line.split('#')
                if slot != "":
                    r.append({
                             "instance": slot,
                              "value": str(errs),
                              "path": '',
                             })
                slot = 'slot'+l[-1]
                errs = 0
            if (line.startswith('State:') and 'Optimal' not in line) or \
               (line.startswith('Firmware state:') and 'Online' not in line):
                errs += 1
        if slot != "":
            r.append({
                 "instance": slot,
                  "value": str(errs),
                  "path": '',
                 })
        return r

    def do_check_bbustatus(self):
        megacli = self.find_megacli()
        if megacli is None:
            return self.undef
        os.chdir(Env.paths.pathtmp)
        logs = [os.path.join(Env.paths.pathtmp, 'MegaSAS.log'),
                os.path.join(Env.paths.pathtmp, 'MegaCli.log'),
                os.path.join(Env.paths.pathtmp, 'MegaRaid.log')]
        for log in logs:
            if not os.path.exists(log):
                continue
            os.unlink(log)
        cmd = [megacli, '-AdpBbuCmd', '-GetBbuStatus', '-aALL']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        slot = ""
        for line in lines:
            line = line.strip()
            if 'Adapter:' in line:
                l = line.split()
                slot = 'slot'+l[-1]
            if line.startswith('BatteryType:') and 'No Battery' in line:
                val = 1
                r.append({
                          "instance": '%s battery NoBattery'%slot,
                          "value": str(val),
                          "path": '',
                         })
            if line.startswith('Relative State of Charge:'):
                val = line.strip('%').split()[-1]
                r.append({
                          "instance": '%s battery charge'%slot,
                          "value": str(val),
                          "path": '',
                         })
            if line.startswith('Temperature:'):
                val = line.split()[-2]
                r.append({
                          "instance": '%s battery temp'%slot,
                          "value": str(val),
                          "path": '',
                         })
            if line.startswith('isSOHGood:'):
                if 'Yes' in line:
                    val = 0
                else:
                    val = 1
                r.append({
                          "instance": '%s battery isSOHGood'%slot,
                          "value": str(val),
                          "path": '',
                         })
        return r
