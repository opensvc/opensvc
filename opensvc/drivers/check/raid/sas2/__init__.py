import os

import drivers.check

from env import Env
from utilities.proc import justcall, which

class Check(drivers.check.Check):
    prefixes = [os.path.join(os.sep, "usr", "local", "admin")]
    sas2ircu = "sas2ircu"
    chk_type = "raid"
    chk_name = "LSI SAS200"

    def find_sas2ircu(self):
        if which(self.sas2ircu):
            return self.sas2ircu
        for prefix in self.prefixes:
            sas2ircu = os.path.join(prefix, self.sas2ircu)
            if os.path.exists(sas2ircu):
                return sas2ircu
        return

    def do_check(self):
        r = self.do_check_ldpdinfo()
        return r

    def do_check_ldpdinfo(self):
        sas2ircu = self.find_sas2ircu()
        if sas2ircu is None:
            return self.undef
        os.chdir(Env.paths.pathtmp)
        logs = [os.path.join(Env.paths.pathtmp, 'sas2ircu.log')]
        for log in logs:
            if not os.path.exists(log):
                continue
            os.unlink(log)
        cmd = [sas2ircu, 'LIST']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        idx = []
        lines = out.split('\n')
        for line in lines:
            if 'SAS20' in line:
                l = line.split()
                idx.append(l[0])

        r = []
        errs = 0
        for ix in idx:
            cmd = [sas2ircu, str(ix), 'DISPLAY']
            out, err, ret = justcall(cmd)
            lines = out.split('\n')
            ctrl = "ctrl:"+str(ix)
            slot=""
            chk_dsk = 0
            for line in lines:
                if line.startswith('IR volume'):
                    chk_dsk = 2
                if line.startswith('  Volume Name') and 'Virtual Disk' in line and (chk_dsk == 2):
                    l = line.split()
                    slot = 'LD'+str(l[-1])
                if line.startswith('  Status of volume') and (chk_dsk == 2):
                    if 'Okay (OKY)' not in line:
                        r.append({ "instance": ctrl+','+slot, "value": '1', "path": '', })
                        errs += 1
                    else :
                        r.append({ "instance": ctrl+','+slot, "value": '0', "path": '', })
                if line.startswith('Device is a Hard disk'):
                    chk_dsk = 1
                if line.startswith('  Enclosure #') and (chk_dsk == 1):
                    l = line.split()
                    enc = l[-1]
                if line.startswith('  Slot #') and (chk_dsk == 1):
                    l = line.split()
                    slot = 'PD'+str(enc)+':'+str(l[-1])
                if line.startswith('  State') and (chk_dsk == 1):
                    if 'Optimal (OPT)' not in line:
                        r.append({ "instance": ctrl+','+slot, "value": '1', "path": '', })
                        errs += 1
                    else :
                        r.append({ "instance": ctrl+','+slot, "value": '0', "path": '', })
                if line.startswith('Device is a Enclosure services device'):
                    chk_dsk = 3
                if line.startswith('  Enclosure #') and (chk_dsk == 3):
                    l = line.split()
                    slot = 'Enc'+str(l[-1])
                if line.startswith('  State') and (chk_dsk == 3):
                    if 'Standby (SBY)' not in line:
                        r.append({ "instance": ctrl+','+slot, "value": '1', "path": '', })
                        errs += 1
                    else :
                        r.append({ "instance": ctrl+','+slot, "value": '0', "path": '', })
            r.append({ "instance": 'all SAS20*', "value": str(errs), "path": '', })
        return r
