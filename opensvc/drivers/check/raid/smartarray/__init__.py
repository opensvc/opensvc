import os

import drivers.check

from utilities.proc import justcall, which

sep = ':'
path_list = os.environ['PATH'].split(sep) + ['/opt/HPQacucli/sbin']

os.environ['PATH'] = sep.join(path_list)
os.environ['INFOMGR_BYPASS_NONSA'] = '1'

class Check(drivers.check.Check):
    chk_type = "raid"
    chk_name = "HP SmartArray"

    def parse_errors(self, out):
        r = []
        lines = out.split('\n')
        if len(lines) == 0:
            return r
        for line in lines:
            l = line.split(': ')
            if len(l) < 2 or not line.startswith('  '):
                continue
            if l[-1].strip() != "OK":
                inst = line.strip().lower()
                status = 1
            else:
                inst = l[0].strip().lower()
                status = 0
            r += [(inst, status)]
        return r

    def check_logicaldrive(self, slot):
        cmd = ['controller', 'slot='+slot, 'logicaldrive', 'all', 'show', 'status']
        out, err, ret = self.hpacucli(cmd)
        if ret != 0:
            return []
        return self.parse_errors(out)

    def check_physicaldrive(self, slot):
        cmd = ['controller', 'slot='+slot, 'physicaldrive', 'all', 'show', 'status']
        out, err, ret = self.hpacucli(cmd)
        if ret != 0:
            return []
        return self.parse_errors(out)

    def check_array(self, slot):
        cmd = ['controller', 'slot='+slot, 'array', 'all', 'show', 'status']
        out, err, ret = self.hpacucli(cmd)
        if ret != 0:
            return []
        return self.parse_errors(out)

    def check_controller(self, slot):
        cmd = ['controller', 'slot='+slot, 'show', 'status']
        out, err, ret = self.hpacucli(cmd)
        if ret != 0:
            return []
        return self.parse_errors(out)

    def hpacucli(self, cmd):
        cmd = ['hpacucli'] + cmd
        try:
            out, err, ret = justcall(cmd)
        except OSError:
            cmd = [os.environ['SHELL']] + cmd
            out, err, ret = justcall(cmd)
        return out, err, ret

    def do_check(self):
        if not which('hpacucli'):
            return self.undef
        cmd = ['controller', 'all', 'show', 'status']
        out, err, ret = self.hpacucli(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        for line in lines:
            if ' Slot ' in line:
                l = line.split()
                idx = l.index('Slot')
                uslot = l[idx+1]
                slot = 'slot ' + uslot
                _r = []
                _r += self.check_controller(uslot)
                _r += self.check_array(uslot)
                _r += self.check_logicaldrive(uslot)
                _r += self.check_physicaldrive(uslot)
                for inst, value in _r:
                    r.append({
                          "instance": ".".join((slot, inst)),
                          "value": str(value),
                          "path": '',
                         })
        return r
