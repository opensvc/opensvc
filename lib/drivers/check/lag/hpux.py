import drivers.check

from utilities.proc import justcall, which

class Check(drivers.check.Check):
    chk_type = "lag"

    def do_check(self):
        if not which("lanscan"):
            return []
        cmd = ["lanscan", "-q"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        r = []
        self.lag = {}
        for line in out.split("\n"):
            if len(line) == 0:
                continue
            l = line.split()
            n = len(l)
            if n < 2:
                # not apa
                continue
            if self.has_inet(l[0]):
                self.lag[l[0]] = l[1:]

        cmd = ["lanscan", "-v"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef

        self.intf_status = {}
        for line in out.split("\n"):
            if 'ETHER' not in line or line.startswith('0x'):
                continue
            l = line.split()
            n = len(l)
            if n < 5 or not l[3].startswith('lan'):
                continue
            intf = l[3].replace('lan', '')
            status = l[2]
            self.intf_status[intf] = status

        for intf, slaves in self.lag.items():
            i = 0
            for slave in slaves:
                if slave in self.intf_status and self.intf_status[slave] == 'UP':
                    i += 1
            inst = "lan" + intf + ".paths"
            val = str(i)
            r.append({
                  "instance": inst,
                  "value": val,
                  "path": '',
                 })

        return r

    def has_inet(self, intf):
        cmd = ["ifconfig", "lan"+intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'inet' in out:
            return True
        return False
