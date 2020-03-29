import drivers.check

from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "eth"

    def do_check(self):
        cmd = ["lanscan", "-q"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        r = []
        intf = set()
        for line in out.split("\n"):
            if len(line) == 0:
                continue
            l = line.split()
            n = len(l)
            if n == 1:
                # add interfaces with an inet config
                if self.has_inet(l[0]):
                    intf.add(l[0])
            elif n > 1:
                # add slaves for apa with an inet config
                if self.has_inet(l[0]):
                    for w in l[1:]:
                        intf.add(w)
            else:
                continue

        for i in intf:
            r += self.do_check_intf(i)

        return r

    def has_inet(self, intf):
        cmd = ["ifconfig", "lan"+intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        if 'inet' in out:
            return True
        return False

    def do_check_intf(self, intf):
        r = []
        cmd = ["lanadmin", "-x", intf]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []

        intf = "lan"+intf
        inst = intf + ".link"
        if "link is down" in out:
            val = "0"
        else:
            val = "1"
        r.append({
                  "instance": inst,
                  "value": val,
                  "path": "",
                 })

        inst = intf + ".speed"
        val = "0"
        for line in out.split('\n'):
            if "Speed" not in line:
                continue
            try:
                val = line.split()[2]
            except:
                pass
        r.append({
                  "instance": inst,
                  "value": val,
                  "path": "",
                 })

        inst = intf + ".autoneg"
        val = "0"
        for line in out.split('\n'):
            if "Autoneg" not in line:
                continue
            if " On":
                val = "1"
        r.append({
                  "instance": inst,
                  "value": val,
                  "path": "",
                 })

        inst = intf + ".duplex"
        val = '0'
        for line in out.split('\n'):
            if "Speed" not in line:
                continue
            if 'Full-Duplex' in line:
                val = "1"
        r.append({
                  "instance": inst,
                  "value": val,
                  "path": "",
                 })

        return r
