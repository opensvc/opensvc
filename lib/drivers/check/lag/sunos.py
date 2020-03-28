import drivers.check
import utilities.os.sunos

from utilities.proc import justcall, which

"""
Solaris 10
key: 1 (0x0001) policy: L4      address: 0:15:17:bb:82:d2 (auto)
           device       address                 speed           duplex  link    state
           e1000g0      0:15:17:bb:82:d2          1000  Mbps    full    up      attached
           bnx0         0:24:e8:35:61:3b          1000  Mbps    full    up      attached

Solaris 11
# dladm show-aggr
LINK              POLICY   ADDRPOLICY           LACPACTIVITY  LACPTIMER   FLAGS
aggr0             L4       auto                 off           short       -----
aggrbck0          L4       auto                 off           short       -----
aggrpriv0         L4       auto                 off           short       -----

# dladm show-phys
LINK              MEDIA                STATE      SPEED  DUPLEX    DEVICE
net4              Ethernet             up         10     full      usbecm0
net1              Ethernet             up         1000   full      ixgbe1
net0              Ethernet             up         1000   full      ixgbe0
net2              Ethernet             up         1000   full      ixgbe2
net3              Ethernet             up         1000   full      ixgbe3

# dladm show-link
LINK                CLASS     MTU    STATE    OVER
net4                phys      1500   up       --
net1                phys      1500   up       --
net2                phys      1500   up       --
net0                phys      1500   up       --
aggrbck0            aggr      1500   up       net1
aggrpriv0           aggr      1500   up       net3
bckg0               vnic      1500   up       aggrbck0
zrac1_a0            vnic      1500   up       aggr0
zrac3_p_a0          vnic      1500   up       aggrpriv0
"""

class Check(drivers.check.Check):
    chk_type = "lag"
    chk_name = "Solaris network interface lag"

    def do_check(self):
        if not which("dladm"):
            return self.undef
        self.osver = utilities.os.sunos.get_solaris_version()
        if self.osver >= 11:
            cmd = ['dladm', 'show-phys', '-p', '-o', 'link,state,speed,duplex']
            out, err, ret = justcall(cmd)
            if ret != 0:
                return self.undef
            self.phys = out.split('\n')
            cmd = ['dladm', 'show-aggr', '-p', '-o', 'link']
        else:
            cmd = ['dladm', 'show-aggr']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        if self.osver >= 11:
            self.aggs = out.split('\n')
            if len(self.aggs) == 0:
                return self.undef
            self.listaggs = {}
            cmd = ['dladm', 'show-link', '-p', '-o', 'link,over']
            out, err, ret = justcall(cmd)
            if ret != 0:
                return self.undef
            lines = out.split('\n')
            for line in lines:
                if len(line) == 0:
                    break
                l = line.split(':')
                if l[0] in self.aggs:
                    self.listaggs[l[0]] = l[1]
        else:
            self.lines = out.split('\n')
            if len(self.lines) == 0:
                return self.undef
        r = []
        r += self.do_check_speed()
        r += self.do_check_duplex()
        r += self.do_check_link()
        r += self.do_check_attach()
        return r

    def do_check_speed(self):
        r = []
        lag = ""
        i = 0
        if self.osver >= 11:
            for lag in self.aggs:
                if len(lag) == 0:
                    break
                nets = self.listaggs[lag].split(' ')
                for net in nets:
                    if len(net) == 0:
                        break
                    for phy in self.phys:
                        if phy.startswith(net+':'):
                            l = phy.split(':')
                            val = l[2]
                            r.append({
                                      "instance": '%s.%s.speed'%(lag, net),
                                      "value": str(val),
                                      "path": '',
                                     })
            return r
        for line in self.lines:
            l = line.split()
            if len(l) < 4:
                continue
            elif line.startswith('key'):
                lag = l[1]
                i = 0
                continue
            elif l[0] == 'device':
                continue
            val = l[2]
            r.append({
                      "instance": '%s.%d.speed'%(lag, i),
                      "value": str(val),
                      "path": '',
                     })
            i += 1
        return r

    def do_check_duplex(self):
        return self._do_check("duplex", "full", 4)

    def do_check_link(self):
        return self._do_check("link", "up", 5)

    def do_check_attach(self):
        return self._do_check("attach", "attached", 6)

    def _do_check(self, key, target, col):
        r = []
        lag = ""
        i = 0
        if self.osver >= 11:
            if key == "duplex":
                col = 3
            if key == "attach":
                return r
            if key == "link":
                col = 1
            for lag in self.aggs:
                if len(lag) == 0:
                    break
                nets = self.listaggs[lag].split(' ')
                for net in nets:
                    if len(net) == 0:
                        break
                    for phy in self.phys:
                        if phy.startswith(net+':'):
                            l = phy.split(':')
                            if l[col] != target:
                               val = 1
                            else:
                               val = 0
                            r.append({
                                      "instance": '%s.%s.%s'%(lag, net, key),
                                      "value": str(val),
                                      "path": '',
                                     })
            return r
        for line in self.lines:
            l = line.split()
            if len(l) < col+1:
                continue
            elif line.startswith('key'):
                lag = l[1]
                i = 0
                continue
            elif l[0] == 'device':
                continue
            else:
                if l[col] != target:
                    val = 1
                else:
                    val = 0
                r.append({
                          "instance": '%s.%d.%s'%(lag, i, key),
                          "value": str(val),
                          "path": '',
                         })
                i += 1
        return r
