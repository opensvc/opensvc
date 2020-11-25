import glob

import drivers.check
import utilities.ifconfig
import utilities.subsystems.ethtool


class Check(drivers.check.Check):
    chk_type = "eth"
    bonding_p = '/proc/net/bonding'

    def get_intf(self):
        intf = []
        l = glob.glob(self.bonding_p+'/*')
        for bond in l:
            intf += self.add_slaves(bond)
        ifconfig = utilities.ifconfig.Ifconfig()
        for i in ifconfig.intf:
            if not i.name.startswith("eth") and \
               not i.name.startswith("en"):
                continue
            if len(i.ipaddr) == 0 and len(i.ip6addr) == 0:
                continue
            if i.name in intf:
                continue
            intf.append(i.name)
        return intf

    def add_slaves(self, bond):
        intf = []
        try:
            f = open(bond, 'r')
            buff = f.read()
            f.close()
        except:
            return intf
        for line in buff.split('\n'):
            if line.startswith('Slave Interface:'):
                intf.append(line.split()[-1])
        return intf

    def do_check(self):
        l = self.get_intf()
        if len(l) == 0:
            return self.undef
        r = []
        for intf in l:
            r += self.do_check_intf(intf)
        return r

    def do_check_intf(self, intf):
        r = []
        try:
            ethtool = utilities.subsystems.ethtool.Ethtool(intf)
            ethtool.load()
        except utilities.subsystems.ethtool.LoadError:
            return []

        inst = intf + ".speed"
        val = ethtool.speed
        # discard unknown (kvm)
        if val is not None and "Unknown" not in val:
            val.replace('Mb/s', '')
            r.append({
                  "instance": inst,
                  "value": val,
                  "path": '',
                 })

        inst = intf + ".autoneg"
        # discard unknown (kvm)
        if val is not None and "Unknown" not in val:
            val = ethtool.auto_negotiation
            if val == 'on':
                val = '1'
            else:
                val = '0'
            r.append({
                  "instance": inst,
                  "value": val,
                  "path": '',
                 })

        inst = intf + ".duplex"
        val = ethtool.duplex
        # discard unknown (kvm)
        if val is not None and "Unknown" not in val:
            if val == 'Full':
                val = '1'
            else:
                val = '0'
            r.append({
                  "instance": inst,
                  "value": val,
                  "path": '',
                 })

        inst = intf + ".link"
        val = ethtool.link_detected
        if val is not None:
            if val == 'yes':
                val = '1'
            else:
                val = '0'
            r.append({
                  "instance": inst,
                  "value": val,
                  "path": '',
                 })

        return r
