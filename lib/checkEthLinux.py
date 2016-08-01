import checks
import os
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import glob
import rcEthtool

class check(checks.check):
    chk_type = "eth"
    bonding_p = '/proc/net/bonding'

    def get_intf(self):
        intf = []
        l = glob.glob(self.bonding_p+'/*')
        for bond in l:
            intf += self.add_slaves(bond)
        rcIfconfig = __import__("rcIfconfig"+rcEnv.sysname)
        ifconfig = rcIfconfig.ifconfig()
        for i in ifconfig.intf:
            if not i.name.startswith('eth'):
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
            ethtool = rcEthtool.Ethtool(intf)
            ethtool.load()
        except rcEthtool.LoadError:
            return []

        inst = intf + ".speed"
        val = ethtool.speed
        if val is not None:
            val.replace('Mb/s', '')
            r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".autoneg"
        if val is not None:
            val = ethtool.auto_negotiation
            if val == 'on':
                val = '1'
            else:
                val = '0'
            r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".duplex"
        val = ethtool.duplex
        if val is not None:
            if val == 'Full':
                val = '1'
            else:
                val = '0'
            r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        inst = intf + ".link"
        val = ethtool.link_detected
        if val is not None:
            if val == 'yes':
                val = '1'
            else:
                val = '0'
            r.append({
                  'chk_instance': inst,
                  'chk_value': val,
                  'chk_svcname': '',
                 })

        return r
