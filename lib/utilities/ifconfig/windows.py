import foreign.wmi as wmi

from subprocess import *

from .ifconfig import BaseIfconfig, Interface


class Ifconfig(BaseIfconfig):
    def __init__(self, mcast=False):
        self.wmi = wmi.WMI()
        self.intf = []
        self.mcast_data = {}
        for n, nc in zip(self.wmi.Win32_NetworkAdapter(), self.wmi.Win32_NetworkAdapterConfiguration()):
            self.parse(n, nc)

    def parse(self, intf, intf_cf):
        if intf_cf.IPAddress is None:
            return
        i = Interface(intf.NetConnectionID)
        self.intf.append(i)

        # defaults
        i.link_encap = ''
        i.scope = ''
        i.bcast = ''
        i.mask = []
        i.mtu = intf_cf.MTU
        i.ipaddr = []
        i.ip6addr = []
        i.ip6mask = []
        i.hwaddr = intf_cf.MACAddress
        try:
            i.flag_up = intf.NetEnabled
        except:
            i.flag_up = False
        i.flag_broadcast = False
        i.flag_running = False
        i.flag_multicast = False
        i.flag_loopback = False

        for idx, ip in enumerate(intf_cf.IPAddress):
            if ":" in ip:
                i.ip6addr.append(ip)
                i.ip6mask.append(intf_cf.IPsubnet[idx])
            else:
                i.ipaddr.append(ip)
                i.mask.append(intf_cf.IPsubnet[idx])

if __name__ == "__main__" :
    o = Ifconfig()
    print(o)
