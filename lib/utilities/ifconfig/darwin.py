from subprocess import *

from utilities.proc import which
from .ifconfig import BaseIfconfig, Interface

class Ifconfig(BaseIfconfig):
    def __init__(self, mcast=False):
        self.intf = []
        if mcast:
            self.mcast_data = self.get_mcast()
        else:
            self.mcast_data = {}
        out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]
        self.parse(out)

    def parse(self, out):
        prev = ''
        prevprev = ''
        for w in out.split():
            if 'flags=' in w:
                i = Interface(prev.replace(':',''))
                self.intf.append(i)

                # defaults
                i.link_encap = ''
                i.scope = ''
                i.bcast = ''
                i.mtu = ''
                i.ipaddr = []
                i.mask = []
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False

                flags = w.split('<')[1].split('>')[0].split(',')
                if 'UP' in flags:
                    i.flag_up = True
                if 'BROADCAST' in flags:
                    i.flag_broadcast = True
                if 'RUNNING' in flags:
                    i.flag_running = True
                if 'MULTICAST' in flags:
                    i.flag_multicast = True
                if 'LOOPBACK' in flags:
                    i.flag_loopback = True
            elif 'inet' == prev:
                i.ipaddr += [w]
            elif 'inet6' == prev:
                i.ip6addr += [w.split('%')[0]]
            elif 'netmask' == prev:
                i.mask += [w]
            elif 'prefixlen' == prev:
                i.ip6mask += [w]
            elif 'ether' == prev:
                i.hwaddr = w

            prevprev = prev
            prev = w

    def get_mcast(self):
        if which('netstat'):
            cmd = ['netstat', '-gn']
            out = Popen(cmd, stdout=PIPE).communicate()[0]
            return self.parse_mcast_netstat(out)

    def parse_mcast_netstat(self, out):
        lines = out.split('\n')
        found = False
        data = {}
        for i, line in enumerate(lines):
            if line.startswith('IPv4 Multicast'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+2:]
        for line in lines:
            if line.startswith('IPv6 Multicast') or line.startswith('Group'):
                continue
            try:
                addr, lladdr, intf = line.split()
            except:
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
        return data


