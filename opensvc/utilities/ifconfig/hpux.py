from subprocess import *

import core.exceptions as ex

from .ifconfig import BaseIfconfig, Interface

class Ifconfig(BaseIfconfig):
    def __init__(self, hwaddr=False, mcast=False):
        self.intf = []
        intf_list = []
        self.hwaddr = {}
        if mcast:
            self.mcast_data = self.get_mcast()
        else:
            self.mcast_data = {}
        if hwaddr:
            lines = Popen(['lanscan', '-i', '-a'], stdout=PIPE).communicate()[0].split('\n')
            for line in lines:
                l = line.split()
                if len(l) < 2:
                    continue
                mac = l[0].replace('0x','').lower()
                if len(mac) < 11:
                    continue
                mac_l = list(mac)
                for c in (10, 8, 6, 4, 2):
                    mac_l.insert(c, ':')
                self.hwaddr[l[1]] = ''.join(mac_l)
        out = Popen(['netstat', '-win'], stdout=PIPE).communicate()[0]
        for line in out.split('\n'):
            if len(line) == 0:
                continue
            if 'IPv4:' in line or 'IPv6' in line:
                continue
            intf = line.split()[0]
            intf_list.append(intf.replace('*', ''))
        for intf in intf_list:
            p = Popen(['ifconfig', intf], stdout=PIPE, stderr=PIPE)
            out = p.communicate()
            if "no such interface" in out[1]:
                continue
            elif p.returncode != 0:
                raise ex.Error
            self.parse(out[0])

    def parse(self, out):
        if len(out) == 0:
            return
        intf = out.split()[0]
        if intf[len(intf)-1] == ':':
            intf = intf[0:len(intf)-1]

        i = Interface(intf)
        self.intf.append(i)

        # defaults
        i.link_encap = ''
        i.scope = ''
        i.bcast = ''
        i.mask = ''
        i.mtu = ''
        i.ipaddr = ''
        i.ip6addr = []
        i.ip6mask = []
        i.hwaddr = ''
        if i.name in self.hwaddr:
            i.hwaddr = self.hwaddr[i.name]
        i.flag_up = False
        i.flag_broadcast = False
        i.flag_running = False
        i.flag_multicast = False
        i.flag_loopback = False

        prev = ''
        for w in out.split():
            if 'broadcast' in prev:
                i.bcast = w
            elif 'netmask' in prev:
                if w == '0':
                    i.mask = "0.0.0.0"
                elif len(w) == 8:
                    i.mask = "%d.%d.%d.%d"%(
                        int(w[0:2], 16),
                        int(w[2:4], 16),
                        int(w[4:6], 16),
                        int(w[6:8], 16)
                    )
                else:
                    raise ex.Error("malformed ifconfig %s netmask: %s"%(intf, w))
            elif 'inet' == prev:
                i.ipaddr = w
            elif 'inet6' == prev:
                i.ip6addr += [w]
            elif 'prefix' == prev:
                i.ip6mask += [w]

            if 'UP' in w:
                i.flag_up = True
            if 'BROADCAST' in w:
                i.flag_broadcast = True
            if 'RUNNING' in w:
                i.flag_running = True
            if 'MULTICAST' in w:
                i.flag_multicast = True
            if 'LOOPBACK' in w:
                i.flag_loopback = True

            prev = w

    def get_mcast(self):
        cmd = ['netstat', '-gn']
        out = Popen(cmd, stdout=PIPE).communicate()[0]
        return self.parse_mcast(out)

    def parse_mcast(self, out):
        lines = out.split('\n')
        found = False
        data = {}
        for i, line in enumerate(lines):
            if line.startswith('Name'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
            if len(line) == 0:
                break
            try:
                intf, addr, refcnt = line.split()
            except:
                continue
            if addr == "*":
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
        if len(lines) <= i + 1:
            return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
            if line.startswith('Name'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+1:]
        for i, line in enumerate(lines):
            if len(line) == 0:
                break
            try:
                intf, addr, refcnt = line.split()
            except:
                continue
            if addr == "*":
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
        return data
