import copy

from utilities.net.converters import cidr_to_dotted
from env import Env
from utilities.proc import justcall
from core.capabilities import capabilities

from .ifconfig import BaseIfconfig, Interface

"""
ip addr:
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 16436 qdisc noqueue
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
...
4: eth0: <BROADCAST,MULTICAST,SLAVE,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast master bond0 qlen 1000
    link/ether 00:23:7d:a1:6f:96 brd ff:ff:ff:ff:ff:ff
6: sit0: <NOARP> mtu 1480 qdisc noop
    link/sit 0.0.0.0 brd 0.0.0.0
7: bond0: <BROADCAST,MULTICAST,MASTER,UP,LOWER_UP> mtu 1500 qdisc noqueue
    link/ether 00:23:7d:a1:6f:96 brd ff:ff:ff:ff:ff:ff
    inet 10.151.32.29/22 brd 10.151.35.255 scope global bond0
    inet 10.151.32.50/22 brd 10.151.35.255 scope global secondary bond0:1
    inet6 fe80::223:7dff:fea1:6f96/64 scope link
       valid_lft forever preferred_lft forever

"""

class Ifconfig(BaseIfconfig):
    def __init__(self, mcast=False, ip_out=None):
        self.intf = []
        if mcast:
            self.mcast_data = self.get_mcast()
        else:
            self.mcast_data = {}
        if ip_out:
            self.parse_ip(ip_out)
        elif "node.x.ip" in capabilities:
            cmd = [Env.syspaths.ip, 'addr']
            out, _, _ = justcall(cmd)
            self.parse_ip(out)
        elif "node.x.ifconfig" in capabilities:
            cmd = ['ifconfig', '-a']
            out, _, _ = justcall(cmd)
            self.parse_ifconfig(out)

    def parse_ip(self, out):
        for line in out.splitlines():
            if len(line) == 0:
                continue
            if line[0] != " ":
                """
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN
                """
                _line = line.split()
                ifname = _line[1].strip(":")
                if "@" in ifname:
                    ifkname = ifname[ifname.index("@"):]
                    ifname = ifname[:ifname.index("@")]
                else:
                    ifkname = None
                i = Interface(ifname)
                i.ifkname = ifkname

                # defaults
                i.link_encap = ''
                i.scope = []
                i.bcast = []
                i.mask = []
                i.mtu = ''
                i.ipaddr = []
                i.ip6addr = []
                i.ip6mask = []
                i.hwaddr = ''
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False
                i.flag_no_carrier = False

                self.intf.append(i)

                prev = ''
                for w in _line:
                    if 'mtu' == prev:
                        i.mtu = w
                    elif w.startswith('<'):
                        w = w.strip('<').strip('>')
                        flags = w.split(',')
                        for w in flags:
                            if 'UP' == w:
                                i.flag_up = True
                            if 'BROADCAST' == w:
                                i.flag_broadcast = True
                            if 'RUNNING' == w:
                                i.flag_running = True
                            if 'MULTICAST' == w:
                                i.flag_multicast = True
                            if 'LOOPBACK' == w:
                                i.flag_loopback = True
                            if 'NO-CARRIER' == w:
                                i.flag_no_carrier = True

                    prev = w
            elif line.strip().startswith("link"):
                """
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
                """
                _line = line.split()
                prev = ''
                for w in _line:
                    if 'link/' in w:
                        i.link_encap = w.split('/')[1]
                    elif 'link/ether' == prev:
                        i.hwaddr = w
                    prev = w
            elif line.strip().startswith("inet"):
                """
    inet 127.0.0.1/8 scope host lo
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
                """
                _line = line.split()
                if "global" in line and ":" in _line[-1]:
                    # clone parent intf and reset inet fields
                    ifname = line.split()[-1]
                    _i = copy.copy(i)
                    _i.name = ifname
                    _i.scope = []
                    _i.bcast = []
                    _i.mask = []
                    _i.ipaddr = []
                    _i.ip6addr = []
                    _i.ip6mask = []
                    self.intf.append(_i)
                else:
                    _i = i

                prev = ''
                for w in _line:
                    if 'inet' == prev :
                        try:
                            ipaddr, mask = w.split('/')
                        except:
                            # tun for example
                            continue
                        _i.ipaddr += [ipaddr]
                        _i.mask += [cidr_to_dotted(mask)]
                    elif 'inet6' == prev:
                        try:
                            ip6addr, ip6mask = w.split('/')
                        except:
                            # tun for example
                            continue
                        _i.ip6addr += [ip6addr]
                        _i.ip6mask += [ip6mask]
                    elif 'brd' == prev and 'inet' in line:
                        _i.bcast += [w]
                    elif 'scope' == prev and 'inet' in line:
                        _i.scope += [w]

                    prev = w


    def parse_ifconfig(self, out):
        prev = ''
        prevprev = ''
        for w in out.split():
            if w == 'Link':
                i = Interface(prev)
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
                i.flag_up = False
                i.flag_broadcast = False
                i.flag_running = False
                i.flag_multicast = False
                i.flag_loopback = False
                i.flag_no_carrier = False
            elif 'encap:' in w:
                (null, i.link_encap) = w.split(':')
            elif 'Scope:' in w:
                (null, i.scope) = w.split(':')
            elif 'Bcast:' in w:
                (null, i.bcast) = w.split(':')
            elif 'Mask:' in w:
                (null, i.mask) = w.split(':')
            elif 'MTU:' in w:
                (null, i.mtu) = w.split(':')

            if 'inet' == prev and 'addr:' in w:
                (null, i.ipaddr) = w.split(':')
            if 'inet6' == prevprev and 'addr:' == prev:
                (ip6addr, ip6mask) = w.split('/')
                i.ip6addr += [ip6addr]
                i.ip6mask += [ip6mask]
            if 'HWaddr' == prev:
                i.hwaddr = w
            if 'UP' == w:
                i.flag_up = True
            if 'BROADCAST' == w:
                i.flag_broadcast = True
            if 'RUNNING' == w:
                i.flag_running = True
            if 'MULTICAST' == w:
                i.flag_multicast = True
            if 'LOOPBACK' == w:
                i.flag_loopback = True
            if 'NO-CARRIER' == w:
                i.flag_no_carrier = True

            prevprev = prev
            prev = w

    def get_mcast(self):
        if "node.x.ip" in capabilities:
            cmd = [Env.syspaths.ip, 'maddr']
            out, _, _ = justcall(cmd)
            return self.parse_mcast_ip(out)
        if "node.x.netstat" in capabilities:
            cmd = ["netstat", "-gn"]
            out, _, _ = justcall(cmd)
            return self.parse_mcast_netstat(out)

    def parse_mcast_netstat(self, out):
        lines = out.splitlines()
        found = False
        data = {}
        for i, line in enumerate(lines):
            if line.startswith('--'):
                found = True
                break
        if not found:
            return data
        if len(lines) == i+1:
            return data
        lines = lines[i+1:]
        for line in lines:
            try:
                intf, refcnt, addr = line.split()
            except:
                continue
            if intf not in data:
                data[intf] = [addr]
            else:
                data[intf] += [addr]
        return data

    def parse_mcast_ip(self, out):
        lines = out.splitlines()
        found = False
        data = {}
        for line in lines:
            if not line.startswith("	"):
                # new interface
                try:
                    name = line.split(":")[-1].strip()
                except Exception as e:
                    print(e)
                    break
                if name == "":
                    continue
                data[name] = []
                continue
            if "inet" not in line:
                continue
            data[name].append(line.split()[-1])
        return data

if __name__ == "__main__":
    ifaces = Ifconfig(mcast=True)
    print(ifaces)

