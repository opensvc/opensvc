from subprocess import *
from rcUtilities import hexmask_to_str

import rcIfconfig

class ifconfig(rcIfconfig.ifconfig):
    def get_mac(self, intf):
        buff = self.get_netstat_in()
        for line in buff.split("\n"):
            l = line.split()
            if len(l) < 4:
                continue
            if l[0] != intf.name:
                continue
            if not l[2].startswith("link"):
                continue
            if '.' not in l[3]:
                return ""
            return l[3].replace('.', ':')
        return ""

    def get_netstat_in(self):
        if hasattr(self, "netstat_in_cache"):
            return self.netstat_in_cache
        cmd = ['netstat', '-in']
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return ""
        self.netstat_in_cache = out
        return out

    def parse(self, out):
        prev = ''
        prevprev = ''
        for w in out.split():
            if 'flags=' in w:
                i = rcIfconfig.interface(prev.replace(':',''))
                i.hwaddr = self.get_mac(i)
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
            elif 'netmask' == prev:
                i.mask += [hexmask_to_str(w)]
            elif 'inet6' == prev:
                i.ip6addr += [w.split('/')[0]]
                i.ip6mask += [w.split('/')[1]]
            elif 'ether' == prev:
                i.hwaddr = w

            prevprev = prev
            prev = w

    def __init__(self, mcast=False):
        rcIfconfig.ifconfig.__init__(self)
        self.intf = []
        out = Popen(['ifconfig', '-a'], stdout=PIPE).communicate()[0]
        self.parse(out)

if __name__ == "__main__":
    ifaces = ifconfig(mcast=True)
    print(ifaces)

