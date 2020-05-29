class Interface:
    def __str__(self):
        a = ['ifconfig %s:'%self.name]
        a += [' link_encap = ' + str(self.link_encap)]
        a += [' scope = ' + str(self.scope)]
        a += [' bcast = ' + str(self.bcast)]
        a += [' mtu = ' + str(self.mtu)]
        a += [' ipaddr = ' + str(self.ipaddr)]
        a += [' mask = ' + str(self.mask)]
        a += [' ip6addr = ' + str(self.ip6addr)]
        a += [' ip6mask = ' + str(self.ip6mask)]
        a += [' hwaddr = ' + self.hwaddr]
        a += [' flag_up = ' + str(self.flag_up)]
        a += [' flag_deprecated = ' + str(self.flag_deprecated)]
        a += [' flag_broadcast = ' + str(self.flag_broadcast)]
        a += [' flag_running = ' + str(self.flag_running)]
        a += [' flag_multicast = ' + str(self.flag_multicast)]
        a += [' flag_loopback = ' + str(self.flag_loopback)]
        a += [' flag_no_carrier = ' + str(self.flag_no_carrier)]
        if self.groupname:
            a += [' groupname = ' + str(self.groupname)]
        return '\n'.join(a)

    def __init__(self, name):
        self.name = name
        # defaults
        self.groupname = ''
        self.link_encap = ''
        self.scope = ''
        self.bcast = ''
        self.mask = ''
        self.mtu = ''
        self.ipaddr = ''
        self.ip6addr = []
        self.ip6mask = []
        self.hwaddr = ''
        self.flag_up = False
        self.flag_deprecated = False
        self.flag_broadcast = False
        self.flag_running = False
        self.flag_multicast = False
        self.flag_loopback = False
        self.flag_no_carrier = False

class BaseIfconfig(object):
    def add_interface(self, name):
        i = Interface(name)
        self.intf.append(i)

    def interface(self, name):
        for i in self.intf:
            if i.name == name:
                return i
        return None

    def has_interface(self, name):
        for i in self.intf:
            if i.name == name:
                return 1
        return 0

    def has_param(self, param, value):
        for i in self.intf:
            if not hasattr(i, param):
                continue

            if isinstance(getattr(i, param), list):
                if value in getattr(i, param):
                    return i
            else:
                if getattr(i, param) == value:
                    return i
        return None

    def get_matching_interfaces(self, param, value):
        l = []
        for i in self.intf:
            if not hasattr(i, param):
                continue
            if isinstance(getattr(i, param), list):
                if value in getattr(i, param):
                    l.append(i)
            else:
                if getattr(i, param) == value:
                    l.append(i)
        return l

    def __str__(self):
        s = ""
        for intf in self.intf:
            s += str(intf)
        s += "\nmcast: " + str(self.mcast_data)
        return s

    def __init__(self, mcast=False):
        self.intf = []
        self.mcast_data = {}

    def next_stacked_dev(self,dev):
        """Return the first available interfaceX:Y on  interfaceX
        """
        i = 1
        while True:
            stacked_dev = dev+':'+str(i)
            if not self.has_interface(stacked_dev):
                return stacked_dev
            i = i + 1

    def get_stacked_dev(self, dev, addr, log):
        """Upon start, a new interfaceX:Y will have to be assigned.
        Upon stop, the currently assigned interfaceX:Y will have to be
        found for ifconfig down
        """
        if ':' in addr:
            stacked_intf = self.has_param("ip6addr", addr)
        else:
            stacked_intf = self.has_param("ipaddr", addr)
        if stacked_intf is not None:
            if dev not in stacked_intf.name:
                base_intf = self.has_param("name", dev)
                if base_intf and hasattr(base_intf, "groupname"):
                    alt_intfs = [ i for i in self.get_matching_interfaces("groupname", base_intf.groupname) if i.name != base_intf.name and stacked_intf.name.startswith(i.name+":")]
                    if len(alt_intfs) == 1:
                        log.info("found %s plumbed on %s, in the same ipmp group than %s" % (addr, stacked_intf.name, dev))
                        return stacked_intf.name
                log.error("%s is plumbed but not on %s" % (addr, dev))
                return
            stacked_dev = stacked_intf.name
            log.debug("found matching stacked device %s" % stacked_dev)
        else:
            stacked_dev = self.next_stacked_dev(dev)
            log.debug("allocate new stacked device %s" % stacked_dev)
        return stacked_dev
