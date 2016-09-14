from rcGlobalEnv import rcEnv
import os
from subprocess import *
import datetime
from rcUtilities import try_decode, justcall, which

class Asset(object):
    s_config = "node configuration file"
    s_probe = "probe"
    s_default = "default"

    def __init__(self, node):
        self.node = node

    def get_mem_bytes(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'mem_bytes')
            source = self.s_config
        except:
            try:
                s = self._get_mem_bytes()
                source = self.s_probe
            except:
                pass
        self.print_mem_bytes(s, source)
        return s

    def print_mem_bytes(self, s, source):
        print("mem (%s)"%source)
        print("  %s MB"%s)

    def get_mem_banks(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'mem_banks')
            source = self.s_config
        except:
            try:
                s = self._get_mem_banks()
                source = self.s_probe
            except:
                pass
        self.print_mem_banks(s, source)
        return s

    def print_mem_banks(self, s, source):
        print("mem banks (%s)"%source)
        print("  %s"%s)

    def get_mem_slots(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'mem_slots')
            source = self.s_config
        except:
            try:
                s = self._get_mem_slots()
                source = self.s_probe
            except:
                pass
        self.print_mem_slots(s, source)
        return s

    def print_mem_slots(self, s, source):
        print("mem slots (%s)"%source)
        print("  %s"%s)

    def get_os_vendor(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'os_vendor')
            source = self.s_config
        except:
            try:
                s = self._get_os_vendor()
                source = self.s_probe
            except:
                pass
        self.print_os_vendor(s, source)
        return s

    def print_os_vendor(self, s, source):
        print("os vendor (%s)"%source)
        print("  %s"%s)

    def get_os_release(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'os_release')
            source = self.s_config
        except:
            try:
                s = self._get_os_release()
                source = self.s_probe
            except:
                pass
        self.print_os_release(s, source)
        return s

    def print_os_release(self, s, source):
        print("os release (%s)"%source)
        print("  %s"%s)

    def get_os_kernel(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'os_kernel')
            source = self.s_config
        except:
            try:
                s = self._get_os_kernel()
                source = self.s_probe
            except:
                pass
        self.print_os_kernel(s, source)
        return s

    def print_os_kernel(self, s, source):
        print("os kernel (%s)"%source)
        print("  %s"%s)

    def get_os_arch(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'os_arch')
            source = self.s_config
        except:
            try:
                s = self._get_os_arch()
                source = self.s_probe
            except:
                pass
        self.print_os_arch(s, source)
        return s

    def print_os_arch(self, s, source):
        print("os arch (%s)"%source)
        print("  %s"%s)

    def get_cpu_freq(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'cpu_freq')
            source = self.s_config
        except:
            try:
                s = self._get_cpu_freq()
                source = self.s_probe
            except:
                pass
        self.print_cpu_freq(s, source)
        return s

    def print_cpu_freq(self, s, source):
        print("cpu freq (%s)"%source)
        print("  %s Mhz"%s)

    def get_cpu_threads(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'cpu_threads')
            source = self.s_config
        except:
            try:
                s = self._get_cpu_threads()
                source = self.s_probe
            except:
                pass
        self.print_cpu_threads(s, source)
        return s

    def print_cpu_threads(self, s, source):
        print("cpu threads (%s)"%source)
        print("  %s"%s)

    def get_cpu_cores(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'cpu_cores')
            source = self.s_config
        except:
            try:
                s = self._get_cpu_cores()
                source = self.s_probe
            except:
                pass
        self.print_cpu_cores(s, source)
        return s

    def print_cpu_cores(self, s, source):
        print("cpu cores (%s)"%source)
        print("  %s"%s)

    def get_cpu_dies(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'cpu_dies')
            source = self.s_config
        except:
            try:
                s = self._get_cpu_dies()
                source = self.s_probe
            except:
                pass
        self.print_cpu_dies(s, source)
        return s

    def print_cpu_dies(self, s, source):
        print("cpu dies (%s)"%source)
        print("  %s"%s)

    def get_cpu_model(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'cpu_model')
            source = self.s_config
        except:
            try:
                s = self._get_cpu_model()
                source = self.s_probe
            except:
                pass
        self.print_cpu_model(s, source)
        return s

    def print_cpu_model(self, s, source):
        print("cpu model (%s)"%source)
        print("  %s"%s)

    def get_serial(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'serial')
            source = self.s_config
        except:
            try:
                s = self._get_serial()
                source = self.s_probe
            except:
                pass
        self.print_serial(s, source)
        return s

    def print_serial(self, s, source):
        print("serial (%s)"%source)
        print("  %s"%s)


    def get_enclosure(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'enclosure')
            source = self.s_config
        except:
            try:
                s = self._get_enclosure()
                source = self.s_probe
            except:
                pass
        self.print_enclosure(s, source)
        return s

    def print_enclosure(self, s, source):
        print("enclosure (%s)"%source)
        print("  %s"%s)

    def get_tz(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'tz')
            source = self.s_config
        except:
            try:
                s = self._get_tz()
                source = self.s_probe
            except Exception as e:
                print(e)
                pass
        if s:
            self.print_tz(s, source)
        return s

    def _get_tz(self):
        cmd = ["date", "+%z"]
        out, err, ret = justcall(cmd)
        out = out.strip()
        if len(out) != 5:
            return
        return out[:3] + ":" + out[3:]

    def print_tz(self, s, source):
        print("timezone (%s)"%source)
        print("  %s"%s)

    def get_connect_to(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'connect_to')
            source = self.s_config
        except:
            try:
                s = self._get_connect_to()
                source = self.s_probe
            except:
                pass
        if s:
            self.print_connect_to(s, source)
        return s

    def _get_connect_to(self):
        if self.data["model"] != "Google":
            return
        if not which("gcloud"):
            return
        cmd = ["gcloud", "compute", "instances", "describe", "-q", "--format", "json", rcEnv.nodename]
        out, err, ret = justcall(cmd)
        """
	  "networkInterfaces": [
	    {
	      "accessConfigs": [
		{
		  "kind": "compute#accessConfig",
		  "name": "external-nat",
		  "natIP": "23.251.137.71",
		  "type": "ONE_TO_ONE_NAT"
		}
	      ],
	      "name": "nic0",
	      "networkIP": "10.132.0.2",
	    }
        """
        import json
        try:
            data = json.loads(out)
        except:
            return
        nics = [d for d in data["networkInterfaces"] if len(d["accessConfigs"]) > 0]
        if len(nics) == 0:
            return
        for nic in nics:
            if nic["name"] == "nic0":
                return nic["accessConfigs"][0]["natIP"]
        return nics[0]["accessConfigs"][0]["natIP"]

    def print_connect_to(self, s, source):
        print("connect to address (%s)"%source)
        print("  %s"%s)

    def get_model(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'model')
            source = self.s_config
        except:
            try:
                s = self._get_model()
                source = self.s_probe
            except:
                pass
        self.print_model(s, source)
        return s

    def print_model(self, s, source):
        print("model (%s)"%source)
        print("  %s"%s)

    def get_node_env(self):
        s = 'TST'
        source = self.s_default
        try:
            if self.node.config.has_option('node', 'env'):
                s = self.node.config.get('node', 'env')
                source = self.s_config
            elif self.node.config.has_option('node', 'host_mode'):
                # compat
                s = self.node.config.get('node', 'host_mode')
                source = self.s_config
        except:
            pass
        self.print_node_env(s, source)
        return s

    def print_node_env(self, s, source):
        print("host mode (%s)"%source)
        print("  %s"%s)

    def get_sec_zone(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'sec_zone')
            source = self.s_config
        except:
            pass
        self.print_sec_zone(s, source)
        return s

    def print_sec_zone(self, s, source):
        if s is None:
            return
        print("security zone (%s)"%source)
        print("  %s"%s)

    def get_asset_env(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'asset_env')
            s = try_decode(s)
            source = self.s_config
        except:
            pass
        self.print_asset_env(s, source)
        return s

    def print_asset_env(self, s, source):
        if s is None:
            return
        print("asset environment (%s)"%source)
        print("  %s"%s)

    def get_version(self):
        try:
            import version
            s = version.version
        except:
            s = "0"
        self.print_version(s)
        return s

    def print_version(self, s):
        print("agent version")
        print("  %s"%s)

    def get_listener_port(self):
        s = str(rcEnv.listener_port)
        source = self.s_default
        try:
            s = str(self.node.config.getint('listener', 'port'))
            source = self.s_config
        except:
            pass
        self.print_listener_port(s, source)
        return s

    def print_listener_port(self, s, source):
        if s is None:
            return
        print("listener port (%s)"%source)
        print("  %s"%s)

    def print_generic_cf(self, s, source, title):
        if s is None:
            return
        print("%s (%s)"%(title, source))
        print("  %s"%s)

    def get_loc_country(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_country')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location country")
        return s

    def get_loc_city(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_city')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location city")
        return s

    def get_loc_addr(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_addr')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location city")
        return s

    def get_loc_building(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_building')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location building")
        return s

    def get_loc_floor(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_floor')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location floor")
        return s

    def get_loc_room(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_room')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location room")
        return s

    def get_loc_rack(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_rack')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location rack")
        return s

    def get_loc_zip(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'loc_zip')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "location zip")
        return s

    def get_team_integ(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'team_integ')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "team integration")
        return s

    def get_team_support(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'team_support')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "team support")
        return s

    def get_hba(self):
        try:
            hba = self._get_hba()
        except:
            hba = []
        self.print_hba(hba)
        return hba

    def print_hba(self, hba):
        print("hba (probe)")
        for h in hba:
            print("  %-5s %s"%(h[1], h[0]))

    def get_targets(self):
        try:
            s = self._get_targets()
        except:
            s = []
        self.print_targets(s)
        return s

    def print_targets(self, targets):
        print("paths to targets (probe)")
        for t in targets:
            print("  %s - %s"%t)

    def get_uids(self):
        return self.get_ids("/etc/passwd")

    def get_gids(self):
        return self.get_ids("/etc/group")

    def get_ids(self, p):
        if rcEnv.sysname == "Windows":
            return []
        if not os.path.exists(p):
            return []
        with open(p, 'r') as f:
            buff = f.read()
        d = []
        for line in buff.split('\n'):
            line = line.strip()
            if line.startswith("#"):
                continue
            l = line.split(':')
            if len(l) < 3:
                continue
            try:
                i = int(l[2])
            except:
                continue
            name = repr(l[0]).strip("'")
            d.append((name, l[2]))
        return d

    def get_lan(self):
        kwargs = {'mcast': True}
        if rcEnv.sysname == 'HP-UX':
            kwargs['hwaddr'] = True
        rcIfconfig = __import__('rcIfconfig'+rcEnv.sysname)
        ifconfig = rcIfconfig.ifconfig(**kwargs)
        lan = {}
        for intf in ifconfig.intf:
            if len(intf.hwaddr) == 0:
                continue
            if intf.hwaddr not in lan:
                lan[intf.hwaddr] = []
            if type(intf.ipaddr) == str and intf.ipaddr != '':
                d = {'type': 'ipv4',
                     'intf': intf.name,
                     'addr': intf.ipaddr,
                     'mask': intf.mask,
                     'flag_deprecated': intf.flag_deprecated,
                    }
                lan[intf.hwaddr] += [d]
            elif type(intf.ipaddr) == list:
                for i, ip in enumerate(intf.ipaddr):
                    if ip != '':
                        d = {'type': 'ipv4',
                             'intf': intf.name,
                             'addr': ip,
                             'mask': intf.mask[i],
                             'flag_deprecated': intf.flag_deprecated,
                            }
                    lan[intf.hwaddr] += [d]
            for i, ip6 in enumerate(intf.ip6addr):
                d = {'type': 'ipv6',
                     'intf': intf.name,
                     'addr': intf.ip6addr[i],
                     'mask': intf.ip6mask[i],
                     'flag_deprecated': intf.flag_deprecated,
                    }
                lan[intf.hwaddr] += [d]
            if intf.name in ifconfig.mcast_data:
                for addr in ifconfig.mcast_data[intf.name]:
                    if ':' in addr:
                        addr_type = 'ipv6'
                    else:
                        addr_type = 'ipv4'
                    d = {'type': addr_type,
                         'intf': intf.name,
                         'addr': addr,
                         'mask': "",
                         'flag_deprecated': intf.flag_deprecated,
                        }
                    lan[intf.hwaddr] += [d]

        self.print_lan(lan)
        return lan

    def print_lan(self, lan):
        print("lan (probe)")
        for h, l in lan.items():
            for d in l:
                if d['mask'] != "":
                    addr_mask = "%s/%s" % (d['addr'], d['mask'])
                else:
                    addr_mask = d['addr']
                s = "  %s %-8s %-5s %s"%(h, d['intf'], d['type'], addr_mask)
                if d['flag_deprecated']:
                    s += " (deprecated)"
                print(s)

    def get_last_boot(self):
        os.environ["LANG"] = "C"
        cmd = ["/usr/bin/uptime"]
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        l = out.split()

        i = 0
        for s in ("days,", "day(s),"):
            try:
                i = l.index(s)
                break
            except:
                pass

        if i == 0:
            last = datetime.datetime.now()
        else:
            try:
                last = datetime.datetime.now() - datetime.timedelta(days=int(l[i-1]))
            except:
                return

        last = last.strftime("%Y-%m-%d")
        self.print_last_boot(last)
        return last

    def print_last_boot(self, last):
        print("last boot (probe)")
        print("  %s" % last)

    def get_asset_dict(self):
        self.data = {}
        self.data['nodename'] = rcEnv.nodename
        self.data['fqdn'] = rcEnv.fqdn
        self.data['version'] = self.get_version()
        self.data['os_name'] = rcEnv.sysname
        self.data['os_vendor'] = self.get_os_vendor()
        self.data['os_release'] = self.get_os_release()
        self.data['os_kernel'] = self.get_os_kernel()
        self.data['os_arch'] = self.get_os_arch()
        self.data['mem_bytes'] = self.get_mem_bytes()
        self.data['mem_banks'] = self.get_mem_banks()
        self.data['mem_slots'] = self.get_mem_slots()
        self.data['cpu_freq'] = self.get_cpu_freq()
        self.data['cpu_threads'] = self.get_cpu_threads()
        self.data['cpu_cores'] = self.get_cpu_cores()
        self.data['cpu_dies'] = self.get_cpu_dies()
        self.data['cpu_model'] = self.get_cpu_model()
        self.data['serial'] = self.get_serial()
        self.data['model'] = self.get_model()
        self.data['env'] = self.get_node_env()
        self.data['enclosure'] = self.get_enclosure()
        self.data['listener_port'] = self.get_listener_port()
        connect_to = self.get_connect_to()
        if connect_to is not None:
            self.data['connect_to'] = connect_to
        last_boot = self.get_last_boot()
        if last_boot is not None:
            self.data['last_boot'] = last_boot
        sec_zone = self.get_sec_zone()
        if sec_zone is not None:
            self.data['sec_zone'] = sec_zone
        asset_env = self.get_asset_env()
        if asset_env is not None:
            self.data['asset_env'] = asset_env
        tz = self.get_tz()
        if tz is not None:
            self.data['tz'] = tz
        loc_country = self.get_loc_country()
        if loc_country is not None:
            self.data['loc_country'] = loc_country
        loc_city = self.get_loc_city()
        if loc_city is not None:
            self.data['loc_city'] = loc_city
        loc_building = self.get_loc_building()
        if loc_building is not None:
            self.data['loc_building'] = loc_building
        loc_room = self.get_loc_room()
        if loc_room is not None:
            self.data['loc_room'] = loc_room
        loc_rack = self.get_loc_rack()
        if loc_rack is not None:
            self.data['loc_rack'] = loc_rack
        loc_addr = self.get_loc_addr()
        if loc_addr is not None:
            self.data['loc_addr'] = loc_addr
        loc_floor = self.get_loc_floor()
        if loc_floor is not None:
            self.data['loc_floor'] = loc_floor
        loc_zip = self.get_loc_zip()
        if loc_zip is not None:
            self.data['loc_zip'] = loc_zip
        team_integ = self.get_team_integ()
        if team_integ is not None:
            self.data['team_integ'] = team_integ
        team_support = self.get_team_support()
        if team_support is not None:
            self.data['team_support'] = team_support
        hba = self.get_hba()
        if hba is not None:
            self.data['hba'] = hba
        targets = self.get_targets()
        if targets is not None:
            self.data['targets'] = targets
        lan = self.get_lan()
        if lan is not None:
            self.data['lan'] = lan
        uids = self.get_uids()
        if uids is not None:
            self.data['uids'] = uids
        gids = self.get_gids()
        if gids is not None:
            self.data['gids'] = gids
        return self.data
