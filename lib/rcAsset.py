#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
from rcGlobalEnv import rcEnv
import os
from subprocess import *
import datetime
from rcUtilities import try_decode

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

    def get_host_mode(self):
        s = 'TST'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'host_mode')
            source = self.s_config
        except:
            pass
        self.print_host_mode(s, source)
        return s

    def print_host_mode(self, s, source):
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

    def get_environnement(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'environment')
            s = try_decode(s)
            source = self.s_config
        except:
            pass
        self.print_environnement(s, source)
        return s

    def print_environnement(self, s, source):
        if s is None:
            return
        print("environment (%s)"%source)
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

    def get_team_responsible(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'team_responsible')
            source = self.s_config
        except:
            pass
        self.print_team_responsible(s, source)
        return s

    def print_team_responsible(self, s, source):
        if s is None:
            return
        print("team responsible (%s)"%source)
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

    def get_project(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'project')
            source = self.s_config
        except:
            pass
        self.print_generic_cf(s, source, "project")
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
            d.append((l[0], l[2]))
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
                    }
                lan[intf.hwaddr] += [d]
            elif type(intf.ipaddr) == list:
                for i, ip in enumerate(intf.ipaddr):
                    if ip != '':
                        d = {'type': 'ipv4',
                             'intf': intf.name,
                             'addr': ip,
                             'mask': intf.mask[i],
                            }
                    lan[intf.hwaddr] += [d]
            for i, ip6 in enumerate(intf.ip6addr):
                d = {'type': 'ipv6',
                     'intf': intf.name,
                     'addr': intf.ip6addr[i],
                     'mask': intf.ip6mask[i],
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
                print("  %s %-8s %-5s %s"%(h, d['intf'], d['type'], addr_mask))

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
        d = {}
        d['nodename'] = rcEnv.nodename
        d['fqdn'] = rcEnv.fqdn
        d['version'] = self.get_version()
        d['os_name'] = rcEnv.sysname
        d['os_vendor'] = self.get_os_vendor()
        d['os_release'] = self.get_os_release()
        d['os_kernel'] = self.get_os_kernel()
        d['os_arch'] = self.get_os_arch()
        d['mem_bytes'] = self.get_mem_bytes()
        d['mem_banks'] = self.get_mem_banks()
        d['mem_slots'] = self.get_mem_slots()
        d['cpu_freq'] = self.get_cpu_freq()
        d['cpu_threads'] = self.get_cpu_threads()
        d['cpu_cores'] = self.get_cpu_cores()
        d['cpu_dies'] = self.get_cpu_dies()
        d['cpu_model'] = self.get_cpu_model()
        d['serial'] = self.get_serial()
        d['model'] = self.get_model()
        d['host_mode'] = self.get_host_mode()
        d['enclosure'] = self.get_enclosure()
        d['listener_port'] = self.get_listener_port()
        last_boot = self.get_last_boot()
        if last_boot is not None:
            d['last_boot'] = last_boot
        sec_zone = self.get_sec_zone()
        if sec_zone is not None:
            d['sec_zone'] = sec_zone
        environnement = self.get_environnement()
        if environnement is not None:
            d['environnement'] = environnement
        loc_country = self.get_loc_country()
        if loc_country is not None:
            d['loc_country'] = loc_country
        loc_city = self.get_loc_city()
        if loc_city is not None:
            d['loc_city'] = loc_city
        loc_building = self.get_loc_building()
        if loc_building is not None:
            d['loc_building'] = loc_building
        loc_room = self.get_loc_room()
        if loc_room is not None:
            d['loc_room'] = loc_room
        loc_rack = self.get_loc_rack()
        if loc_rack is not None:
            d['loc_rack'] = loc_rack
        loc_addr = self.get_loc_addr()
        if loc_addr is not None:
            d['loc_addr'] = loc_addr
        loc_floor = self.get_loc_floor()
        if loc_floor is not None:
            d['loc_floor'] = loc_floor
        loc_zip = self.get_loc_zip()
        if loc_zip is not None:
            d['loc_zip'] = loc_zip
        team_responsible = self.get_team_responsible()
        if team_responsible is not None:
            d['team_responsible'] = team_responsible
        team_integ = self.get_team_integ()
        if team_integ is not None:
            d['team_integ'] = team_integ
        team_support = self.get_team_support()
        if team_support is not None:
            d['team_support'] = team_support
        project = self.get_project()
        if project is not None:
            d['project'] = project
        hba = self.get_hba()
        if hba is not None:
            d['hba'] = hba
        targets = self.get_targets()
        if targets is not None:
            d['targets'] = targets
        lan = self.get_lan()
        if lan is not None:
            d['lan'] = lan
        uids = self.get_uids()
        if uids is not None:
            d['uids'] = uids
        gids = self.get_gids()
        if gids is not None:
            d['gids'] = gids
        return d
