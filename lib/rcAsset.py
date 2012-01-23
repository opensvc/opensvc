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
        print "mem (%s)"%source
        print "  %s MB"%s

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
        print "mem banks (%s)"%source
        print "  %s"%s

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
        print "mem slots (%s)"%source
        print "  %s"%s

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
        print "os vendor (%s)"%source
        print "  %s"%s

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
        print "os release (%s)"%source
        print "  %s"%s

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
        print "os kernel (%s)"%source
        print "  %s"%s

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
        print "os arch (%s)"%source
        print "  %s"%s

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
        print "cpu freq (%s)"%source
        print "  %s Mhz"%s

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
        print "cpu cores (%s)"%source
        print "  %s"%s

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
        self.print_cpu_cores(s, source)
        return s

    def print_cpu_dies(self, s, source):
        print "cpu dies (%s)"%source
        print "  %s"%s

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
        print "cpu model (%s)"%source
        print "  %s"%s

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
        print "serial (%s)"%source
        print "  %s"%s


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
        print "model (%s)"%source
        print "  %s"%s

    def get_environnement(self):
        s = 'TST'
        source = self.s_default
        try:
            s = self.node.config.get('node', 'host_mode')
            source = self.s_config
        except:
            pass
        self.print_environnement(s, source)
        return s

    def print_environnement(self, s, source):
        print "environment (%s)"%source
        print "  %s"%s

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
        print "team responsible (%s)"%source
        print "  %s"%s

    def print_generic_cf(self, s, source, title):
        if s is None:
            return
        print "%s (%s)"%(title, source)
        print "  %s"%s

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
        except:
            pass
        self.print_generic_cf(s, source, "team integration")
        return s

    def get_team_support(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'team_support')
        except:
            pass
        self.print_generic_cf(s, source, "team support")
        return s

    def get_project(self):
        s = None
        source = self.s_default
        try:
            s = self.node.config.get('node', 'project')
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
        print "hba (probe)"
        for h in hba:
            print "  %-5s %s"%(h[1], h[0])

    def get_targets(self):
        try:
            s = self._get_targets()
        except:
            s = []
        self.print_targets(s)
        return s

    def print_targets(self, targets):
        print "paths to targets (probe)"
        for t in targets:
            print "  %s - %s"%t

    def get_asset_dict(self):
        d = {}
        d['nodename'] = rcEnv.nodename
        d['os_name'] = rcEnv.sysname
        d['os_vendor'] = self.get_os_vendor()
        d['os_release'] = self.get_os_release()
        d['os_kernel'] = self.get_os_kernel()
        d['os_arch'] = self.get_os_arch()
        d['mem_bytes'] = self.get_mem_bytes()
        d['mem_banks'] = self.get_mem_banks()
        d['mem_slots'] = self.get_mem_slots()
        d['cpu_freq'] = self.get_cpu_freq()
        d['cpu_cores'] = self.get_cpu_cores()
        d['cpu_dies'] = self.get_cpu_dies()
        d['cpu_model'] = self.get_cpu_model()
        d['serial'] = self.get_serial()
        d['model'] = self.get_model()
        d['environnement'] = self.get_environnement()
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
        return d
