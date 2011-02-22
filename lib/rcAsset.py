#!/usr/bin/python2.6
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
    def __init__(self, node):
        self.node = node

    def get_mem_bytes(self):
        return '0'

    def get_mem_banks(self):
        return '0'

    def get_mem_slots(self):
        return '0'

    def get_os_vendor(self):
        return 'Unknown'

    def get_os_release(self):
        return 'Unknown'

    def get_os_kernel(self):
        return 'Unknown'

    def get_os_arch(self):
        return 'Unknown'

    def get_cpu_freq(self):
        return 'Unknown'

    def get_cpu_cores(self):
        return '0'

    def get_cpu_dies(self):
        return '0'

    def get_cpu_model(self):
        return 'Unknown'

    def get_serial(self):
        return 'Unknown'

    def get_model(self):
        return 'Unknown'

    def get_environnement(self):
        try:
            return self.node.config.get('node', 'host_mode')
        except:
            return 'TST'

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
        return d
