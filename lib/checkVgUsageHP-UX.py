#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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
import checks
from rcUtilities import call

class check(checks.check):
    chk_type = "vg_u"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for rs in svc.get_res_sets('disk.vg'):
                for r in rs.resources:
                    if r.name == vgname:
                        return svc.svcname
        return ''

    def do_check(self):
        """
        # vgdisplay -F
vg_name=/dev/vg00:vg_write_access=read,write:vg_status=available:max_lv=255:cur_lv=9:open_lv=9:max_pv=16:cur_pv=1:act_pv=1:max_pe_per_pv=4384:vgda=2:pe_size=32:total_pe=4347:alloc_pe=2712:free_pe=1635:total_pvg=0:total_spare_pvs=0:total_spare_pvs_in_use=0:vg_version=1.0:vg_max_size=2192g:vg_max_extents=70144
        """
        cmd = ['vgdisplay', '-F']
        (ret, out) = call(cmd, errlog=False)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        for line in lines:
            l = line.split(':')
            if len(l) < 10:
                continue
            instance = None
            free = None
            size = None
            for w in l:
                if 'vg_name' in w:
                    instance = w.split('=')[1].replace('/dev/','')
                elif 'total_pe' in w:
                    size = int(w.split('=')[1])
                elif 'free_pe' in w:
                    free = int(w.split('=')[1])
            if instance is None or free is None or size is None:
                continue
            val = int(100*(size-free)/size)
            r.append({'chk_instance': instance,
                      'chk_value': str(val),
                      'chk_svcname': self.find_svc(instance),
                     }
                    )
        return r
