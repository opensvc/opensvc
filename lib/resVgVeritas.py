#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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
# To change this template, choose Tools | Templates
# and open the template in the editor.
"""Module providing Veritas Volume group resources
"""

import resDg
import re
from rcUtilities import qcall

class Vg(resDg.Dg):
    """ basic Veritas Volume group resource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 type=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        self.label = name
        resDg.Dg.__init__(self,
                          rid=rid,
                          name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

    def has_it(self):
        """Returns True if the vg is present
        """
        ret = qcall( [ 'vxdg', 'list', self.name ] )
        if ret == 0 :
            return True
        else:
            return False

    def is_up(self):
        """Returns True if the vg is present and not disabled
        """
        if not self.has_it():
            return False
        cmd = [ 'vxprint', '-ng', self.name ]
        ret = qcall(cmd)
        if ret == 0 :
                return True
        else:
                return False

    def do_startvol(self):
        cmd = [ 'vxvol', '-g', self.name, '-f', 'startall' ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def do_stopvol(self):
        cmd = [ 'vxvol', '-g', self.name, '-f', 'stopall' ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            ret = self.do_startvol()
            if self == 0 :
                return 0
            else:
                return ret
        for flag in [ '-t', '-tC', '-tCf']:
            cmd = [ 'vxdg', flag, 'import', self.name ]
            (ret, out, err) = self.vcall(cmd)
            if ret == 0 :
                ret = self.do_startvol()
                return ret
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        ret = self.do_stopvol()
        cmd = [ 'vxdg', 'deport', self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def disklist(self):
        """disklist() search vg disks from
        output of : vxdisk -g vgname -q  path

        disklist(self) update self.disks[]
        """
        if len(self.disks) > 0 :
            return self.disks

        disks = set([])
        cmd = [ 'vxdisk', '-g', self.name, '-q', 'list' ]
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0 :
            self.disks = disks
            return disks
        for line in out.split('\n'):
            disk = line.split(" ")[0]
            if disk != '' :
                if re.match('^.*s[0-9]$', disk) is None:
                    disk += "s2"
                disks.add("/dev/rdsk/" + disk )
       
        self.log.debug("found disks %s held by pool %s" % (disks, self.name))
        self.disks = disks

        return disks

if __name__ == "__main__":
    for c in (Pool,) :
        help(c)

    print("""p=Vg("vzone1vg")""")
    p=Pool("vzone1vg")
    print("show p", p)
    print("""p.do_action("start")""")
    p.do_action("start")
    print("""p.do_action("stop")""")
    p.do_action("stop")
