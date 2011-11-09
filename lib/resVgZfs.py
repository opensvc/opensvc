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
"""Module providing ZFS resources
"""

from rcGlobalEnv import rcEnv
import resDg
from rcUtilities import qcall
import os
import rcExceptions as ex

import re

class Pool(resDg.Dg):
    """ basic pool resource
    """
    def __init__(self, rid=None, name=None, type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False):
        self.label = 'pool ' + name
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.zpool',
                          always_on=always_on,
                          optional=optional, disabled=disabled, tags=tags,
                          monitor=monitor)

    def disklist_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.disklist')

    def files_to_sync(self):
        return [self.disklist_name()]

    def presync(self):
        """ this one is exported as a service command line arg
        """
        dl = self._disklist()
        import json
        with open(self.disklist_name(), 'w') as f:
            f.write(json.dumps(list(dl)))

    def has_it(self):
        """Returns True if the pool is present
        """
        ret = qcall( [ 'zpool', 'list', self.name ] )
        if ret == 0 :
            return True
        return False

    def is_up(self):
        """Returns True if the pool is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'zpool', 'list', '-H', '-o', 'health', self.name ]
        (ret, out, err) = self.call(cmd)
        if out.strip() == "ONLINE" :
            return True
        return False

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            return 0
        cmd = [ 'zpool', 'import', '-f', '-o', 'cachefile='+os.path.join(rcEnv.pathvar, 'zpool.cache'), self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        cmd = [ 'zpool', 'export', self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def disklist(self):
        if not os.path.exists(self.disklist_name()):
            s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
            import rcStatus
            if s['overall'].status == rcStatus.UP:
                self.log.debug("no disklist cache file and service up ... refresh disklist cache")
                self.presync()
            else:
                self.log.debug("no disklist cache file and service not up ... unable to evaluate disklist")
                return []
        with open(self.disklist_name(), 'r') as f:
            buff = f.read()
        import json
        try:
            dl = set(json.loads(buff))
        except:
            self.log.error("corrupted disklist cache file %s"%self.disklist_name())
            raise ex.excError
        return dl

    def _disklist(self):
        """disklist() search zpool vdevs from
        output of : zpool status poolname if status cmd == 0
        else
        output of : zpool import output if status cmd == 0

        disklist(self) update self.disks[]
        """

        # return cache if initialized
        if len(self.disks) > 0 :
            return self.disks

        disks = set([])
        cmd = [ 'zpool', 'status', self.name ]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        for line in out.split('\n'):
            if re.match('^\t  ', line) is not None:
                if re.match('^\t  mirror', line) is not None:
                    continue
                if re.match('^\t  raid', line) is not None:
                    continue
                # vdev entry
                disk=line.split()[0]
                if re.match("^.*", disk) is not None :
                    disks.add("/dev/rdsk/" + disk )

        self.log.debug("found disks %s held by pool %s" % (disks, self.name))
        for d in disks:
            if re.match('^.*s[0-9]*$', d) is None:
                d += "s2"
            else:
                regex = re.compile('s[0-9]*$', re.UNICODE)
                d = regex.sub('s2', d)
            self.disks.add(d)

        return self.disks

if __name__ == "__main__":
    for c in (Pool,) :
        help(c)


        # return cache if initialized
    print """p=Pool("svczfs1")"""
    p=Pool("svczfs1")
    print "show p", p
    print """p.do_action("start")"""
    p.do_action("start")
    print """p.do_action("stop")"""
    p.do_action("stop")
