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

from svc import Svc
from freezer import Freezer
import rcStatus
import os

pathetc = os.path.join(pathsvc, 'etc')

def is_service(f):
    if os.path.realpath(f) != os.path.realpath(svcmgr):
        return False
    if not os.path.exists(f + '.env'):
        return False
    return True



class Node(Svc, Freezer):
    """Defines a cluster node.  It contain list of Svc.
    """
    def __init__(self):
        self.svcs = []
        Svc.__init__(self, None, None, optional, disabled)
        Freezer.__init__(self, '')
        #
        # add services
        #
        for name in os.listdir(pathetc):
            if is_service(os.path.join(pathetc, name)):
                s = svcBuilder.build(name)
                if s is None :
                    continue
                self += s

        #
        # lookup drp_nodes
        #
        drp_nodes = []
        for s in self.svcs:
            if s.drpnode not in drp_nodes:
                drp_nodes.append(s.drpnode)

        #
        # add node file synchronization resources to drp
        #
        for drp_node in drp_nodes:
            for src, dst, excl in rcEnv.drp_syncs:
                dst = drp_node+":"+rcEnv.drp_dir+"/"+dst
                self += Rsync(src.join(' '), dst, excl.join(' '))

    def __iadd__(self, s):
        if not isinstance(s, Svc):
                pass
        self.svcs.append(s)
        return self

    def syncnodes(self):
        for s in self.svcs:
            s.syncnodes()

    def syncdrp(self):
        Svc.syncdrp(self)
        for s in self.svcs:
            s.syncnodes()

    def get_svcs_on(self, status):
        return [ s for s in self.svcs if s.status == status ]

    def allupservice(self):
        return get_svcs_on(rcStatus.UP)

    def alldownservice(self):
        return get_svcs_on(rcStatus.UP)

    def allservices:
        return self.svcs

if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
