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
import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which
import rcStatus
import resources as Res

def remote_node_type(self, node, type):
    if type == 'drpnodes':
        type = 'DEV'
    elif type == 'nodes':
        type = 'PRD'
    else:
        self.log.error('expected remote node type is bogus: %s'%type)
        raise
    host_mode_f = os.path.join(rcEnv.pathvar, 'host_mode') 
    cmd = rcEnv.rsh.split(' ')+[node, '--', 'cat', host_mode_f]
    (ret, out) = self.call(cmd)
    if out.split()[0] == type:
        return True
    self.log.error("node %s type is not '%s'. Check %s:%s"%\
                   (node, type, node, host_mode_f))
    return False

def sync(self, type):
    if type not in self.target.keys():
        self.log.debug('%s => %s sync not applicable to %s',
                  (self.src, self.dst, type))
        return 0
    targets = self.target[type]

    """Accept to sync from here only if the service is up
    """
    if self.svc.status() != 0:
        self.log.error("won't sync a service not up")
        return 1

    """Discard the local node from the set
    """
    targets -= set([rcEnv.nodename])

    if len(targets) == 0:
        self.log.info("no node to sync")
        return 0
    if len(self.src) == 0:
        self.log.debug("no files to sync")
        return 0

    for node in targets:
        if not remote_node_type(self, node, type):
            return 1
        dst = node + ':' + self.dst
        cmd = self.cmd
        cmd.append(dst)
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("node %s synchronization failed (%s => %s)" % (node, self.src, dst))
            return 1
    return 0

class Rsync(Res.Resource):
    """Defines a rsync job from local node to its remote nodes. Target nodes
    can be restricted to production sibblings or to disaster recovery nodes,
    or both.
    """
    timeout = 3600
    options = [ '-HpogDtrlvx', '--stats', '--delete', '--force', '--timeout='+str(timeout) ]

    def syncnodes(self):
        return sync(self, "nodes")

    def syncdrp(self):
        return sync(self, "drpnodes")

    def __init__(self, src, dst, exclude=[], target={},
                 optional=False, disabled=False):
        self.src = src
        self.dst = dst
        self.exclude = exclude
        self.target = target
        self.cmd = ['rsync'] + self.options + self.exclude + self.src
        Res.Resource.__init__(self, "rsync", optional, disabled)

    def __str__(self):
        return "%s src=%s dst=%s exclude=%s target=%s" % (Res.Resource.__str__(self),\
                self.src, self.dst, self.exclude, str(self.target))

