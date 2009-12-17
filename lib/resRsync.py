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
import rcExceptions as ex
import rcStatus
import resources as Res

def remote_fs_mounted(self, node):
    """Verify the remote fs is mounted before we send data.
    """
    if self.dstfs is None:
        """No check has been configured. Assume the admin knows better.
        """
        return True
    cmd = rcEnv.rsh.split(' ')+[node, '--', 'df', self.dst]
    (ret, out) = self.call(cmd)
    if ret != 0:
        raise ex.excError
    if self.dstfs not in out.split():
        self.log.error("The destination fs %s is not mounted on node %s. refuse to sync %s to protect parent fs"%(self.dstfs, node, self.dst))
        return False
    return True

def remote_node_type(self, node, type):
    if type == 'drpnodes':
        type = 'DEV'
    elif type == 'nodes':
        type = 'PRD'
    else:
        self.log.error('expected remote node type is bogus: %s'%type)
        raise
    host_mode_f = os.path.join(rcEnv.pathvar, 'host_mode') 
    cmd = rcEnv.rsh.split(' ')+[node, '--', 'LANG=C', 'cat', host_mode_f]
    (ret, out) = self.call(cmd)
    if ret != 0:
        raise ex.excError
    if out.split()[0] == type:
        return True
    self.log.error("node %s type is not '%s'. Check %s:%s"%\
                   (node, type, node, host_mode_f))
    return False

def nodes_to_sync(self, type=None):
    """Discard the local node from the set
    """
    if type in self.target.keys():
        targets = self.target[type]
    else:
        return set([])

    targets -= set([rcEnv.nodename])

    for node in targets.copy():
        if not remote_node_type(self, node, type):
            targets -= set([node])
            continue

    if len(targets) == 0:
        raise ex.syncNoNodesToSync

    return targets

def sync(self, type):
    if hasattr(self, "alt_src"):
        """ The pre_action() has provided us with a better source
            to sync from. Use that
        """
        src = self.alt_src
    else:
        src = self.src

    if type not in self.target.keys():
        self.log.debug('%s => %s sync not applicable to %s',
                  (src, self.dst, type))
        return 0
    targets = nodes_to_sync(self, type)

    if len(src) == 0:
        self.log.debug("no files to sync")
        raise ex.syncNoFilesToSync

    for node in targets:
        if not remote_fs_mounted(self, node):
            continue
        dst = node + ':' + self.dst
        cmd = ['rsync'] + self.options + self.exclude + src
        cmd.append(dst)
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
            continue
    return

class Rsync(Res.Resource):
    """Defines a rsync job from local node to its remote nodes. Target nodes
    can be restricted to production sibblings or to disaster recovery nodes,
    or both.
    """
    timeout = 3600
    options = [ '-HpogDtrlvx', '--stats', '--delete', '--force', '--timeout='+str(timeout) ]

    def pre_action(self, rset, action):
        """Actions to do before resourceSet iterates through the resources to
           trigger action() on each one
        """

        """Don't sync PRD services when running on !PRD node
        """
        if self.svc.svctype == 'PRD' and rcEnv.host_mode != 'PRD':
            self.log.debug("won't sync a PRD service running on a !PRD node")
            raise ex.excAbortAction

        """Accept to sync from here only if the service is up
        """
        if self.svc.status() != 0:
            self.log.debug("won't sync a service not up")
            raise ex.excAbortAction

        """ Is there at least one node to sync ?
        """
        targets = set([])
        for r in rset.resources:
            try:
                if action == "syncnodes":
                    targets |= nodes_to_sync(r, 'nodes')
                else:
                    targets |= nodes_to_sync(r, 'drpnodes')
            except ex.syncNoNodesToSync:
                pass
        if len(targets) == 0:
            self.log.debug("no node to sync")
            raise ex.excAbortAction

        import snapLvmLinux as snap
        try:
            rset.snaps = snap.snap(self, rset, action)
        except ex.syncNotSnapable:
            raise ex.excError

    def post_action(self, rset, action):
        """Actions to do after resourceSet has iterated through the resources to
           trigger action() on each one
        """
        import snapLvmLinux as snap
        snap.snap_cleanup(self, rset)

    def syncnodes(self):
        try:
            sync(self, "nodes")
        except ex.syncNoFilesToSync:
            pass
        except ex.syncNoNodesToSync:
            self.log.debug("no node to sync")
            raise ex.excAbortAction

    def syncdrp(self):
        try:
            sync(self, "drpnodes")
        except ex.syncNoFilesToSync:
            pass
        except ex.syncNoNodesToSync:
            self.log.debug("no node to sync")
            raise ex.excAbortAction

    def __init__(self, src, dst, exclude=[], target={}, dstfs=None, snap=False,
                 optional=False, disabled=False):
        self.src = src
        self.dst = dst
        self.dstfs = dstfs
        self.exclude = exclude
        self.snap = snap
        self.target = target
        Res.Resource.__init__(self, "rsync", optional, disabled)

    def __str__(self):
        return "%s src=%s dst=%s exclude=%s target=%s" % (Res.Resource.__str__(self),\
                self.src, self.dst, self.exclude, str(self.target))

