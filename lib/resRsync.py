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
import action as ex
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
        return False
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
    cmd = rcEnv.rsh.split(' ')+[node, '--', 'cat', host_mode_f]
    (ret, out) = self.call(cmd)
    if out.split()[0] == type:
        return True
    self.log.error("node %s type is not '%s'. Check %s:%s"%\
                   (node, type, node, host_mode_f))
    return False

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
    targets = self.target[type]

    """Don't sync PRD services when running on !PRD node
    """
    if self.svc.svctype == 'PRD' and rcEnv.host_mode != 'PRD':
        self.log.error("won't sync a PRD service running on a !PRD node")
        return 1

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
        raise ex.syncNoNodesToSync
    if len(src) == 0:
        self.log.debug("no files to sync")
        raise ex.syncNoFilesToSync

    for node in targets:
        if not remote_node_type(self, node, type):
            return 1
        if not remote_fs_mounted(self, node):
            return 1
        dst = node + ':' + self.dst
        cmd = ['rsync'] + self.options + self.exclude + src
        cmd.append(dst)
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
            return 1
    return 0

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
        import snapLvmLinux as snap
        try:
            rset.snaps = snap.snap(self, rset)
        except ex.syncNotSnapable:
            raise ex.excError
        except:
            raise

    def post_action(self, rset, action):
        """Actions to do after resourceSet has iterated through the resources to
           trigger action() on each one
        """
        import snapLvmLinux as snap
        snap.snap_cleanup(self, rset)

    def syncnodes(self):
        try:
            sync(self, "nodes")
        except (ex.syncNoNodesToSync, ex.syncNoFilesToSync):
            pass
        except:
            raise

    def syncdrp(self):
        try:
            sync(self, "drpnodes")
        except (syncNoNodesToSync, syncNoFilesToSync):
            pass
        except:
            raise

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

