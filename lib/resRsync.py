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
import datetime

def lookup_snap_mod():
    if rcEnv.sysname == 'Linux':
        return __import__('snapLvmLinux')
    elif rcEnv.sysname == 'HP-UX':
        return __import__('snapVxfsHP-UX')
    else:
        raise ex.excError

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

cache_remote_node_type = {}

def remote_node_type(self, node, type):
    if type == 'drpnodes':
        expected_type = 'DEV'
    elif type == 'nodes':
        expected_type = self.svc.svctype
    else:
        self.log.error('expected remote node type is bogus: %s'%type)
        raise

    if node not in cache_remote_node_type:
        host_mode_f = os.path.join(rcEnv.pathvar, 'host_mode') 
        cmd = rcEnv.rsh.split(' ')+[node, '--', 'LANG=C', 'cat', host_mode_f]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        cache_remote_node_type[node] = out.split()[0]

    if cache_remote_node_type[node] == expected_type:
        return True
    self.log.error("node %s type is not '%s'. Check %s:%s"%\
                   (node, expected_type, node, host_mode_f))
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
        if not need_sync(self, node):
            targets -= set([node])

    if len(targets) == 0:
        raise ex.syncNoNodesToSync

    return targets

def get_timestamp_filename(self, node):
    sync_timestamp_d = os.path.join(rcEnv.pathvar, 'sync', node)
    sync_timestamp_f = os.path.join(sync_timestamp_d, self.svc.svcname+'!'+self.dst.replace('/', '_'))
    return sync_timestamp_f

def sync_timestamp(self, node):
    sync_timestamp_f = get_timestamp_filename(self, node)
    sync_timestamp_d = os.path.dirname(sync_timestamp_f)
    if not os.path.isdir(sync_timestamp_d):
        os.makedirs(sync_timestamp_d ,0755)
    with open(sync_timestamp_f, 'w') as f:
        f.write(str(datetime.datetime.now())+'\n')
        f.close()

def need_sync(self, node):
    sync_timestamp_f = get_timestamp_filename(self, node)
    if not os.path.exists(sync_timestamp_f):
        return True
    try:
        with open(sync_timestamp_f, 'r') as f:
            d = f.read()
            if datetime.datetime.now() < datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n") + datetime.timedelta(minutes=self.sync_min_delay):
                return False
            f.close()
    except:
        self.log.info("failed to determine last sync date for %s to %s"%(self.src, node))
        return True
    return True

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

    if self.bwlimit is not None:
        bwlimit = [ '--bwlimit='+self.bwlimit ]
    elif self.svc.bwlimit is not None:
        bwlimit = [ '--bwlimit='+self.svc.bwlimit ]
    else:
        bwlimit = []

    for node in targets:
        if not need_sync(self, node):
            self.log.debug("skip sync of %s to %s because last sync too close"%(self.src, node))
            continue
        if not remote_fs_mounted(self, node):
            continue
        dst = node + ':' + self.dst
        cmd = ['rsync'] + self.options + bwlimit + self.exclude + src
        cmd.append(dst)
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
            continue
        sync_timestamp(self, node)
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
        rtargets = {0: set([])}
        need_snap = False
        for i, r in enumerate(rset.resources):
            if r.is_disabled():
                continue
            rtargets[i] = set([])
            try:
                if action == "syncnodes":
                    rtargets[i] |= nodes_to_sync(r, 'nodes')
                else:
                    rtargets[i] |= nodes_to_sync(r, 'drpnodes')
            except ex.syncNoNodesToSync:
                pass
            for node in rtargets[i].copy():
                if not need_sync(r, node):
                    rtargets[i] -= set([node])
                elif r.snap:
                    need_snap = True
        for i in rtargets:
            targets |= rtargets[i]

        if len(targets) == 0:
            self.log.debug("no node to sync")
            raise ex.excAbortAction

        if not need_snap:
            return

        snap = lookup_snap_mod()
        try:
            rset.snaps = snap.snap(self, rset, action)
        except ex.syncNotSnapable:
            raise ex.excError

    def post_action(self, rset, action):
        """Actions to do after resourceSet has iterated through the resources to
           trigger action() on each one
        """
        snap = lookup_snap_mod()
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
                 bwlimit=None, sync_min_delay=30, optional=False, disabled=False, internal=False):
        self.src = src
        self.dst = dst
        self.dstfs = dstfs
        self.exclude = exclude
        self.snap = snap
        self.target = target
        self.bwlimit = bwlimit
        self.internal = internal
        self.sync_min_delay = sync_min_delay
        Res.Resource.__init__(self, "rsync", optional, disabled)

    def __str__(self):
        return "%s src=%s dst=%s exclude=%s target=%s" % (Res.Resource.__str__(self),\
                self.src, self.dst, self.exclude, str(self.target))

