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
import datetime
import resSync

def lookup_snap_mod():
    if rcEnv.sysname == 'Linux':
        return __import__('snapLvmLinux')
    elif rcEnv.sysname == 'HP-UX':
        return __import__('snapVxfsHP-UX')
    elif rcEnv.sysname == 'AIX':
        return __import__('snapJfs2AIX')
    elif rcEnv.sysname in ['SunOS', 'FreeBSD']:
        return __import__('snapZfsSunOS')
    else:
        raise ex.excError

def remote_fs_mounted(self, node):
    """Verify the remote fs is mounted before we send data.
    """
    if self.dstfs is None:
        """No check has been configured. Assume the admin knows better.
        """
        return True
    cmd = rcEnv.rsh.split(' ')+[node, '--', 'df', self.dstfs]
    (ret, out) = self.call(cmd, cache=True)
    if ret != 0:
        raise ex.excError
    if self.dstfs not in out.split():
        self.log.error("The destination fs %s is not mounted on node %s. refuse to sync %s to protect parent fs"%(self.dstfs, node, self.dst))
        return False
    return True

cache_remote_node_type = {}

def remote_node_type(self, node, target):
    if target == 'drpnodes':
        expected_type = list(set(rcEnv.allowed_svctype) - set(['PRD']))
    elif target == 'nodes':
        expected_type = [self.svc.svctype]
    else:
        self.log.error('unknown sync target: %s'%target)
        raise ex.excError

    rcmd = [os.path.join(rcEnv.pathbin, 'nodemgr'), 'get', '--param', 'node.host_mode']

    if node not in cache_remote_node_type:
        cmd = rcEnv.rsh.split(' ')+[node, '--'] + rcmd
        (ret, out) = self.call(cmd, cache=True)
        if ret != 0:
            raise ex.excError
        words = out.split()
        if len(words) == 1:
            cache_remote_node_type[node] = words[0]
        else:
            cache_remote_node_type[node] = out

    if cache_remote_node_type[node] in expected_type:
        return True
    self.log.error("incompatible remote node '%s' host mode: '%s' (expected in %s)"%\
                   (node, cache_remote_node_type[node], str(expected_type)))
    return False

def get_timestamp_filename(self, node):
    sync_timestamp_d = os.path.join(rcEnv.pathvar, 'sync', node)
    sync_timestamp_f = os.path.join(sync_timestamp_d, self.svc.svcname+'!'+self.rid)
    return sync_timestamp_f

def sync_timestamp(self, node):
    sync_timestamp_f = get_timestamp_filename(self, node)
    sync_timestamp_d = os.path.dirname(sync_timestamp_f)
    sync_timestamp_d_src = os.path.join(rcEnv.pathvar, 'sync', rcEnv.nodename)
    sync_timestamp_f_src = os.path.join(sync_timestamp_d_src, self.svc.svcname+'!'+self.rid)
    if not os.path.isdir(sync_timestamp_d):
        os.makedirs(sync_timestamp_d ,0755)
    if not os.path.isdir(sync_timestamp_d_src):
        os.makedirs(sync_timestamp_d_src ,0755)
    with open(sync_timestamp_f, 'w') as f:
        f.write(str(datetime.datetime.now())+'\n')
        f.close()
    import shutil
    shutil.copy2(sync_timestamp_f, sync_timestamp_d_src)
    cmd = ['rsync'] + self.options + bwlimit_option(self) + ['-R', sync_timestamp_f, sync_timestamp_f_src, node+':/']
    self.vcall(cmd)

def get_timestamp(self, node):
    ts = None
    sync_timestamp_f = get_timestamp_filename(self, node)
    if not os.path.exists(sync_timestamp_f):
        return None
    try:
        with open(sync_timestamp_f, 'r') as f:
            d = f.read()
            ts = datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n")
            f.close()
    except:
        self.log.info("failed get last sync date for %s to %s"%(self.src, node))
        return ts
    return ts

def bwlimit_option(self):
    if self.bwlimit is not None:
        bwlimit = [ '--bwlimit='+self.bwlimit ]
    elif self.svc.bwlimit is not None:
        bwlimit = [ '--bwlimit='+self.svc.bwlimit ]
    else:
        bwlimit = []
    return bwlimit

class Rsync(resSync.Sync):
    """Defines a rsync job from local node to its remote nodes. Target nodes
    can be restricted to production sibblings or to disaster recovery nodes,
    or both.
    """
    def node_can_sync(self, node):
        ts = get_timestamp(self, node)
        return not self.skip_sync(ts)

    def node_need_sync(self, node):
        ts = get_timestamp(self, node)
        return self.alert_sync(ts)

    def can_sync(self, target=None):
        targets = set([])
        if target is None:
            targets = self.nodes_to_sync('nodes')
            targets |= self.nodes_to_sync('drpnodes')
        else:
            targets = self.nodes_to_sync(target)

        if len(targets) == 0:
            return False
        return True

    def nodes_to_sync(self, target=None, state="syncable", status=False):
        """ Checks are ordered by cost
        """

        """ DRP nodes are not allowed to sync nodes nor drpnodes
        """
        if rcEnv.nodename in self.svc.drpnodes:
            return set([])

        """ Refuse to sync from a flex non-primary node
        """
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            self.log.debug("won't sync this resource from a flex non-primary node")
            return set([])

        """Discard the local node from the set
        """
        if target in self.target.keys():
            targets = self.target[target].copy()
        else:
            return set([])

        targets -= set([rcEnv.nodename])
        if len(targets) == 0:
            return set([])

        for node in targets.copy():
            if state == "syncable" and not self.node_can_sync(node):
                targets -= set([node])
                continue
            elif state == "late" and not self.node_need_sync(node):
                targets -= set([node])
                continue

        if len(targets) == 0:
            return set([])

        """Accept to sync from here only if the service is up
           Also accept n/a status, because it's what the overall status
           ends up to be when only sync#* are specified using --rid

           sync#i1 is an exception, because we want all prd nodes to
           sync their system files to all drpnodes regardless of the service
           state
        """
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if s['overall'].status not in [rcStatus.UP, rcStatus.NA] and \
           self.rid != "sync#i1":
            if s['overall'].status == rcStatus.WARN:
                self.log.debug("won't sync this resource service in warn status")
            self.log.debug("won't sync this resource for a service not up")
            return set([])

        for node in targets.copy():
            if not status and not remote_node_type(self, node, target):
                targets -= set([node])
                continue
            if not status and not remote_fs_mounted(self, node):
                targets -= set([node])
                continue

        if len(targets) == 0:
            return set([])

        return targets

    def sync(self, target):
        if target not in self.target.keys():
            self.log.debug('%s => %s sync not applicable to %s'%(self.src, self.dst, target))
            return 0

        targets = self.nodes_to_sync(target)

        if len(targets) == 0:
            self.log.debug("no nodes to sync")
            raise ex.syncNoNodesToSync

        if "delay_snap" in self.tags:
            if not hasattr(self.rset, 'snaps'):
                Snap = lookup_snap_mod()
                self.rset.snaps.log = self.log
                self.rset.snaps = Snap.Snap(self.rid)
            self.rset.snaps.try_snap(self.rset, target, rid=self.rid)

        if hasattr(self, "alt_src"):
            """ The pre_action() has provided us with a better source
                to sync from. Use that
            """
            src = self.alt_src
        else:
            src = self.src

        if len(src) == 0:
            self.log.debug("no files to sync")
            raise ex.syncNoFilesToSync

        bwlimit = bwlimit_option(self)

        for node in targets:
            dst = node + ':' + self.dst
            cmd = ['rsync'] + self.options + bwlimit + src
            cmd.append(dst)
            (ret, out) = self.vcall(cmd)
            if ret != 0:
                self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
                continue
            sync_timestamp(self, node)
            self.svc.need_postsync |= set([node])
        return

    def pre_action(self, rset, action):
        """Actions to do before resourceSet iterates through the resources to
           trigger action() on each one
        """

        """Don't sync PRD services when running on !PRD node
        """
        if self.svc.svctype == 'PRD' and rcEnv.host_mode != 'PRD':
            self.log.debug("won't sync a PRD service running on a !PRD node")
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
            if action == "syncnodes":
                rtargets[i] |= r.nodes_to_sync('nodes')
            else:
                rtargets[i] |= r.nodes_to_sync('drpnodes')
            for node in rtargets[i].copy():
                if not r.node_can_sync(node):
                    rtargets[i] -= set([node])
                elif r.snap:
                    need_snap = True
        for i in rtargets:
            targets |= rtargets[i]

        if len(targets) == 0:
            self.log.debug("no node to sync")
            raise ex.excAbortAction

        if not need_snap:
            self.log.debug("snap not needed")
            return

        Snap = lookup_snap_mod()
        try:
            rset.snaps = Snap.Snap(self.rid)
            rset.snaps.log = self.log
            rset.snaps.try_snap(rset, action)
        except ex.syncNotSnapable:
            raise ex.excError

    def post_action(self, rset, action):
        """Actions to do after resourceSet has iterated through the resources to
           trigger action() on each one
        """
        if hasattr(rset, 'snaps'):
            rset.snaps.snap_cleanup(rset)

    def syncnodes(self):
        try:
            self.sync("nodes")
        except ex.syncNoFilesToSync:
            self.log.debug("no file to sync")
            pass
        except ex.syncNoNodesToSync:
            self.log.debug("no node to sync")
            pass

    def syncdrp(self):
        try:
            self.sync("drpnodes")
        except ex.syncNoFilesToSync:
            self.log.debug("no file to sync")
            pass
        except ex.syncNoNodesToSync:
            self.log.debug("no node to sync")
            pass

    def _status(self, verbose=False):
        """ mono-node service should return n/a as a sync state
        """
        target = set([])
        for i in self.target:
            target |= self.target[i]
        if len(target - set([rcEnv.nodename])) == 0:
            self.status_log("no destination nodes")
            return rcStatus.NA

        """ sync state on nodes where the service is not UP
        """
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if s['overall'].status != rcStatus.UP or \
           (self.svc.clustertype in ['flex', 'autoflex'] and \
            rcEnv.nodename != self.svc.flex_primary and \
            s['overall'].status == rcStatus.UP):
            if rcEnv.nodename not in target:
                self.status_log("passive node not in sync destination nodes")
                return rcStatus.NA
            if self.node_need_sync(rcEnv.nodename):
                self.status_log("passive node needs update")
                return rcStatus.WARN
            else:
                return rcStatus.UP

        """ sync state on DRP nodes where the service is UP
        """
        if 'drpnodes' in self.target and rcEnv.nodename in self.target['drpnodes']:
            self.status_log("service up on drp node, sync disabled")
            return rcStatus.NA

        """ sync state on nodes where the service is UP
        """
        nodes = []
        nodes += self.nodes_to_sync('nodes', state="late", status=True)
        nodes += self.nodes_to_sync('drpnodes', state="late", status=True)
        if len(nodes) == 0:
            return rcStatus.UP

        self.status_log("%s need update"%', '.join(nodes))
        return rcStatus.DOWN

    def __init__(self, rid=None, src=[], dst=None, options=[], target={}, dstfs=None, snap=False,
                 bwlimit=None, sync_max_delay=None, sync_interval=None,
                 sync_days=None, sync_period=None,
                 optional=False, disabled=False, tags=set([]), internal=False):
        resSync.Sync.__init__(self, rid=rid, type="sync.rsync",
                              sync_max_delay=sync_max_delay,
                              sync_interval=sync_interval,
                              sync_days=sync_days,
                              sync_period=sync_period,
                              optional=optional, disabled=disabled, tags=tags)

        if internal:
            if rcEnv.drp_path in dst:
                self.label = "rsync system files to drpnodes"
            else:
                self.label = "rsync svc config to %s"%(', '.join(target.keys()))
        else:
            self.label = "rsync %s to %s"%(', '.join(src),
                                        ', '.join(target.keys()))
        self.src = src
        self.dst = dst
        self.dstfs = dstfs
        self.snap = snap
        self.target = target
        self.bwlimit = bwlimit
        self.internal = internal
        self.timeout = 3600
        self.options = ['-HpogDtrlvx',
                        '--stats',
                        '--delete',
                        '--force',
                        '--timeout='+str(self.timeout)]
        self.options += options

    def __str__(self):
        return "%s src=%s dst=%s options=%s target=%s" % (resSync.Sync.__str__(self),\
                self.src, self.dst, self.options, str(self.target))

