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
    elif rcEnv.sysname in ['OSF1']:
        return __import__('snapAdvfsOSF1')
    else:
        raise ex.excError

def get_timestamp_filename(self, node):
    sync_timestamp_d = os.path.join(rcEnv.pathvar, 'sync', node)
    sync_timestamp_f = os.path.join(sync_timestamp_d, self.svc.svcname+'!'+self.rid)
    return sync_timestamp_f

def add_sudo_rsync_path(options):
    if "--rsync-path" not in " ".join(options):
        options += ['--rsync-path', 'sudo rsync']
        return options

    new = []
    skip = False
    for i, w in enumerate(options):
        if skip:
            skip = False
            continue
        if w.startswith('--rsync-path'):
            if "=" in w:
                l = w.split("=")
                if len(l) == 2:
                    val = l[1]
            elif len(options) > i+1:
                val = options[i+1]
                skip = True
            else:
                raise ex.excError("malformed --rsync-path value")
            if not "sudo " in val:
                val = val.strip("'")
                val = val.strip('"')
                val = "sudo "+val
            new += ['--rsync-path', val]
        else:
            new.append(w)
    return new

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

        if self.skip or self.is_disabled():
            return set([])

        """ DRP nodes are not allowed to sync nodes nor drpnodes
        """
        if rcEnv.nodename in self.svc.drpnodes:
            return set([])

        """ Refuse to sync from a flex non-primary node
        """
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            if not self.svc.cron:
                self.log.info("won't sync this resource from a flex non-primary node")
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
        s = self.svc.group_status(excluded_groups=set(["sync", "hb", "app"]))
        if not self.svc.force and \
           s['overall'].status not in [rcStatus.UP, rcStatus.NA] and \
           self.rid != "sync#i1":
            if s['overall'].status == rcStatus.WARN:
                if not self.svc.cron:
                    self.log.info("won't sync this resource service in warn status")
            if not self.svc.cron:
                self.log.info("won't sync this resource for a service not up")
            return set([])

        for node in targets.copy():
            if not status and not self.remote_node_type(node, target):
                targets -= set([node])
                continue
            if not status and not self.remote_fs_mounted(node):
                targets -= set([node])
                continue

        if len(targets) == 0:
            return set([])

        return targets

    def bwlimit_option(self):
        if self.bwlimit is not None:
            bwlimit = [ '--bwlimit='+str(self.bwlimit) ]
        elif self.svc.bwlimit is not None:
            bwlimit = [ '--bwlimit='+str(self.svc.bwlimit) ]
        else:
            bwlimit = []
        return bwlimit

    def mangle_options(self, ruser):
        if ruser != "root":
            options = add_sudo_rsync_path(self.options)
        else:
            options = self.options
        options += self.bwlimit_option()
        if '-e' in options:
            return options

        if rcEnv.rsh.startswith("/usr/bin/ssh") and rcEnv.sysname == "SunOS":
            # SunOS "ssh -n" doesn't work with rsync
            rsh = rcEnv.rsh.replace("-n", "")
        else:
            rsh = rcEnv.rsh
        options += ['-e', rsh]
        return options

    def sync_timestamp(self, node):
        sync_timestamp_f = get_timestamp_filename(self, node)
        sync_timestamp_d = os.path.dirname(sync_timestamp_f)
        sync_timestamp_d_src = os.path.join(rcEnv.pathvar, 'sync', rcEnv.nodename)
        sync_timestamp_f_src = os.path.join(sync_timestamp_d_src, self.svc.svcname+'!'+self.rid)
        sched_timestamp_f = os.path.join(rcEnv.pathvar, '_'.join(('last_sync', self.svc.svcname, self.rid)))
        if not os.path.isdir(sync_timestamp_d):
            os.makedirs(sync_timestamp_d, 0o755)
        if not os.path.isdir(sync_timestamp_d_src):
            os.makedirs(sync_timestamp_d_src, 0o755)
        with open(sync_timestamp_f, 'w') as f:
            f.write(str(self.svc.action_start_date)+'\n')
        import shutil
        shutil.copy2(sync_timestamp_f, sync_timestamp_d_src)
        shutil.copy2(sync_timestamp_f, sched_timestamp_f)
        ruser = self.svc.node.get_ruser(node)
        options = self.mangle_options(ruser)
        cmd = ['rsync'] + options
        cmd += ['-R', sync_timestamp_f, sync_timestamp_f_src, ruser+'@'+node+':/']
        self.call(cmd)

    def sync(self, target):
        if target not in self.target.keys():
            if not self.svc.cron:
                self.log.info('%s => %s sync not applicable to %s'%(self.src, self.dst, target))
            return 0

        targets = self.nodes_to_sync(target)

        if len(targets) == 0:
            if not self.svc.cron:
                self.log.info("no nodes to sync")
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
            if not self.svc.cron:
                self.log.info("no files to sync")
            raise ex.syncNoFilesToSync

        for node in targets:
            ruser = self.svc.node.get_ruser(node)
            dst = ruser + '@' + node + ':' + self.dst
            options = self.mangle_options(ruser)
            cmd = ['rsync'] + options + src
            cmd.append(dst)
            if self.rid.startswith("sync#i"):
                (ret, out, err) = self.call(cmd)
            else:
                (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
                continue
            self.sync_timestamp(node)
            self.svc.need_postsync |= set([node])
        return

    def pre_action(self, rset, action):
        """Actions to do before resourceSet iterates through the resources to
           trigger action() on each one
        """

        """Don't sync PRD services when running on !PRD node
        """
        if self.svc.svctype == 'PRD' and rcEnv.host_mode != 'PRD':
            if not self.svc.cron:
                self.log.info("won't sync a PRD service running on a !PRD node")
            raise ex.excAbortAction

        """ Is there at least one node to sync ?
        """
        targets = set([])
        rtargets = {0: set([])}
        need_snap = False
        for i, r in enumerate(rset.resources):
            if self.skip or r.is_disabled():
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
            if not self.svc.cron:
                self.log.info("no node to sync")
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
            if not self.svc.cron:
                self.log.info("no file to sync")
            pass
        except ex.syncNoNodesToSync:
            if not self.svc.cron:
                self.log.info("no node to sync")
            pass

    def syncdrp(self):
        try:
            self.sync("drpnodes")
        except ex.syncNoFilesToSync:
            if not self.svc.cron:
                self.log.info("no file to sync")
            pass
        except ex.syncNoNodesToSync:
            if not self.svc.cron:
                self.log.info("no node to sync")
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
        s = self.svc.group_status(excluded_groups=set(["sync", "hb", "app"]))
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

    def __init__(self,
                 rid=None,
                 src=[],
                 dst=None,
                 options=[],
                 target={},
                 dstfs=None,
                 snap=False,
                 bwlimit=None,
                 sync_max_delay=None,
                 sync_interval=None,
                 sync_days=None,
                 sync_period=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.rsync",
                              sync_max_delay=sync_max_delay,
                              sync_interval=sync_interval,
                              sync_days=sync_days,
                              sync_period=sync_period,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        if internal:
            if rcEnv.drp_path in dst:
                self.label = "rsync system files to drpnodes"
            else:
                self.label = "rsync svc config to %s"%(', '.join(target.keys()))
        else:
            _src = ', '.join(src)
            if len(_src) > 300:
                _src = _src[0:300]
            _dst = ', '.join(target.keys())
            self.label = "rsync %s to %s"%(_src, _dst)
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

