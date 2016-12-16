import os
import logging

import rcExceptions as ex
import resources as Res
import datetime
import time
import rcStatus
from rcGlobalEnv import rcEnv
from rcScheduler import *

cache_remote_node_env = {}

class Sync(Res.Resource, Scheduler):
    def __init__(self,
                 rid=None,
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 type=type,
                 subset=None):
        if sync_max_delay is None:
            self.sync_max_delay = 1500
        else:
            self.sync_max_delay = sync_max_delay

        if schedule is None:
            self.schedule = "03:59-05:59@121"
        else:
            self.schedule = schedule

        Res.Resource.__init__(self,
                              rid=rid,
                              type=type,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

    def can_sync(self, target):
        return True

    def check_timestamp(self, ts, comp='more', delay=10):
        """ Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval
        """
        if delay == 0:
            raise
        limit = ts + datetime.timedelta(minutes=delay)
        if comp == "more" and datetime.datetime.now() < limit:
            return False
        elif comp == "less" and datetime.datetime.now() < limit:
            return False
        else:
            return True
        return True

    def skip_sync(self, ts):
        if not self.svc.options.cron:
            return False
        if self.skip_action_schedule(self.rid, "sync_schedule", last=ts):
            return True
        return False

    def alert_sync(self, ts):
        if ts is None:
            return True
        if not self.check_timestamp(ts, comp="less", delay=self.sync_max_delay):
            return False
        return True

    def remote_fs_mounted(self, node):
        """
        Verify the remote fs is mounted. Some sync resource might want to abort in
        this case.
        """
        if self.dstfs is None:
            # No dstfs check has been configured. Assume the admin knows better.
            return True
        ruser = self.svc.node.get_ruser(node)
        cmd = rcEnv.rsh.split(' ')+['-l', ruser, node, '--', 'LANG=C', 'df', self.dstfs]
        (ret, out, err) = self.call(cmd, cache=True, errlog=False)
        if ret != 0:
            raise ex.excError

        """
        # df /zones
        /zones             (rpool/zones       ):131578197 blocks 131578197 files
               ^
               separator !

        # df /zones/frcp03vrc0108/root
        /zones/frcp03vrc0108/root(rpool/zones/frcp03vrc0108/rpool/ROOT/solaris-0):131578197 blocks 131578197 files
                                 ^
                                 no separator !
        """
        if self.dstfs+'(' not in out and self.dstfs not in out.split():
            self.log.error("The destination fs %s is not mounted on node %s. refuse to sync %s to protect parent fs"%(self.dstfs, node, self.dst))
            return False
        return True

    def remote_node_env(self, node, target):
        if target == 'drpnodes':
            expected_type = list(set(rcEnv.allowed_svc_envs) - set(['PRD']))
        elif target == 'nodes':
            if self.svc.svc_env == "PRD":
                expected_type = ["PRD"]
            else:
                expected_type = list(set(rcEnv.allowed_svc_envs) - set(["PRD"]))
        else:
            self.log.error('unknown sync target: %s'%target)
            raise ex.excError

        ruser = self.svc.node.get_ruser(node)
        rcmd = [rcEnv.nodemgr, 'get', '--param', 'node.env']
        if ruser != "root":
            rcmd = ['sudo'] + rcmd

        if node not in cache_remote_node_env:
            cmd = rcEnv.rsh.split(' ')+['-l', ruser, node, '--'] + rcmd
            (ret, out, err) = self.call(cmd, cache=True)
            if ret != 0:
                return False
            words = out.split()
            if len(words) == 1:
                cache_remote_node_env[node] = words[0]
            else:
                cache_remote_node_env[node] = out

        if cache_remote_node_env[node] in expected_type:
            return True
        self.log.error("incompatible remote node '%s' env: '%s' (expected in %s)"%\
                       (node, cache_remote_node_env[node], ', '.join(expected_type)))
        return False

    def pre_sync_check_svc_not_up(self):
        if self.svc.options.force:
            self.log.info("skip service up status check because --force is set")
        else:
            s = self.svc.group_status(excluded_groups=set(["sync", "hb", "app"]))
            if s['overall'].status != rcStatus.UP:
                if self.svc.options.cron:
                    self.log.debug("won't sync this resource for a service not up")
                else:
                    self.log.info("won't sync this resource for a service not up")
                raise ex.excAbortAction

    def pre_sync_check_flex_primary(self):
        """ Refuse to sync from a flex non-primary node
        """
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            if self.svc.options.cron:
                self.log.debug("won't sync this resource from a flex non-primary node")
            else:
                self.log.info("won't sync this resource from a flex non-primary node")
            raise ex.excAbortAction

    def pre_sync_check_prd_svc_on_non_prd_node(self):
        if self.svc.svc_env == 'PRD' and rcEnv.node_env != 'PRD':
            if self.svc.options.cron:
                self.log.debug("won't sync a PRD service running on a !PRD node")
            else:
                self.log.info("won't sync a PRD service running on a !PRD node")
            raise ex.excAbortAction

