#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
from rcGlobalEnv import rcEnv
import datetime
from subprocess import *
import rcExceptions as ex
import rcStatus
import resources as Res
from rcZfs import a2pool_dataset

class SyncZfs(Res.Resource):
    """define zfs sync resource to be zfs send/zfs receive between nodes
    """
    def __init__(self, rid=None, target=None, src=None, dst=None,
                 delta_store=None, sender=None, recursive = True,
                 snap_size=0, sync_max_delay=1450, sync_min_delay=30,
                 optional=False, disabled=False, tags=set([])):
        self.label = "zfs of %s to %s"%(src, target)
        self.target = target
        self.sender = sender
        self.recursive = recursive
        (self.src_pool, self.src_ds) = a2pool_dataset(src)
        (self.dst_pool, self.dst_ds) = a2pool_dataset(dst)
        self.sync_max_delay = sync_max_delay
        self.sync_min_delay = sync_min_delay
        if delta_store is None:
            self.delta_store = rcEnv.pathvar
        else:
            self.delta_store = delta_store
        Res.Resource.__init__(self, rid, "sync.zfs", optional=optional, disabled=disabled, tags=tags)

    def __str__(self):
        return "%s target=%s src=%s" % (Res.Resource.__str__(self),\
                self.target, self.src)

    def snap_exists(self, snapname, node=None):
        cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'list', '-t', 'snapshot', snapname]
        if node is not None:
            cmd = rcEnv.rsh.split() + [node] + cmd
        (ret, out) = self.call(cmd, errlog=False)
        if ret == 0:
            return True
        else:
            return False

    def create_snap(self, snap):
        if self.snap_exists(snap):
            self.log.error('%s should not exist'%snap)
            raise ex.excError
        if self.recursive :
            cmd = ['zfs', 'snapshot' , '-r' , snap ]
        else:
            cmd = ['zfs', 'snapshot' , snap ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def get_src_info(self):
        self.src_snap_sent = self.src_ds + '@sent'
        self.src_snap_tosend = self.src_ds + '@tosend'

    def get_dst_info(self):
        self.dst_snap_sent = self.dst_ds + '@sent'
        self.dst_snap_tosend = self.dst_ds + '@tosend'

    def get_peersenders(self):
        self.peersenders = set([])
        if 'nodes' == self.sender:
            self.peersenders |= self.svc.nodes
            self.peersenders -= set([rcEnv.nodename])

    def get_targets(self):
        self.targets = set()
        if 'nodes' in self.target:
            self.targets |= self.svc.nodes
        if 'drpnodes' in self.target:
            self.targets |= self.svc.drpnodes
        self.targets -= set([rcEnv.nodename])

    def get_info(self):
        self.get_targets()
        self.get_src_info()
        self.get_dst_info()

    def syncnodes(self):
        self.syncupdate()

    def syncfullsync(self):
        self.syncupdate()

    def do_update(self, node):
        if self.recursive :  
            send_cmd = ['zfs', 'send', '-R', '-i',
                            self.src_snap_sent, self.src_snap_tosend]
        else:
            send_cmd = ['zfs', 'send', '-i',
                            self.src_snap_sent, self.src_snap_tosend]

	receive_cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'receive', '-dF', self.dst_pool]
        if node is not None:
            receive_cmd = rcEnv.rsh.strip(' -n').split() + [node] + receive_cmd

        self.log.info(' '.join(send_cmd + ["|"] + receive_cmd))
        p1 = Popen(send_cmd, stdout=PIPE)
        p2 = Popen(receive_cmd, stdin=p1.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode != 0:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("sync update failed")
            raise ex.excError
        if buff[0] is not None and len(buff[0]) > 0:
            self.log.info(buff[0])

    def do_fullsync(self, node=None):
        if self.recursive :  
            send_cmd = ['zfs', 'send', '-R', self.src_snap_tosend]
        else:
            send_cmd = ['zfs', 'send', self.src_snap_tosend]

	receive_cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'receive', '-dF', self.dst_pool ]
        if node is not None:
            receive_cmd = rcEnv.rsh.strip(' -n').split() + [node] + receive_cmd

        self.log.info(' '.join(send_cmd + ["|"] + receive_cmd))
        p1 = Popen(send_cmd, stdout=PIPE)
        p2 = Popen(receive_cmd, stdin=p1.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode != 0:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("full sync failed")
            raise ex.excError
        if buff[0] is not None and len(buff[0]) > 0:
            self.log.info(buff[0])

    def remove_snap(self, snap, node=None):
        if not self.snap_exists(snap, node=node):
            return
        if self.recursive :
            cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'destroy', '-r', snap]
        else:
            cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'destroy', snap]
        if node is not None:
            cmd = rcEnv.rsh.split() + [ node ] + cmd
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def rename_snap(self, src, dst,  node=None):
        if self.snap_exists(dst, node):
            self.log.error("%s should not exist"%dst)
            raise ex.excError
        if self.recursive :
            cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'rename', '-r', src, dst]
        else:
            cmd = ['env', 'PATH=/usr/sbin:/sbin', 'zfs', 'rename', src, dst]

        if node is not None:
            cmd = rcEnv.rsh.split() + [ node ] + cmd
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def rotate_snaps(self, src, dst, node=None):
        self.remove_snap(dst, node)
        self.rename_snap(src, dst, node)

    def syncupdate(self):
        s = self.svc.group_status(excluded_groups=set(["sync"]))
        if s['overall'].status != rcStatus.UP:
            self.log.debug("won't sync this resource for a service not up")
            return
        self.get_info()
        if not self.snap_exists(self.src_snap_tosend):
            self.create_snap(self.src_snap_tosend)
        if self.snap_exists(self.src_snap_sent):
            for n in self.targets:
                self.do_update(n)
                self.rotate_snaps(self.dst_snap_tosend, self.dst_snap_sent, n)
        else:
            for n in self.targets:
                self.do_fullsync(n)
                self.rotate_snaps(self.dst_snap_tosend, self.dst_snap_sent, n)
        self.rotate_snaps(self.src_snap_tosend, self.src_snap_sent)
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)

    def start(self):
        pass

    def stop(self):
        pass

    def _status(self, verbose=False):
        try:
            ls = self.get_local_state()
            now = datetime.datetime.now()
            last = datetime.datetime.strptime(ls['date'], "%Y-%m-%d %H:%M:%S.%f")
            delay = datetime.timedelta(minutes=self.sync_max_delay)
        except IOError:
            self.status_log("zfs state file not found")
            return rcStatus.WARN
        except:
            import sys
            import traceback
            e = sys.exc_info()
            print e[0], e[1], traceback.print_tb(e[2])
            return rcStatus.WARN
        if last < now - delay:
            self.status_log("Last sync on %s older than %i minutes"%(last, self.sync_max_delay))
            return rcStatus.WARN
        return rcStatus.UP

    def check_remote(self, node):
        rs = self.get_remote_state(node)
        if self.snap1_uuid != rs['uuid']:
            self.log.error("%s last update uuid doesn't match snap1 uuid"%(node))
            raise ex.excError

    def get_remote_state(self, node):
        self.set_statefile()
        cmd1 = ['env', 'LANG=C', 'cat', self.statefile]
        cmd = rcEnv.rsh.split() + [node] + cmd1
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("could not fetch %s last update uuid"%node)
            raise ex.excError
        return self.parse_statefile(out, node=node)

    def get_local_state(self):
        self.set_statefile()
        with open(self.statefile, 'r') as f:
            out = f.read()
        return self.parse_statefile(out)

    def get_snap_uuid(self, snap):
        cmd = ['zfs', 'list', '-H', '-o', 'creation', snap]
        (ret, out) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        self.snap_uuid = out.strip()

    def set_statefile(self):
        self.statefile = os.path.join(rcEnv.pathvar,
                                      self.svc.svcname+'_'+self.rid+'_zfs_state')

    def write_statefile(self):
        self.set_statefile()
        self.get_snap_uuid(self.src_snap_sent)
        self.log.info("update state file with snap uuid %s"%self.snap_uuid)
        with open(self.statefile, 'w') as f:
             f.write(str(datetime.datetime.now())+';'+self.snap_uuid+'\n')

    def _push_statefile(self, node):
        cmd = rcEnv.rcp.split() + [self.statefile, node+':'+self.statefile]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def push_statefile(self, node):
        self.set_statefile()
        self._push_statefile(node)
        self.get_peersenders()
        for s in self.peersenders:
            self._push_statefile(s)

    def parse_statefile(self, out, node=None):
        self.set_statefile()
        if node is None:
            node = rcEnv.nodename
        lines = out.strip().split('\n')
        if len(lines) != 1:
            self.log.error("%s:%s is corrupted"%(node, self.statefile))
            raise ex.excError
        fields = lines[0].split(';')
        if len(fields) != 2:
            self.log.error("%s:%s is corrupted"%(node, self.statefile))
            raise ex.excError
        return dict(date=fields[0], uuid=fields[1])

