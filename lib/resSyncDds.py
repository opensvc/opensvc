import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which
from rcUtilitiesLinux import lv_info
from subprocess import *
import rcExceptions as ex
import rcStatus
import datetime
import resSync

class syncDds(resSync.Sync):
    def pre_action(self, rset, action):
        resources = [ r for r in rset.resources if not r.skip and not r.is_disabled() ]

        if len(resources) == 0:
            return

        """Don't sync PRD services when running on !PRD node
        """
        if self.svc.svctype == 'PRD' and rcEnv.host_mode != 'PRD':
            self.log.debug("won't sync a PRD service running on a !PRD node")
            raise ex.excAbortAction

        for i, r in enumerate(resources):
            if not r.svc_syncable():
                return
            r.get_info()
            if action == 'sync_full':
                r.create_snap1()
            elif action in ['sync_update', 'sync_resync', 'sync_drp', 'sync_nodes']:
                if action == 'sync_nodes' and self.target != ['nodes']:
                    return
                if action == 'sync_drp' and self.target != ['drpnodes']:
                    return
                r.get_info()
                r.get_snap1_uuid()
                nb = 0
                tgts = r.targets.copy()
                for n in tgts:
                    try:
                        r.check_remote(n)
                        nb += 1
                    except:
                        self.targets -= set([n])
                if nb != len(tgts):
                    self.log.error('all destination nodes must be present for dds-based synchronization to proceed')
                    raise ex.excError
                r.create_snap2()

    def snap_exists(self, dev):
        if not os.path.exists(dev):
            self.log.debug('dev path does not exist')
            return False
        cmd = ['lvs', '--noheadings', '-o', 'snap_percent', dev]
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0:
            return False
        if len(out.strip()) == 0:
            self.log.debug('dev is not a snapshot')
            return False
        return True

    def create_snap(self, dev, lv):
        if self.snap_exists(dev):
            self.log.error('%s should not exist'%dev)
            raise ex.excError
        cmd = ['lvcreate', '-s', '-n', lv,
               '-L', str(self.snap_size)+'M',
               os.path.join(os.sep, 'dev', self.src_vg, self.src_lv)
              ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def set_statefile(self):
        self.statefile = os.path.join(rcEnv.pathvar,
                                      self.svc.svcname+'_'+self.rid+'_dds_state')

    def create_snap1(self):
        if self.snap_exists(self.snap2):
            self.log.error('%s should not exist'%self.snap2)
            raise ex.excError
        self.create_snap(self.snap1, self.snap1_lv)
        self.write_statefile()

    def create_snap2(self):
        self.create_snap(self.snap2, self.snap2_lv)

    def snap_name(self, snap):
        return os.path.basename(self.src_lv).replace('-', '_')+'_osvc_'+snap

    def get_src_info(self):
        (self.src_vg, self.src_lv, self.src_size) = lv_info(self, self.src)
        if self.src_lv is None:
            self.log.error("unable to fetch source logical volume information")
            raise ex.excError
        if self.snap_size == 0:
            self.snap_size = self.src_size//10
        self.snap1_lv = self.snap_name('snap1')
        self.snap2_lv = self.snap_name('snap2')
        self.snap1 = os.path.join(os.sep, 'dev', self.src_vg, self.snap1_lv)
        self.snap2 = os.path.join(os.sep, 'dev', self.src_vg, self.snap2_lv)
        self.snap1_cow = os.path.join(os.sep, 'dev', 'mapper',
                                      '-'.join([self.src_vg.replace('-', '--'),
                                                self.snap1_lv,
                                                'cow'])
                                     )

    def get_peersenders(self):
        self.peersenders = set([])
        if 'nodes' not in self.target:
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

    def svc_syncable(self):
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if s['overall'].status != rcStatus.UP:
            self.log.debug("won't sync this resource for a service not up")
            return False
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            self.log.debug("won't sync this resource from a flex non-primary node")
            return set([])

        return True

    def sync_full(self):
        if not self.svc_syncable():
            return
        for n in self.targets:
            self.do_fullsync(n)

    def do_fullsync(self, node):
        dst = self.dsts[node]
        cmd1 = ['dd', 'if='+self.snap1, 'bs=1M']
        cmd2 = rcEnv.rsh.split() + [node, 'dd', 'bs=1M', 'of='+dst]
        self.log.info(' '.join(cmd1 + ["|"] + cmd2))
        p1 = Popen(cmd1, stdout=PIPE)
        p2 = Popen(cmd2, stdin=p1.stdout, stdout=PIPE)
        p2.communicate()[0]
        if p2.returncode != 0:
            self.log.error("full sync failed")
            raise ex.excError
        self.push_statefile(node)

    def get_snap1_uuid(self):
        cmd = ['lvs', '--noheadings', '-o', 'uuid', self.snap1]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        self.snap1_uuid = out.strip()

    def write_statefile(self):
        self.set_statefile()
        self.get_snap1_uuid()
        self.log.info("update state file with snap uuid %s"%self.snap1_uuid)
        with open(self.statefile, 'w') as f:
             f.write(str(datetime.datetime.now())+';'+self.snap1_uuid+'\n')

    def _push_statefile(self, node):
        cmd = rcEnv.rcp.split() + [self.statefile, node+':'+self.statefile]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def push_statefile(self, node):
        self.set_statefile()
        self._push_statefile(node)
        self.get_peersenders()
        for s in self.peersenders:
            self._push_statefile(s)

    def apply_delta(self, node):
        if not which('dds'):
            raise ex.excError("dds executable not found")
        dst = self.dsts[node]
        extract_cmd = ['dds', '--extract', '--cow', self.snap1_cow, '--source',
                       self.snap2]
        merge_cmd = ['dds', '--merge', '--dest', dst, '-v']
        merge_cmd = rcEnv.rsh.split() + [node] + merge_cmd
        self.log.info(' '.join(extract_cmd + ["|"] + merge_cmd))
        p1 = Popen(extract_cmd, stdout=PIPE)
        p2 = Popen(merge_cmd, stdin=p1.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode != 0:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("sync update failed")
            raise ex.excError
        if buff[0] is not None and len(buff[0]) > 0:
            self.log.info(buff[0])

    def do_update(self, node):
        self.apply_delta(node)

    def remove_snap1(self):
        if not self.snap_exists(self.snap1):
            return
        cmd = ['lvremove', '-f', self.snap1]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def rename_snap2(self):
        if not self.snap_exists(self.snap2):
            self.log.error("%s should exist"%self.snap2)
            raise ex.excError
        if self.snap_exists(self.snap1):
            self.log.error("%s should not exist"%self.snap1)
            raise ex.excError
        cmd = ['lvrename', self.src_vg, self.snap2_lv, self.snap1_lv]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def rotate_snaps(self):
        self.remove_snap1()
        self.rename_snap2()

    def check_remote(self, node):
        rs = self.get_remote_state(node)
        if self.snap1_uuid != rs['uuid']:
            self.log.error("%s last update uuid doesn't match snap1 uuid"%(node))
            raise ex.excError

    def get_remote_state(self, node):
        self.set_statefile()
        cmd1 = ['env', 'LANG=C', 'cat', self.statefile]
        cmd = rcEnv.rsh.split() + [node] + cmd1
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("could not fetch %s last update uuid"%node)
            raise ex.excError
        return self.parse_statefile(out, node=node)

    def get_local_state(self):
        self.set_statefile()
        with open(self.statefile, 'r') as f:
            out = f.read()
        return self.parse_statefile(out)

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

    def sync_nodes(self):
        if self.target != ['nodes']:
            return
        self.sync_update()

    def sync_drp(self):
        if self.target != ['drpnodes']:
            return
        self.sync_update()

    def sync_update(self):
        if not self.svc_syncable():
            return
        for n in self.targets:
            self.do_update(n)
        self.rotate_snaps()
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)

    def checksum(self, node, bdev, q=None):
        cmd = ['md5sum', bdev]
        if node != rcEnv.nodename:
            cmd = rcEnv.rsh.split() + [node] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return ""
        o = out.split()
        if q is not None:
            q.put(o[0])
        else:
            self.checksums[node] = o[0]

    def sync_verify(self):
        s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
        if s['overall'].status != rcStatus.UP:
            self.log.debug("won't verify this resource for a service not up")
            return
        if self.svc.clustertype in ["flex", "autoflex"] and \
           self.svc.flex_primary != rcEnv.nodename:
            self.log.debug("won't verify this resource from a flex non-primary node")
            return set([])

        self.get_info()
        from multiprocessing import Process, Queue
        self.checksums = {}
        queues = {}
        ps = []
        self.log.info("start checksum threads. please be patient.")
        for n in self.targets:
            dst = self.dsts[n]
            queues[n] = Queue()
            p = Process(target=self.checksum, args=(n, dst, queues[n]))
            p.start()
            ps.append(p)
        self.checksum(rcEnv.nodename, self.snap1)
        self.log.info("md5 %s: %s"%(rcEnv.nodename, self.checksums[rcEnv.nodename]))
        for p in ps:
            p.join()
        for n in self.targets:
            self.checksums[n] = queues[n].get()
            self.log.info("md5 %s: %s"%(n, self.checksums[n]))
        if len(self.checksums) < 2:
            self.log.error("not enough checksums collected")
            raise ex.excError
        err = False
        for n in self.targets:
            if self.checksums[rcEnv.nodename] != self.checksums[n]:
                self.log.error("src/dst checksums differ for %s/%s"%(rcEnv.nodename, n))
                err = True
        if not err:
            self.log.info("src/dst checksums verified")

    def start(self):
        pass

    def stop(self):
        pass

    def can_sync(self, target=None):
        try:
            ls = self.get_local_state()
            last = datetime.datetime.strptime(ls['date'], "%Y-%m-%d %H:%M:%S.%f")
        except IOError:
            return True
        return not self.skip_sync(last)

    def _status(self, verbose=False):
        try:
            ls = self.get_local_state()
            now = datetime.datetime.now()
            last = datetime.datetime.strptime(ls['date'], "%Y-%m-%d %H:%M:%S.%f")
            delay = datetime.timedelta(minutes=self.sync_max_delay)
        except ex.excError:
            self.status_log("failed to get status")
            return rcStatus.WARN
        except IOError:
            self.status_log("dds state file not found")
            return rcStatus.WARN
        except:
            import sys
            import traceback
            e = sys.exc_info()
            print(e[0], e[1], traceback.print_tb(e[2]))
            return rcStatus.WARN
        if last < now - delay:
            self.status_log("Last sync on %s older than %i minutes"%(last, self.sync_max_delay))
            return rcStatus.WARN
        return rcStatus.UP

    def __init__(self,
                 rid=None,
                 target=None,
                 src=None,
                 dsts={},
                 delta_store=None,
                 sender=None,
                 snap_size=0,
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.dds",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.label = "dds of %s to %s"%(src, target)
        self.target = target
        self.src = src
        self.dsts = dsts
        self.snap_size = snap_size
        if delta_store is None:
            self.delta_store = rcEnv.pathvar
        else:
            self.delta_store = delta_store

    def __str__(self):
        return "%s target=%s src=%s" % (resSync.Sync.__str__(self),\
                self.target, self.src)

