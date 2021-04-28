import datetime
import os

from subprocess import *

import core.exceptions as ex
import core.status
import utilities.devices.linux

from .. import Sync, notify
from utilities.converters import print_duration
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import which
from utilities.lazy import lazy

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "dds"
KEYWORDS = [
    {
        "keyword": "src",
        "required": True,
        "text": "Points the origin of the snapshots to replicate from."
    },
    {
        "keyword": "dst",
        "at": True,
        "required": True,
        "text": "Target file or block device. Optional. Defaults to src. Points the media to replay the binary-delta received from source node to. This media must have a size superior or equal to source."
    },
    {
        "keyword": "target",
        "convert": "list",
        "required": True,
        "candidates": ['nodes', 'drpnodes'],
        "text": "Accepted values are ``drpnodes``, ``nodes`` or both, whitespace-separated. Points the target nodes to replay the binary-deltas on. Be warned that starting the service on a target node without a stop-sync_update-start cycle, will break the synchronization, so this mode is usually restricted to drpnodes sync, and should not be used to replicate data between nodes with automated services failover."
    },
    {
        "keyword": "snap_size",
        "text": "Default to 10% of origin. In MB, rounded to physical extent boundaries by lvm tools. Size of the snapshots created by OpenSVC to extract binary deltas from. Opensvc creates at most 2 snapshots : one short-lived to gather changed data from, and one long-lived to gather changed chunks list from. Volume groups should have the necessary space always available."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("dds") and which("lvcreate"):
        return ["sync.dds"]
    return []

class SyncDds(Sync):
    def __init__(self,
                 target=None,
                 src=None,
                 dst=None,
                 snap_size=0,
                 **kwargs):
        super(SyncDds, self).__init__(type="sync.dds", **kwargs)
        self.label = "dds of %s to %s" % (src, ", ".join(target))
        self.target = target
        self.src = src
        self.dst = dst
        self.snap_size = snap_size

    def __str__(self):
        return "%s target=%s src=%s" % (
            super(SyncDds, self).__str__(),
            self.target,
            self.src
        )

    @lazy
    def dsts(self):
        data = {}
        for node in self.svc.nodes | self.svc.drpnodes:
            data[node] = self.oget("dst", impersonate=node)

        if len(data) == 0:
            for node in self.svc.nodes | self.svc.drpnodes:
                data[node] = self.src
        return data

    def pre_action(self, action):
        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        if len(resources) == 0:
            return

        self.pre_sync_check_prd_svc_on_non_prd_node()

        for i, r in enumerate(resources):
            if not r.svc_syncable():
                return
            r.get_info()
            if action == 'sync_full':
                r.remove_snap1()
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
                    raise ex.Error
                r.create_snap2()

    def snap_exists(self, dev):
        if not os.path.exists(dev):
            self.log.debug('dev path does not exist')
            return False
        cmd = [Env.syspaths.lvs, '--noheadings', '-o', 'snap_percent', dev]
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
            raise ex.Error
        cmd = ['lvcreate', '-s', '-n', lv,
               '-L', str(self.snap_size)+'M',
               os.path.join(os.sep, 'dev', self.src_vg, self.src_lv)
              ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def set_statefile(self):
        self.statefile = os.path.join(self.var_d, 'dds_state')

    def create_snap1(self):
        if self.snap_exists(self.snap2):
            self.log.error('%s should not exist'%self.snap2)
            raise ex.Error
        self.create_snap(self.snap1, self.snap1_lv)
        self.write_statefile()

    def create_snap2(self):
        self.create_snap(self.snap2, self.snap2_lv)

    def snap_name(self, snap):
        return os.path.basename(self.src_lv).replace('-', '_')+'_osvc_'+snap

    def get_src_info(self):
        (self.src_vg, self.src_lv, self.src_size) = utilities.devices.linux.lv_info(self, self.src)
        if self.src_lv is None:
            self.log.error("unable to fetch source logical volume information")
            raise ex.Error
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
        self.peersenders = set()
        if 'nodes' not in self.target:
            self.peersenders |= self.svc.nodes
            self.peersenders -= set([Env.nodename])

    def get_targets(self):
        self.targets = set()
        if 'nodes' in self.target:
            self.targets |= self.svc.nodes
        if 'drpnodes' in self.target:
            self.targets |= self.svc.drpnodes
        self.targets -= set([Env.nodename])

    def get_info(self):
        self.get_targets()
        self.get_src_info()

    def svc_syncable(self):
        try:
            self.pre_sync_check_svc_not_up()
            self.pre_sync_check_flex_primary()
        except ex.AbortAction:
            return False
        return True

    def sync_full(self):
        if not self.svc_syncable():
            return
        for n in self.targets:
            self.do_fullsync(n)

    def do_fullsync(self, node):
        dst = self.dsts[node]
        cmd1 = ['dd', 'if='+self.snap1, 'bs=1M']
        cmd2 = Env.rsh.split() + [node, 'dd', 'bs=1M', 'of='+dst]
        self.log.info(' '.join(cmd1 + ["|"] + cmd2))
        p1 = Popen(cmd1, stdout=PIPE)
        p2 = Popen(cmd2, stdin=p1.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode == 0:
            stats_buff = buff[1]
            stats = self.parse_dd(stats_buff)
            self.update_stats(stats, target=node)
        else:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("full sync failed")
            raise ex.Error
        self.push_statefile(node)

    def get_snap1_uuid(self):
        cmd = [Env.syspaths.lvs, '--noheadings', '-o', 'uuid', self.snap1]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
        self.snap1_uuid = out.strip()

    def write_statefile(self):
        self.set_statefile()
        self.get_snap1_uuid()
        self.log.info("update state file with snap uuid %s"%self.snap1_uuid)
        with open(self.statefile, 'w') as f:
             f.write(str(datetime.datetime.now())+';'+self.snap1_uuid+'\n')

    def _push_statefile(self, node):
        cmd = Env.rcp.split() + [self.statefile, node+':'+self.statefile]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def push_statefile(self, node):
        self.set_statefile()
        self._push_statefile(node)
        self.get_peersenders()
        for s in self.peersenders:
            self._push_statefile(s)

    def apply_delta(self, node):
        if not which('dds'):
            raise ex.Error("dds executable not found")
        dst = self.dsts[node]
        extract_cmd = ['dds', '--extract', '--cow', self.snap1_cow, '--source',
                       self.snap2]
        merge_cmd = ['dds', '--merge', '--dest', dst, '-v']
        merge_cmd = Env.rsh.split() + [node] + merge_cmd
        self.log.info(' '.join(extract_cmd + ["|"] + merge_cmd))
        p1 = Popen(extract_cmd, stdout=PIPE)
        pi = Popen(["dd", "bs=4096"], stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        p2 = Popen(merge_cmd, stdin=pi.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode == 0:
            stats_buff = pi.communicate()[1]
            stats = self.parse_dd(stats_buff)
            self.update_stats(stats, target=node)
        else:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("sync update failed")
            raise ex.Error
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
            raise ex.Error

    def rename_snap2(self):
        if not self.snap_exists(self.snap2):
            self.log.error("%s should exist"%self.snap2)
            raise ex.Error
        if self.snap_exists(self.snap1):
            self.log.error("%s should not exist"%self.snap1)
            raise ex.Error
        cmd = ['lvrename', self.src_vg, self.snap2_lv, self.snap1_lv]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def rotate_snaps(self):
        self.remove_snap1()
        self.rename_snap2()

    def check_remote(self, node):
        rs = self.get_remote_state(node)
        if self.snap1_uuid != rs['uuid']:
            self.log.error("%s last update uuid doesn't match snap1 uuid"%(node))
            raise ex.Error

    def get_remote_state(self, node):
        self.set_statefile()
        cmd1 = ['env', 'LANG=C', 'cat', self.statefile]
        cmd = Env.rsh.split() + [node] + cmd1
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("could not fetch %s last update uuid"%node)
            raise ex.Error
        return self.parse_statefile(out, node=node)

    def get_local_state(self):
        self.set_statefile()
        with open(self.statefile, 'r') as f:
            out = f.read()
        return self.parse_statefile(out)

    def parse_statefile(self, out, node=None):
        self.set_statefile()
        if node is None:
            node = Env.nodename
        lines = out.strip().split('\n')
        if len(lines) != 1:
            self.log.error("%s:%s is corrupted"%(node, self.statefile))
            raise ex.Error
        fields = lines[0].split(';')
        if len(fields) != 2:
            self.log.error("%s:%s is corrupted"%(node, self.statefile))
            raise ex.Error
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
        self.write_stats()

    def checksum(self, node, bdev, q=None):
        cmd = ['md5sum', bdev]
        if node != Env.nodename:
            cmd = Env.rsh.split() + [node] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return ""
        o = out.split()
        if q is not None:
            q.put(o[0])
        else:
            self.checksums[node] = o[0]

    def sync_verify(self):
        if not self.svc_syncable():
            return
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
        self.checksum(Env.nodename, self.snap1)
        self.log.info("md5 %s: %s"%(Env.nodename, self.checksums[Env.nodename]))
        for p in ps:
            p.join()
        for n in self.targets:
            self.checksums[n] = queues[n].get()
            self.log.info("md5 %s: %s"%(n, self.checksums[n]))
        if len(self.checksums) < 2:
            self.log.error("not enough checksums collected")
            raise ex.Error
        err = False
        for n in self.targets:
            if self.checksums[Env.nodename] != self.checksums[n]:
                self.log.error("src/dst checksums differ for %s/%s"%(Env.nodename, n))
                err = True
        if not err:
            self.log.info("src/dst checksums verified")

    def start(self):
        pass

    def stop(self):
        pass

    def can_sync(self, target=None):
        return True

    def sync_status(self, verbose=False):
        try:
            ls = self.get_local_state()
            now = datetime.datetime.now()
            last = datetime.datetime.strptime(ls['date'], "%Y-%m-%d %H:%M:%S.%f")
            delay = datetime.timedelta(seconds=self.sync_max_delay)
        except ex.Error:
            self.status_log("failed to get status")
            return core.status.WARN
        except IOError:
            self.status_log("dds state file not found")
            return core.status.WARN
        except:
            import sys
            import traceback
            e = sys.exc_info()
            print(e[0], e[1], traceback.print_tb(e[2]))
            return core.status.WARN
        if last < now - delay:
            self.status_log("Last sync on %s older than %s"%(last, print_duration(self.sync_max_delay)))
            return core.status.WARN
        return core.status.UP

    def _info(self):
        data = [
          ["src", self.src],
          ["target", " ".join(self.target) if self.target else ""],
        ]
        data += self.stats_keys()
        return data

    @notify
    def sync_all(self):
        self.sync_nodes()
        self.sync_drp()
