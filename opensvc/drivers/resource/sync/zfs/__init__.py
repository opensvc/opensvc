import datetime
import os

from subprocess import *

import core.exceptions as ex
import core.status
from .. import Sync, notify
from env import Env
from utilities.subsystems.zfs import a2pool_dataset, Dataset
from utilities.lazy import lazy
from utilities.converters import print_duration
from core.objects.svcdict import KEYS
from utilities.proc import justcall
from utilities.string import bdecode

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "zfs"
KEYWORDS = [
    {
        "keyword": "src",
        "at": True,
        "required": True,
        "text": "Source dataset of the sync."
    },
    {
        "keyword": "dst",
        "at": True,
        "required": True,
        "text": "Destination dataset of the sync."
    },
    {
        "keyword": "target",
        "convert": "list",
        "required": True,
        "candidates": ['nodes', 'drpnodes', 'local'],
        "text": "Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
    },
    {
        "keyword": "recursive",
        "at": True,
        "default": True,
        "convert": "boolean",
        "candidates": (True, False),
        "text": "Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
    },
    {
        "keyword": "tags",
        "convert": "set",
        "default": set(),
        "default_text": "",
        "example": "delay_snap",
        "at": True,
        "text": "The zfs sync resource supports the :c-tag:`delay_snap` tag. This tag is used to delay the snapshot creation just before the sync, thus after :kw:`postsnap_trigger` execution. The default behaviour (no tags) is to group all snapshots creation before copying data to remote nodes, thus between :kw:`presnap_trigger` and :kw:`postsnap_trigger`."
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
    data = []
    if which("zfs"):
        data.append("sync.zfs")
    return data

class SyncZfs(Sync):
    """define zfs sync resource to be zfs send/zfs receive between nodes
    """
    def __init__(self,
                 target=None,
                 src=None,
                 dst=None,
                 recursive = True,
                 snap_size=0,
                 **kwargs):
        super(SyncZfs, self).__init__(type="sync.zfs", **kwargs)
        self.label = "zfs of %s to %s"%(src, ",".join(target))
        self.target = target
        self.recursive = recursive
        self.src = src
        self.dst = dst
        (self.src_pool, self.src_ds) = a2pool_dataset(src)
        (self.dst_pool, self.dst_ds) = a2pool_dataset(dst)

    def __str__(self):
        return "%s target=%s src=%s" % (
            super(SyncZfs, self).__str__(),
            self.target,
            self.src
        )

    def _info(self):
        data = [
          ["src", self.src],
          ["dst", self.dst],
          ["target", " ".join(self.target) if self.target else ""],
          ["recursive", str(self.recursive).lower()],
        ]
        data += self.stats_keys()
        return data

    def sanity_check(self):
        if self.src_pool == self.dst_pool and 'local' in self.target:
            self.log.error('zfs send/receive in same pool not allowed')
            raise ex.Error

    def pre_action(self, action):
        """Prepare dataset snapshots
        Don't sync PRD services when running on !PRD node
        skip snapshot creation if delay_snap in tags
        delay_snap should be used for oracle archive datasets
        """
        self.sanity_check()
        if not hasattr(self, action):
            return
        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        if len(resources) == 0:
            return

        self.pre_sync_check_prd_svc_on_non_prd_node()
        self.pre_sync_check_svc_not_up()
        self.pre_sync_check_flex_primary()

        for i, r in enumerate(resources):
            if 'delay_snap' in r.tags:
                continue
            r.get_info()
            if action in ['sync_update', 'sync_resync', 'sync_drp', 'sync_nodes']:
                if action == 'sync_nodes' and r.target != ['nodes']:
                    return
                if action == 'sync_drp' and r.target != ['drpnodes']:
                    return
                nb = 0
                tgts = r.targets.copy()
                if len(tgts) == 0 :
                    continue
            if not r.snap_exists(r.src_snap_tosend):
                r.create_snap(r.src_snap_tosend)

    def snap_exists(self, snapname, node=None):
        cmd = [Env.syspaths.zfs, 'list', '-t', 'snapshot', snapname]
        if node is not None:
            cmd = Env.rsh.split() + [node] + cmd
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        else:
            return False

    def create_snap(self, snap):
        snapds = Dataset(snap)
        if snapds.exists():
            self.log.error('%s should not exist'%snap)
            raise ex.Error
        if self.recursive :
            cmd = [Env.syspaths.zfs, 'snapshot' , '-r' , snap]
        else:
            cmd = [Env.syspaths.zfs, 'snapshot' , snap]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def get_upper_fs(self, fs):
        fsmembers = fs.split('/')
        if len(fsmembers) > 1:
            fsmembers = fsmembers[:len(fsmembers)-1]
            upperfs = "/".join(fsmembers)
        else:
            upperfs = '/'
        return upperfs

    def fs_exists(self, fsname, node=None):
        cmd = [Env.syspaths.zfs, 'list', '-t', 'filesystem', fsname]
        if node is not None:
            cmd = Env.rsh.split() + [node] + cmd
        out, err, ret = justcall(cmd)
        if ret == 0:
            return True
        else:
            return False

    def create_fs(self, fs, node=None):
        if self.fs_exists(fs, node):
            msg = None
            if node is not None:
                msg = 'on node %s'%node
            self.log.info('%s already exist %s'%(fs, msg))
            return
        fsmembers = fs.split('/')
        if len(fsmembers) > 1:
            upperfs = self.get_upper_fs(fs)
            if not self.fs_exists(upperfs, node):
                self.create_fs(upperfs, node)
        cmd = [Env.syspaths.zfs, 'create' , fs]
        if node is not None:
            cmd = Env.rsh.split() + [node] + cmd
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def get_src_info(self):
        self.src_snap_sent_old = self.src_ds + "@sent"
        self.src_snap_tosend_old = self.src_ds + "@tosend"
        self.src_snap_sent = self.src_ds + "@"+self.rid.replace("#",".") + ".sent"
        self.src_snap_tosend = self.src_ds + "@"+self.rid.replace("#",".") + ".tosend"

    def get_dst_info(self):
        self.dst_snap_sent_old = self.dst_ds + "@sent"
        self.dst_snap_tosend_old = self.dst_ds + "@tosend"
        self.dst_snap_sent = self.dst_ds + "@"+self.rid.replace("#",".") + ".sent"
        self.dst_snap_tosend = self.dst_ds + "@"+self.rid.replace("#",".") + ".tosend"

    def get_peersenders(self):
        self.peersenders = set()
        if Env.nodename not in self.svc.nodes or self.target != ["drpnodes"]:
            return
        self.peersenders |= self.svc.nodes
        self.peersenders -= set([Env.nodename])

    def get_targets(self):
        self.targets = set()
        if len(self.target) > 1 and 'local' in self.target:
            self.status_log("incompatible targets %s" % self.target, 'WARN')
            self.targets = set()
            return
        if 'local' in self.target:
            self.targets |= set([Env.nodename])
            return
        if 'nodes' in self.target:
            self.targets |= self.svc.nodes
        if 'drpnodes' in self.target:
            self.targets |= self.svc.drpnodes
        self.targets -= set([Env.nodename])

    def get_info(self):
        self.get_targets()
        self.get_src_info()
        self.get_dst_info()

    def sync_nodes(self):
        """
        Run sync_update if the target contains only nodes.
        """
        if self.target == ["nodes"] or self.target == ["local"]:
            self.sync_update()
        else:
            self.log.warning("skip: target also has drp nodes.")

    def sync_drp(self):
        """
        Run sync_update if the target contains only drpnodes.
        """
        if self.target == ["drpnodes"]:
            self.sync_update()
        else:
            self.log.warning("skip: target is not only drp nodes.")

    def sync_full(self):
        """
        Purge all local and remote snaps, and call sync_update, which
        will do a non-incremental send/recv.
        """
        self.destroy_all_snaps()
        self.sync_update()

    def destroy_all_snaps(self):
        self.force_remove_snap(self.src_snap_tosend)
        self.force_remove_snap(self.src_snap_sent)
        for node in self.targets:
            self.force_remove_snap(self.dst_snap_tosend, node)
            self.force_remove_snap(self.dst_snap_sent, node)

    def do_it(self, send_cmd, receive_cmd, node):
        self.log.info(' '.join(send_cmd + ["|"] + receive_cmd))
        p1 = Popen(send_cmd, stdout=PIPE)
        pi = Popen(["dd", "bs=4096"], stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        p2 = Popen(receive_cmd, stdin=pi.stdout, stdout=PIPE, stderr=PIPE)
        buff =  p2.communicate()
        if p2.returncode == 0:
            stats_buff = pi.communicate()[1]
            stats = self.parse_dd(stats_buff)
            self.update_stats(stats, target=node)
        out = bdecode(buff[0])
        err = bdecode(buff[1])
        if p2.returncode != 0:
            if err is not None and len(err) > 0:
                self.log.error(err)
            raise ex.Error("sync failed")
        if out is not None and len(out) > 0:
            self.log.info(out)

    def zfs_send_incremental(self, node):
        if not self.snap_exists(self.dst_snap_sent, node):
            return self.zfs_send_initial(node)
        if self.recursive:
            send_cmd = [Env.syspaths.zfs, "send", "-R", "-I",
                        self.src_snap_sent, self.src_snap_tosend]
        else:
            send_cmd = [Env.syspaths.zfs, "send", "-I",
                        self.src_snap_sent, self.src_snap_tosend]

        if self.src_ds == self.dst_ds or ( self.src_ds == self.src_pool and self.dst_ds == self.dst_pool ):
            receive_cmd = [Env.syspaths.zfs, "receive", "-dF", self.dst_pool]
        else:
            fspath = self.get_upper_fs(self.dst_ds)
            receive_cmd = [Env.syspaths.zfs, "receive", "-eF", fspath]
        if node is not None and 'local' not in self.target:
            _receive_cmd = Env.rsh.strip(' -n').split()
            if "-q" in _receive_cmd:
                _receive_cmd.remove("-q")
            receive_cmd = _receive_cmd + [node] + receive_cmd

        self.do_it(send_cmd, receive_cmd, node)

    def zfs_send_initial(self, node=None):
        if self.recursive:
            send_cmd = [Env.syspaths.zfs, "send", "-R",
                        self.src_snap_tosend]
        else:
            send_cmd = [Env.syspaths.zfs, "send", "-p",
                        self.src_snap_tosend]

        if self.src_ds == self.dst_ds or ( self.src_ds == self.src_pool and self.dst_ds == self.dst_pool ):
            receive_cmd = [Env.syspaths.zfs, "receive", "-dF", self.dst_pool]
        else:
            fspath = self.get_upper_fs(self.dst_ds)
            self.create_fs(fspath, node)
            receive_cmd = [Env.syspaths.zfs, "receive", "-eF", fspath]
        if node is not None and 'local' not in self.target:
            _receive_cmd = Env.rsh.strip(' -n').split()
            if "-q" in _receive_cmd:
                _receive_cmd.remove("-q")
            receive_cmd = _receive_cmd + [node] + receive_cmd

        self.do_it(send_cmd, receive_cmd, node)

    def force_remove_snap(self, snap, node=None):
        try:
            self.remove_snap(snap, node=node, check_exists=False)
        except ex.Error:
            pass

    def remove_snap(self, snap, node=None, check_exists=True):
        if check_exists and not self.snap_exists(snap, node=node):
            return
        if self.recursive :
            cmd = [Env.syspaths.zfs, 'destroy', '-r', snap]
        else:
            cmd = [Env.syspaths.zfs, 'destroy', snap]
        if node is not None:
            cmd = Env.rsh.split() + [node] + cmd
        if check_exists:
            err_to_info = False
        else:
            err_to_info = True
        (ret, out, err) = self.vcall(cmd, err_to_info=err_to_info)
        if ret != 0:
            raise ex.Error

    def rename_snap(self, src, dst,  node=None):
        if self.snap_exists(dst, node):
            self.log.error("%s should not exist"%dst)
            raise ex.Error
        if self.recursive :
            cmd = [Env.syspaths.zfs, 'rename', '-r', src, dst]
        else:
            cmd = [Env.syspaths.zfs, 'rename', src, dst]

        if node is not None:
            cmd = Env.rsh.split() + [node] + cmd
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def rotate_snaps(self, src, dst, node=None):
        self.remove_snap(dst, node)
        self.rename_snap(src, dst, node)

    @notify
    def sync_update(self):
        """
        test if service status is UP else return
        create the snap_tosend if not already created (during pre_action)
        if a snap has already been sent
        then for all targets
             zfs_send_incremental
             rotate snap
        else for all targets
            zfs_send_initial
            rotate snap
        rotate snap on local node
        """
        self.pre_sync_check_svc_not_up()
        self.pre_sync_check_flex_primary()

        self.get_info()

        if self.snap_exists(self.src_snap_sent_old):
            self.log.info("migrate local sent snap name")
            self.rename_snap(self.src_snap_sent_old, self.src_snap_sent)
            for node in self.targets:
                self.log.info("migrate sent snap name on node %s", node)
                self.rename_snap(self.dst_snap_sent_old, self.dst_snap_sent, node=node)

        if not self.snap_exists(self.src_snap_tosend):
            self.create_snap(self.src_snap_tosend)
        if self.snap_exists(self.src_snap_sent):
            for n in self.targets:
                self.remove_snap(self.dst_snap_tosend, n)
                self.zfs_send_incremental(n)
                self.rotate_snaps(self.dst_snap_tosend, self.dst_snap_sent, n)
        else:
            for n in self.targets:
                self.remove_snap(self.dst_snap_tosend, n)
                self.zfs_send_initial(n)
                self.rotate_snaps(self.dst_snap_tosend, self.dst_snap_sent, n)
        self.rotate_snaps(self.src_snap_tosend, self.src_snap_sent)
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)
        self.write_stats()

    def can_sync(self, target=None):
        if not Dataset(self.src).exists():
            return False
        return True

    def sync_status(self, verbose=False):
        try:
            ls = self.get_local_state()
            now = datetime.datetime.now()
            last = datetime.datetime.strptime(ls['date'], "%Y-%m-%d %H:%M:%S.%f")
            delay = datetime.timedelta(seconds=self.sync_max_delay)
        except IOError:
            self.status_log("zfs state file not found")
            return core.status.WARN
        except:
            import sys
            import traceback
            e = sys.exc_info()
            print(e[0], e[1], traceback.print_tb(e[2]))
            return core.status.WARN
        if last < now - delay:
            self.status_log("Last sync on %s older than %s" % (last, print_duration(self.sync_max_delay)))
            return core.status.WARN
        return core.status.UP

    def check_remote(self, node):
        rs = self.get_remote_state(node)
        if self.snap_uuid != rs['uuid']:
            self.log.error("%s last update uuid doesn't match snap uuid"%(node))
            raise ex.Error

    def get_remote_state(self, node):
        cmd1 = ['cat', self.statefile]
        cmd = Env.rsh.split() + [node] + cmd1
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("could not fetch %s last update uuid"%node)
            raise ex.Error
        return self.parse_statefile(out, node=node)

    def get_local_state(self):
        with open(self.statefile, 'r') as f:
            out = f.read()
        return self.parse_statefile(out)

    def get_snap_uuid(self, snap):
        cmd = ['env', 'LC_ALL=C', Env.syspaths.zfs, 'list', '-H', '-o', 'creation', '-t', 'snapshot', snap]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error
        self.snap_uuid = out.strip()

    @lazy
    def statefile(self):
        return os.path.join(self.var_d, 'zfs_state')

    def write_statefile(self):
        self.get_snap_uuid(self.src_snap_sent)
        self.log.info("update state file with snap uuid %s"%self.snap_uuid)
        with open(self.statefile, 'w') as f:
             f.write(str(datetime.datetime.now())+';'+self.snap_uuid+'\n')

    def _push_statefile(self, node):
        cmd = Env.rcp.split() + [self.statefile, node+':'+self.statefile.replace('#', r'\#')]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def push_statefile(self, node):
        self._push_statefile(node)
        self.get_peersenders()
        for node in self.peersenders:
            self._push_statefile(node)

    def parse_statefile(self, out, node=None):
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

