import os
import time
import json

from subprocess import *

import core.status
import utilities.subsystems.btrfs
import core.exceptions as ex
from .. import Sync, notify
from env import Env
from utilities.converters import print_duration
from utilities.string import bdecode
from core.objects.svcdict import KEYS

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "btrfs"
KEYWORDS = [
    {
        "keyword": "src",
        "at": True,
        "required": True,
        "text": "Source subvolume of the sync."
    },
    {
        "keyword": "dst",
        "at": True,
        "required": True,
        "text": "Destination subvolume of the sync."
    },
    {
        "keyword": "target",
        "convert": "list",
        "at": True,
        "required": True,
        "candidates": ["nodes", "drpnodes"],
        "text": "Destination nodes of the sync."
    },
    {
        "keyword": "recursive",
        "at": True,
        "default": False,
        "convert": "boolean",
        "candidates": [True, False],
        "text": "Also replicate subvolumes in the src tree."
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
    if which("btrfs"):
        return ["sync.btrfs"]
    return []


class SyncBtrfs(Sync):
    """
    Define btrfs sync resource to be btrfs send/btrfs receive between nodes
    """
    def __init__(self,
                 target=None,
                 src=None,
                 dst=None,
                 sender=None,
                 recursive=False,
                 snap_size=0,
                 **kwargs):
        super(SyncBtrfs, self).__init__(type="sync.btrfs", **kwargs)

        self.target = target or []
        self.label = "btrfs of %s to %s"%(src, ", ".join(self.target))
        self.src = src
        self.sender = sender
        self.recursive = recursive

        if ":" not in src or src.index(":") == len(src) - 1:
            raise ex.InitError("malformed src value")
        if ":" not in dst or dst.index(":") == len(dst) - 1:
            raise ex.InitError("malformed dst value")

        self.src_label = src[:src.index(":")]
        self.src_subvol = src[src.index(":")+1:]
        if dst is None:
            self.dst_label = self.src_label
            self.dst_subvol = self.src_subvol
        else:
            self.dst_label = dst[:dst.index(":")]
            self.dst_subvol = dst[dst.index(":")+1:]

        self.dst_btrfs = {}
        self.src_btrfs = None

    def on_add(self):
        self.statefile = os.path.join(self.var_d, "btrfs_state")

    def __str__(self):
        return "%s target=%s src=%s" % (
            super(SyncBtrfs, self).__str__(),\
            self.target, self.src
        )

    def sort_rset(self, rset):
        rset.resources.sort(key=lambda x: getattr(x, "src_subvol", x.rid))

    def _info(self):
        data = [
          ["src", self.src],
          ["target", " ".join(self.target) if self.target else ""],
        ]
        data += self.stats_keys()
        return data

    def init_src_btrfs(self):
        if self.src_btrfs is not None:
            return
        try:
            self.src_btrfs = utilities.subsystems.btrfs.Btrfs(label=self.src_label, resource=self)
        except utilities.subsystems.btrfs.ExecError as e:
            raise ex.Error(str(e))

    def pre_action(self, action):
        """Prepare snapshots
        Don't sync PRD services when running on !PRD node
        skip snapshot creation if delay_snap in tags
        delay_snap should be used for oracle archive datasets
        """
        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        if len(resources) == 0:
            return

        if not action.startswith("sync"):
            return

        self.pre_sync_check_svc_not_up()
        self.pre_sync_check_prd_svc_on_non_prd_node()

        self.init_src_btrfs()
        for i, r in enumerate(resources):
            if "delay_snap" in r.tags:
                continue
            r.get_targets(action)
            tgts = r.targets.copy()
            if len(tgts) == 0:
                continue

            r.get_src_info()
            tosends = []

            for subvol in r.subvols():
                src = r.src_btrfs.rootdir + "/" + subvol["path"]
                dst = self.src_snap_tosend(subvol)
                tosends.append((src, dst))

            r.recreate_snaps(tosends)

    def all_subvols(self):
        """
        sort by path so the subvols are sorted by path depth
        """
        if not self.src:
            return []
        if not self.recursive:
            sub = self.src_btrfs.get_subvol(self.src)
            if not sub:
                return []
            return [sub]
        subvols = self.src_btrfs.get_subvols().values()
        subvols = sorted(subvols, key=lambda x: x["path"])
        return subvols

    def subvols(self):
        """
        sort by path so the subvols are sorted by path depth
        """
        if not self.src:
            return []
        if not self.recursive:
            sub = self.src_btrfs.get_subvol(self.src)
            if not sub:
                return []
            return [sub]
        subvols = []
        for subvol in self.src_btrfs.get_subvols().values():
            if subvol["path"].endswith("@tosend"):
                continue
            if subvol["path"].endswith("@sent"):
                continue
            if subvol["path"] == self.src_subvol:
                subvols.append(subvol)
            elif subvol["path"].startswith(self.src_subvol + "/"):
                subvols.append(subvol)
        subvols = sorted(subvols, key=lambda x: x["path"])
        return subvols

    def remote_subvols(self, node):
        if not self.dst:
            return []
        if not self.recursive:
            sub = self.dst_btrfs[node].get_subvol(self.dst)
            if not sub:
                return []
            return [sub]
        subvols = self.dst_btrfs[node].get_subvols().values()
        subvols = sorted(subvols, key=lambda x: x["path"])
        return subvols

    def recreate_snaps(self, snaps):
        self.init_src_btrfs()
        self.src_btrfs.subvol_delete([snap[1] for snap in snaps])
        try:
            self.src_btrfs.snapshots(snaps, readonly=True)
        except utilities.subsystems.btrfs.ExistError:
            self.log.error("%s should not exist"%snap)
            raise ex.Error
        except utilities.subsystems.btrfs.ExecError:
            raise ex.Error

    def get_src_info(self):
        self.init_src_btrfs()
        self.src = os.path.join(self.src_btrfs.rootdir, self.src_subvol)

    def rel_src_snap_sent(self, subvol):
        p = subvol["path"].replace("/","_")
        base = self.src_btrfs.snapvol + "/" + p
        return  base + "@sent"

    def src_snap_sent(self, subvol):
        p = subvol["path"].replace("/","_")
        base = self.src_btrfs.snapdir + "/" + p
        return  base + "@sent"

    def src_snap_tosend(self, subvol):
        p = subvol["path"].replace("/","_")
        base = self.src_btrfs.snapdir + "/" + p
        return base + "@tosend"

    def get_dst_info(self, node):
        if node not in self.dst_btrfs:
            try:
                self.dst_btrfs[node] = utilities.subsystems.btrfs.Btrfs(label=self.dst_label, resource=self, node=node)
            except utilities.subsystems.btrfs.ExecError as e:
                raise ex.Error(str(e))
            #self.dst_btrfs[node].setup_snap()
        self.dst = os.path.join(self.dst_btrfs[node].rootdir, self.dst_subvol)

    def rel_dst_snap_tosend(self, subvol, node):
        p = subvol["path"].replace("/","_")
        base = self.dst_btrfs[node].snapvol + "/" + p
        return base + "@tosend"

    def dst_snap_tosend(self, subvol, node):
        p = subvol["path"].replace("/","_")
        base = self.dst_btrfs[node].snapdir + "/" + p
        return base + "@tosend"

    def rel_dst_snap_sent(self, subvol, node):
        p = subvol["path"].replace("/","_")
        base = self.dst_btrfs[node].snapvol + "/" + p
        return base + "@sent"

    def dst_snap_sent(self, subvol, node):
        p = subvol["path"].replace("/","_")
        base = self.dst_btrfs[node].snapdir + "/" + p
        return base + "@sent"

    def get_peersenders(self):
        self.peersenders = set()
        if "nodes" == self.sender:
            self.peersenders |= self.svc.nodes
            self.peersenders -= set([Env.nodename])

    def get_targets(self, action=None):
        self.targets = set()
        if "nodes" in self.target and action in (None, "sync_nodes", "sync_full"):
            self.targets |= self.svc.nodes
        if "drpnodes" in self.target and action in (None, "sync_drp", "sync_full"):
            self.targets |= self.svc.drpnodes
        self.targets -= set([Env.nodename])

    def sync_nodes(self):
        self._sync_update("sync_nodes")

    def sync_drp(self):
        self._sync_update("sync_drp")

    def sanity_checks(self):
        self.pre_sync_check_svc_not_up()
        self.pre_sync_check_flex_primary()

    def btrfs_send_incremental(self, subvols, node):
        if len(subvols) == 0:
            return
        send_cmd = ["btrfs", "send"]
        for subvol in subvols:
            send_cmd += [
                    "-c", self.src_snap_sent(subvol),
                    self.src_snap_tosend(subvol),
            ]

        receive_cmd = ["btrfs", "receive", self.dst_btrfs[node].snapdir]
        if node is not None:
            receive_cmd = Env.rsh.strip(" -n").split() + [node] + receive_cmd

        self.log.info(" ".join(send_cmd + ["|"] + receive_cmd))
        p1 = Popen(send_cmd, stdout=PIPE, stderr=PIPE)
        pi = Popen(["dd", "bs=4096"], stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        p2 = Popen(receive_cmd, stdin=pi.stdout, stdout=PIPE)
        buff = p2.communicate()
        send_buff = p1.stderr.read()
        if send_buff is not None and len(send_buff) > 0:
            for line in bdecode(send_buff).split("\n"):
                if line:
                    self.log.info("| " + line)
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
            for line in bdecode(buff[0]).split("\n"):
                if line:
                    self.log.info("| " + line)

    def btrfs_send_initial(self, subvols, node):
        if len(subvols) == 0:
            return
        send_cmd = ["btrfs", "send"]
        for subvol in subvols:
            send_cmd += [self.src_snap_tosend(subvol)]
        receive_cmd = ["btrfs", "receive", self.dst_btrfs[node].snapdir]
        if node is not None:
            receive_cmd = Env.rsh.strip(" -n").split() + [node] + receive_cmd

        self.log.info(" ".join(send_cmd + ["|"] + receive_cmd))
        p1 = Popen(send_cmd, stdout=PIPE)
        pi = Popen(["dd", "bs=4096"], stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        p2 = Popen(receive_cmd, stdin=pi.stdout, stdout=PIPE)
        buff = p2.communicate()
        if p2.returncode == 0:
            stats_buff = pi.communicate()[1]
            stats = self.parse_dd(stats_buff)
            self.update_stats(stats, target=node)
        else:
            if buff[1] is not None and len(buff[1]) > 0:
                self.log.error(buff[1])
            self.log.error("full sync failed")
            raise ex.Error
        if buff[0] is not None and len(buff[0]) > 0:
            self.log.info(buff[0])

    def to_snap_path(self, p, node=None):
        btr = self.get_btrfs(node)
        snapdir = btr.snapdir
        return os.path.join(snapdir, p.replace("/", "_"))

    def cleanup_remote(self, node, remote_subvols):
        self.init_src_btrfs()
        o = self.get_btrfs(node)
        l = []
        candidates = []
        for subvol in self.subvols():
            candidates.append(self.rel_dst_snap_tosend(subvol, node))
            candidates.append(self.rel_dst_tmp(subvol, node))

        for subvol in remote_subvols:
            if subvol["path"] in candidates:
                l.append(subvol["path"])

        l = [o.rootdir+"/"+p for p in l]

        try:
            o.subvol_delete(l)
        except utilities.subsystems.btrfs.ExecError:
            raise ex.Error()

    def remove_snap(self, subvol, node=None):
        #self.init_src_btrfs()
        o = self.get_btrfs(node)
        if node is not None:
            p = self.dst_snap_sent(subvol, node)
        else:
            p = self.src_snap_sent(subvol)

        if not o.has_subvol(p):
            return []

        cmd = o.subvol_delete_cmd(p) or []
        return [" ".join(cmd)]

    def rename_snap(self, subvol, node=None):
        self.init_src_btrfs()
        o = self.get_btrfs(node)
        if node is None:
            src = self.src_snap_tosend(subvol)
            dst = self.src_snap_sent(subvol)
        else:
            src = self.dst_snap_tosend(subvol, node)
            dst = self.dst_snap_sent(subvol, node)

        cmd = ["mv", "-v", src, dst]
        return [" ".join(cmd)]

    def remove_dst(self, subvol, node):
        dst = os.path.join(self.dst, subvol["path"])
        cmd = self.dst_btrfs[node].subvol_delete_cmd(dst)
        if not cmd:
            return []
        return [" ".join(cmd)]

    def rel_dst_tmp(self, subvol, node):
        return os.path.join(
            ".opensvc/tmp",
            self.dst_subvol,
            subvol["path"][len(self.src_subvol):].lstrip("/"),
        ).rstrip("/")


    def dst_tmp(self, subvol, node):
        return os.path.join(
                self.dst_btrfs[node].rootdir,
                self.rel_dst_tmp(subvol, node),
        )

    def install_final(self, node):
        cmds = []

        subvols = self.dst_btrfs[node].get_subvols()
        paths = [os.path.join(self.dst_btrfs[node].rootdir, s["path"]) for s in subvols.values() if s["path"].startswith(self.dst_subvol+"/") or s["path"] == self.dst_subvol]

        head_subvol = self.subvols()[0]
        src = self.dst_tmp(head_subvol, node)
        cmd = self.dst_btrfs[node].subvol_delete_cmd(paths) or []
        if cmd:
            cmds.append(" ".join(cmd))
        cmd = ["mv", "-v", src, self.dst]
        cmds.append(" ".join(cmd))
        return cmds

    def install_dst(self, subvol, node):
        src = self.dst_snap_sent(subvol, node)
        dst = self.dst_tmp(subvol, node)
        cmd = self.dst_btrfs[node].snapshot_cmd(src, dst, readonly=False)
        if not cmd:
            return []
        return [" ".join(cmd)]

    def get_btrfs(self, node=None):
        if node:
            o = self.dst_btrfs[node]
        else:
            o = self.src_btrfs
        return o

    def do_cmds(self, cmds, node=None):
        o = self.get_btrfs(node)
        ret, out, err = o.vcall(" && ".join(cmds), shell=True)
        if ret != 0:
            raise ex.Error

    def _sync_update(self, action):
        self.init_src_btrfs()
        try:
            self.sanity_checks()
        except ex.Error:
            return
        self.get_targets(action)
        if len(self.targets) == 0:
            return
        self.get_src_info()
        subvols_paths = [s["path"] for s in self.all_subvols()]
        src_cmds = []
        subvols = self.subvols()

        for n in self.targets:
            self.get_dst_info(n)
            remote_subvols = self.remote_subvols(n)
            self.cleanup_remote(n, remote_subvols)
            rsubvols_paths = [s["path"] for s in remote_subvols]
            cmds = []
            incrs = []
            fulls = []
            for subvol in subvols:
                if self.rel_dst_snap_sent(subvol, n) in rsubvols_paths and self.rel_src_snap_sent(subvol) in subvols_paths:
                    incrs.append(subvol)
                else:
                    fulls.append(subvol)
            for subvol in subvols:
                cmds += self.remove_snap(subvol, n)
                src_cmds += self.remove_snap(subvol)
            for subvol in subvols:
                cmds += self.rename_snap(subvol, n)
                src_cmds += self.rename_snap(subvol)
            for subvol in subvols:
                cmds += self.remove_dst(subvol, n)
            for subvol in subvols:
                cmds += self.install_dst(subvol, n)
            cmds += self.install_final(n)
            self.btrfs_send_incremental(incrs, n)
            self.btrfs_send_initial(fulls, n)
            self.do_cmds(cmds, n)

        self.do_cmds(src_cmds)
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)
        self.write_stats()

    def sync_full(self):
        self.init_src_btrfs()
        try:
            self.sanity_checks()
        except ex.Error:
            return
        self.get_src_info()
        self.get_targets()
        src_cmds = []
        for n in self.targets:
            self.get_dst_info(n)
            remote_subvols = self.remote_subvols(n)
            self.cleanup_remote(n, remote_subvols)
            cmds = []
            for subvol in self.subvols():
                cmds += self.remove_snap(subvol, n)
                src_cmds += self.remove_snap(subvol, n)
            for subvol in self.subvols():
                cmds += self.rename_snap(subvol, n)
                src_cmds += self.rename_snap(subvol, n)
            for subvol in self.subvols():
                cmds += self.remove_dst(subvol, n)
            for subvol in self.subvols():
                cmds += self.install_dst(subvol, n)

            cmds += self.install_final(n)
            self.btrfs_send_initial(self.subvols(), n)
            self.do_cmds(cmds, n)
        self.do_cmds(src_cmds)
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)

    def can_sync(self, target=None):
        return True

    def sync_status(self, verbose=False):
        self.init_src_btrfs()
        try:
            last = os.path.getmtime(self.statefile)
            now = time.time()
            delay = self.sync_max_delay
        except (KeyError, TypeError):
            self.status_log("btrfs state file is corrupt")
            return core.status.WARN
        except IOError:
            self.status_log("btrfs state file not found")
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

    def get_remote_state(self, node):
        cmd1 = ["cat", self.statefile]
        cmd = Env.rsh.split() + [node] + cmd1
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("could not fetch %s last update uuid"%node)
            raise ex.Error
        return json.loads(out)

    def get_local_state(self):
        with open(self.statefile, "r") as f:
            out = f.read()
        return json.loads(out)

    def write_statefile(self):
        data = self.all_subvols(),
        with open(self.statefile, "w") as f:
            json.dump(data, f)

    def _push_statefile(self, node):
        cmd = Env.rcp.split() + [self.statefile, node+":"+self.statefile.replace("#", r"\#")]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def push_statefile(self, node):
        self._push_statefile(node)
        self.get_peersenders()
        for s in self.peersenders:
            self._push_statefile(s)

    @notify
    def sync_all(self):
        self.sync_nodes()
        self.sync_drp()
