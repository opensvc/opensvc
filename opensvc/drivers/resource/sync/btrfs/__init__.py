import os
import time
import json
import subprocess
import datetime

import core.status
import utilities.subsystems.btrfs
import core.exceptions as ex
from .. import Sync, notify
from env import Env
from utilities.chunker import chunker
from utilities.converters import print_duration
from utilities.string import bdecode
from utilities.files import makedirs, protected_dir
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
          ["target", subprocess.list2cmdline(self.target) if self.target else ""],
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
            r.remove_src_snap_next()

        for i, r in enumerate(resources):
            tosends = []


            for subvol in r.subvols():
                src = r.src_btrfs.rootdir + "/" + subvol["path"]
                dst = r.src_snap_next(subvol)
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
        self.make_src_workdirs()
        self.init_src_btrfs()
        self.src_btrfs.subvol_delete([snap[1] for snap in snaps if os.path.exists(snap[1])])
        try:
            self.src_btrfs.snapshots(snaps, readonly=True)
        except utilities.subsystems.btrfs.ExistError:
            self.log.error("%s should not exist"%snaps)
            raise ex.Error
        except utilities.subsystems.btrfs.ExecError:
            raise ex.Error

    def get_src_info(self):
        self.init_src_btrfs()
        self.src = os.path.join(self.src_btrfs.rootdir, self.src_subvol)

    def get_dst_info(self, node):
        if node not in self.dst_btrfs:
            try:
                self.dst_btrfs[node] = utilities.subsystems.btrfs.Btrfs(label=self.dst_label, resource=self, node=node)
            except utilities.subsystems.btrfs.ExecError as e:
                raise ex.Error(str(e))
        self.dst = os.path.join(self.dst_btrfs[node].rootdir, self.dst_subvol)

    def src_temp_dir(self):
        return os.path.join(self.src_btrfs.rootdir, ".osync", self.svc.fullname, self.rid, "temp")

    def dst_temp_dir(self, node):
        return os.path.join(self.dst_btrfs[node].rootdir, ".osync", self.svc.fullname, self.rid, "temp")

    def src_next_dir(self):
        return os.path.join(self.src_btrfs.rootdir, ".osync", self.svc.fullname, self.rid, "next")

    def dst_next_dir(self, node):
        return os.path.join(self.dst_btrfs[node].rootdir, ".osync", self.svc.fullname, self.rid, "next")

    def src_last_dir(self):
        return os.path.join(self.src_btrfs.rootdir, ".osync", self.svc.fullname, self.rid, "last")

    def dst_last_dir(self, node):
        return os.path.join(self.dst_btrfs[node].rootdir, ".osync", self.svc.fullname, self.rid, "last")

    def rel_snap_last(self, subvol):
        p = subvol["path"].replace("/","_")
        return  os.path.join(".osync", self.svc.fullname, self.rid, "last", p)

    def rel_snap_next(self, subvol):
        p = subvol["path"].replace("/","_")
        return  os.path.join(".osync", self.svc.fullname, self.rid, "next", p)

    def rel_tmp(self, subvol):
        p = self.dst_subvol + subvol["path"][len(self.src_subvol):]
        return  os.path.join(".osync", self.svc.fullname, self.rid, "temp", p)

    def dst_tmp(self, subvol, node):
        p = self.rel_tmp(subvol)
        return os.path.join(self.dst_btrfs[node].rootdir, p)

    def src_snap_last(self, subvol):
        p = self.rel_snap_last(subvol)
        return os.path.join(self.src_btrfs.rootdir, p)

    def src_snap_next(self, subvol):
        p = self.rel_snap_next(subvol)
        return os.path.join(self.src_btrfs.rootdir, p)

    def dst_snap_next(self, subvol, node):
        p = self.rel_snap_next(subvol)
        return os.path.join(self.dst_btrfs[node].rootdir, p)

    def dst_snap_last(self, subvol, node):
        p = self.rel_snap_last(subvol)
        return os.path.join(self.dst_btrfs[node].rootdir, p)

    def get_peersenders(self):
        self.peersenders = set()
        if "nodes" == self.sender:
            self.peersenders |= self.svc.nodes
            self.peersenders -= set([Env.nodename])

    def get_targets(self, action=None):
        self.targets = set()
        if "nodes" in self.target and action in (None, "sync_nodes", "sync_full", "sync_all"):
            self.targets |= self.svc.nodes
        if "drpnodes" in self.target and action in (None, "sync_drp", "sync_full", "sync_all"):
            self.targets |= self.svc.drpnodes
        self.targets -= set([Env.nodename])

    def sync_nodes(self):
        self._sync_update("sync_nodes")

    def sync_drp(self):
        self._sync_update("sync_drp")

    def sanity_checks(self):
        self.pre_sync_check_svc_not_up()
        self.pre_sync_check_flex_primary()

    def btrfs_send(self, subvols, node, incremental=True):
        if len(subvols) == 0:
            return

        send_cmd = ["btrfs", "send"]
        for subvol in subvols:
            if incremental:
                send_cmd += ["-c", self.src_snap_last(subvol)]
            send_cmd += [self.src_snap_next(subvol)]

        if node is not None:
            receive_cmd = Env.rsh.strip(" -n").split() + [node]
            receive_cmd += self.make_dst_workdirs(node) + [";"] + ["btrfs", "receive", self.dst_next_dir(node)]
        else:
            receive_cmd = ["btrfs", "receive", self.dst_next_dir(node)]

        self.log.info(subprocess.list2cmdline(send_cmd) + " | " + subprocess.list2cmdline(receive_cmd))
        p1 = subprocess.Popen(send_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pi = subprocess.Popen(["dd", "bs=4096"], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p2 = subprocess.Popen(receive_cmd, stdin=pi.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
            if buff[1] is not None and len(buff[1]) > 0:
                for line in bdecode(buff[1]).split("\n"):
                    if line:
                        self.log.info("| " + line)
        else:
            if buff[1] is not None and len(buff[1]) > 0:
                for line in bdecode(buff[1]).split("\n"):
                    if line:
                        self.log.error("| " + line)
            self.log.error("sync failed")
            raise ex.Error
        if buff[0] is not None and len(buff[0]) > 0:
            for line in bdecode(buff[0]).split("\n"):
                if line:
                    self.log.info("| " + line)

    def cleanup_remote(self, node, remote_subvols):
        self.init_src_btrfs()
        o = self.get_btrfs(node)
        l = []
        candidates = []
        for subvol in self.subvols():
            candidates.append(self.rel_snap_next(subvol))
            candidates.append(self.rel_tmp(subvol))

        for subvol in remote_subvols:
            if subvol["path"] in candidates:
                l.append(subvol["path"])

        l = [o.rootdir+"/"+p for p in l]

        try:
            o.subvol_delete(l)
        except utilities.subsystems.btrfs.ExecError:
            raise ex.Error()

    def remove_dst_snap_temp(self, node):
        return [subprocess.list2cmdline(["rm", "-rf", self.dst_temp_dir(node)])]

    def remove_dst_snap_next(self, node):
        o = self.get_btrfs(node)
        p = self.dst_next_dir(node)
        subvols = o.get_subvols_in_path(p)
        cmd = o.subvol_delete_cmd(subvols) or []
        if not cmd:
            return []
        return [subprocess.list2cmdline(cmd)]

    def remove_dst_snap_last(self, node):
        o = self.get_btrfs(node)
        p = self.dst_last_dir(node)
        subvols = o.get_subvols_in_path(p)
        cmd = o.subvol_delete_cmd(subvols) or []
        if not cmd:
            return []
        return [subprocess.list2cmdline(cmd)]

    def remove_src_snap_next(self):
        o = self.get_btrfs()
        p = self.src_next_dir()
        subvols = o.get_subvols_in_path(p)
        cmds = []
        for bunch in chunker(subvols, 20):
            cmd = o.subvol_delete_cmd(bunch) or []
            if not cmd:
                continue
            cmds = [subprocess.list2cmdline(cmd)]
            self.do_cmds(cmds)

    def remove_src_snap_last(self):
        o = self.get_btrfs()
        p = self.src_last_dir()
        subvols = o.get_subvols_in_path(p)
        cmd = o.subvol_delete_cmd(subvols) or []
        if not cmd:
            return []
        return [subprocess.list2cmdline(cmd)]

    def remove_dst(self, node):
        o = self.get_btrfs(node)
        p = self.dst
        subvols = o.get_subvols_in_path(p)
        cmd = o.subvol_delete_cmd(subvols) or []
        if not cmd:
            return []
        return [subprocess.list2cmdline(cmd)]

    def rename_src_snap_next(self):
        src = self.src_next_dir()
        dst = self.src_last_dir()
        return [
                subprocess.list2cmdline(["rm", "-rf", dst]),
                subprocess.list2cmdline(["mv", "-v", src, dst]),
        ]

    def rename_dst_snap_next(self, node):
        src = self.dst_next_dir(node)
        dst = self.dst_last_dir(node)
        cmds = [
            subprocess.list2cmdline(["rm", "-rf", dst]),
            subprocess.list2cmdline(["mv", "-v", src, dst]),
        ]
        return cmds

    def install_final(self, node):
        head_subvol = self.subvols()[0]
        src = os.path.join(self.dst_temp_dir(node), head_subvol["path"])
        if protected_dir(self.dst):
            raise ex.Error("%s is a protected dir. refuse to remove" % self.dst)
        cmds = self.remove_dst(node)
        cmds += [
            subprocess.list2cmdline(["rm", "-rf", self.dst]),
            subprocess.list2cmdline(["mv", "-v", src, self.dst]),
        ]
        return cmds

    def install_dst(self, subvols, node):
        cmds = []
        for subvol in subvols:
            src = self.dst_snap_last(subvol, node)
            dst = self.dst_tmp(subvol, node)
            cmd = self.dst_btrfs[node].snapshot_cmd(src, dst, readonly=False)
            if not cmd:
                continue
            cmds.append(subprocess.list2cmdline(["mkdir", "-p", os.path.dirname(dst)]))
            cmds.append(subprocess.list2cmdline(cmd))
        return cmds

    def make_src_workdirs(self):
        makedirs(self.src_last_dir())
        makedirs(self.src_next_dir())
        makedirs(self.src_temp_dir())

    def make_dst_workdirs(self, node):
        return [ "mkdir -p %s %s %s" % (self.dst_last_dir(node), self.dst_next_dir(node), self.dst_temp_dir(node)) ]

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

    def _sync_update(self, action, full=False):
        self.init_src_btrfs()
        try:
            self.sanity_checks()
        except ex.Error:
            return
        self.get_targets(action)
        if len(self.targets) == 0:
            return
        self.get_src_info()
        self.make_src_workdirs()
        subvols = self.subvols()
        src_cmds = []

        for n in self.targets:
            self.get_dst_info(n)
            remote_btrfs = self.get_btrfs(n)
            remote_subvols = self.remote_subvols(n)
            self.cleanup_remote(n, remote_subvols)
            dst_cmds = []
            incrs = []
            fulls = []
            for subvol in subvols:
                src_snap_path = self.src_snap_last(subvol)
                src_snap = self.src_btrfs.get_subvol(src_snap_path)
                dst_snap_path = self.dst_snap_last(subvol, n)
                dst_snap = remote_btrfs.get_subvol(dst_snap_path)
                if not src_snap:
                    self.log.info("upgrade %s to full copy because %s was not found", subvol["path"], src_snap_path)
                    fulls.append(subvol)
                elif not dst_snap:
                    self.log.info("upgrade %s to full copy because %s on %s was not found", subvol["path"], dst_snap_path, n)
                    fulls.append(subvol)
                elif dst_snap["received_uuid"] == "-":
                    self.log.info("upgrade %s to full copy because %s on %s has been turned rw", subvol["path"], dst_snap_path, n)
                    fulls.append(subvol)
                elif src_snap["uuid"] != dst_snap["received_uuid"]:
                    self.log.info("upgrade %s to full copy because %s on %s has been received from a different subset %s", subvol["path"], dst_snap_path, n, dst_snap["received_uuid"])
                    fulls.append(subvol)
                else:
                    incrs.append(subvol)
            dst_cmds += self.remove_dst_snap_last(n)
            dst_cmds += self.remove_dst_snap_temp(n)
            dst_cmds += self.rename_dst_snap_next(n)
            dst_cmds += self.install_dst(subvols, n)
            dst_cmds += self.install_final(n)
            self.btrfs_send(incrs, n, incremental=True)
            self.btrfs_send(fulls, n, incremental=False)
            self.do_cmds(dst_cmds, n)

        src_cmds += self.remove_src_snap_last()
        src_cmds += self.rename_src_snap_next()
        self.do_cmds(src_cmds)
        self.write_statefile()
        for n in self.targets:
            self.push_statefile(n)
        self.write_stats()

    def sync_full(self):
        self._sync_update(None, full=True)

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
            self.status_log("last sync on %s older than %s"%(datetime.datetime.fromtimestamp(last), print_duration(self.sync_max_delay)))
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
