import os
import glob

import core.exceptions as ex
import core.status
import datetime

from .. import Sync, notify
from utilities.converters import convert_speed
from env import Env
from utilities.cache import cache
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which, drop_option
from utilities.string import bdecode

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "rsync"
KEYWORDS = [
    {
        "keyword": "src",
        "convert": "list",
        "at": True,
        "required": True,
        "text": "Source of the sync. Can be a whitespace-separated list of files or dirs passed as-is to rsync. Beware of the meaningful ending '/'. Refer to the rsync man page for details."
    },
    {
        "keyword": "dst",
        "required": True,
        "text": "Destination of the sync. Beware of the meaningful ending '/'. Refer to the rsync man page for details."
    },
    {
        "keyword": "tags",
        "convert": "set",
        "default": set(),
        "default_text": "",
        "example": "delay_snap",
        "at": True,
        "text": "The sync resource supports the :c-tag:`delay_snap` tag. This tag is used to delay the snapshot creation just before the rsync, thus after :kw:`postsnap_trigger` execution. The default behaviour (no tags) is to group all snapshots creation before copying data to remote nodes, thus between :kw:`presnap_trigger` and :kw:`postsnap_trigger`."
    },
    {
        "keyword": "options",
        "convert": "shlex",
        "default": [],
        "default_text": "",
        "example": "--acls --xattrs --exclude foo/bar",
        "at": True,
        "text": "A whitespace-separated list of params passed unchanged to rsync. Typical usage is ACL preservation activation."
    },
    {
        "keyword": "reset_options",
        "convert": "boolean",
        "default": False,
        "at": True,
        "text": "Use options as-is instead of appending options to default hardcoded options. Can be used to disable --xattr or --acls for example."
    },
    {
        "keyword": "target",
        "convert": "list",
        "required": True,
        "candidates": ['nodes', 'drpnodes'],
        "text": "Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
    },
    {
        "keyword": "snap",
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "If set to ``true``, OpenSVC will try to snapshot the first snapshottable parent of the source of the sync and try to sync from the snap."
    },
    {
        "keyword": "dstfs",
        "text": "If set to a remote mount point, OpenSVC will verify that the specified mount point is really hosting a mounted FS. This can be used as a safety net to not overflow the parent FS (may be root)."
    },
    {
        "keyword": "bwlimit",
        "convert": "integer",
        "text": "Bandwidth limit in KB applied to this rsync transfer. Leave empty to enforce no limit. Takes precedence over :kw:`bwlimit` set in [DEFAULT]."
    },
]
DEPRECATED_KEYWORDS = {
    "sync.rsync.exclude": None,
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("rsync"):
        data.append("sync.rsync")

    cmd = ['rsync', '--version']
    out, _, ret = justcall(cmd)
    if ret != 0:
        return data

    if 'no xattrs' not in out:
        data.append("sync.rsync.xattrs")
    if 'no ACLs' not in out:
        data.append("sync.rsync.acls")
    return data


def snap_factory():
    if Env.sysname == 'Linux':
        from utilities.snap.lvm.linux import Snap
    elif Env.sysname == 'HP-UX':
        from utilities.snap.vxfs.hpux import Snap
    elif Env.sysname == 'AIX':
        from utilities.snap.jfs2.aix import Snap
    elif Env.sysname in ['SunOS', 'FreeBSD']:
        from utilities.snap.zfs.sunos import Snap
    elif Env.sysname in ['OSF1']:
        from utilities.snap.advfs.osf1 import Snap
    else:
        raise ex.Error
    return Snap

def get_timestamp_filename(self, node):
    sync_timestamp_f = os.path.join(self.var_d, "last_sync_"+node)
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
                raise ex.Error("malformed --rsync-path value")
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

class SyncRsync(Sync):
    """Defines a rsync job from local node to its remote nodes. Target nodes
    can be restricted to production sibblings or to disaster recovery nodes,
    or both.
    """
    def __init__(self,
                 src=None,
                 dst=None,
                 options=None,
                 target=None,
                 dstfs=None,
                 snap=False,
                 bwlimit=None,
                 internal=False,
                 reset_options=False,
                 **kwargs):
        super(SyncRsync, self).__init__(type="sync.rsync", **kwargs)

        if target is None:
            target = []
        if options is None:
            options = []
        if src is None:
            src = []
        glob_src = []
        for s in src:
            glob_src += glob.glob(s)
        src = glob_src
        if internal:
            if Env.paths.drp_path in dst:
                self.label = "rsync system files to drpnodes"
            else:
                self.label = "rsync svc config to %s"%(', '.join(sorted(sorted(target))))
        else:
            _src = ', '.join(sorted(src))
            if len(_src) > 300:
                _src = _src[0:300]
            _dst = ', '.join(sorted(target))
            self.label = "rsync %s to %s"%(_src, _dst)
        self.src = src
        self.dst = dst
        self.dstfs = dstfs
        self.snap = snap
        self.target = target
        self.bwlimit = bwlimit
        self.internal = internal
        self.timeout = 3600
        self.options = options
        self.reset_options = reset_options

    def __str__(self):
        return "%s src=%s dst=%s options=%s target=%s" % (
            super(SyncRsync, self).__str__(),
            self.src,
            self.dst,
            self.full_options,
            self.target
        )

    def node_need_sync(self, node):
        ts = get_timestamp(self, node)
        return self.alert_sync(ts)

    def can_sync(self, target=None):
        targets = set()
        if target is None:
            targets = self.nodes_to_sync('nodes')
            targets |= self.nodes_to_sync('drpnodes')
        else:
            targets = self.nodes_to_sync(target)

        if len(targets) == 0:
            return False
        return True

    def nodes_to_sync(self, target=None, state="syncable", status=False):
        # Checks are ordered by cost
        if self.is_disabled():
            return set()

        if Env.nodename in self.svc.drpnodes:
            self.log.debug("drp node not allowed to sync nodes nor drpnodes")
            return set()

        self.pre_sync_check_flex_primary()

        if target in self.target:
            targets = self.target_nodes(target)
        else:
            return set()

        # Discard the local node from the set
        targets -= set([Env.nodename])

        if len(targets) == 0:
            return set()

        for node in targets.copy():
            if state == "late" and not self.node_need_sync(node):
                targets.remove(node)
                continue

        if len(targets) == 0:
            return set()

        for node in targets.copy():
            if not status and not self.remote_fs_mounted(node):
                targets.remove(node)
                continue

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
        options = [] + self.full_options
        if ruser != "root":
            options = add_sudo_rsync_path(options)
        options += self.bwlimit_option()
        if '-e' in options:
            return options

        if Env.rsh.startswith("/usr/bin/ssh") and Env.sysname == "SunOS":
            # SunOS "ssh -n" doesn't work with rsync
            rsh = Env.rsh.replace("-n", "")
        else:
            rsh = Env.rsh
        options += ['-e', rsh]
        return options

    def sync_timestamp(self, node):
        sync_timestamp_f = get_timestamp_filename(self, node)
        sync_timestamp_f_src = get_timestamp_filename(self, Env.nodename)
        sched_timestamp_f = os.path.join(self.svc.var_d, "scheduler", "last_syncall_"+self.rid)
        dst_d = os.path.dirname(sched_timestamp_f)
        if not os.path.exists(dst_d):
            os.makedirs(dst_d)
        with open(sync_timestamp_f, 'w') as f:
            f.write(str(self.svc.action_start_date)+'\n')
        import shutil
        shutil.copy2(sync_timestamp_f, sync_timestamp_f_src)
        shutil.copy2(sync_timestamp_f, sched_timestamp_f)
        tsfiles = glob.glob(os.path.join(self.var_d, "last_sync_*"))
        ruser = self.svc.node.get_ruser(node)
        options = self.mangle_options(ruser)
        cmd = ['rsync'] + options
        cmd += ['-R'] + tsfiles + [ruser+'@'+node+':/']
        self.call(cmd)

    def sync(self, target):
        if target not in self.target:
            return

        targets = self.nodes_to_sync(target)

        if len(targets) == 0:
            raise ex.syncNoNodesToSync

        self.add_resource_files_to_sync()

        if "delay_snap" in self.tags:
            if not hasattr(self.rset, 'snaps'):
                Snap = snap_factory()
                self.rset.snaps = Snap(self.rid)
                self.rset.snaps.set_logger(self.log)
            self.rset.snaps.try_snap(self.rset, target, rid=self.rid)

        if hasattr(self, "alt_src") and self.rid != "sync#i0":
            # The pre_action() has provided us with a better source
            # to sync from. Use that
            src = getattr(self, "alt_src")
        else:
            src = self.src

        if len(src) == 0:
            raise ex.syncNoFilesToSync

        for node in targets:
            ruser = self.svc.node.get_ruser(node)
            dst = ruser + '@' + node + ':' + self.dst
            options = self.mangle_options(ruser)
            cmd = ['rsync'] + options + src
            cmd.append(dst)
            if self.rid.startswith("sync#i"):
                ret, out, err = self.call(cmd)
            else:
                ret, out, err = self.vcall(cmd)
            if ret != 0:
                self.log.error("node %s synchronization failed (%s => %s)" % (node, src, dst))
                continue
            self.sync_timestamp(node)
            stats = self.parse_rsync(out)
            self.update_stats(stats, target=node)
            self.remote_postsync(node)

        self.write_stats()

    def parse_rsync(self, buff):
        """
        Extract normalized speed and transfered data size from the dd output
        """
        data = {"bytes": 0, "speed": 0}
        for line in bdecode(buff).splitlines():
            if line.startswith("Total bytes sent"):
                data["bytes"] = int(line.split()[-1].replace(",",""))
            elif line.endswith("/sec"):
                data["speed"] = int(convert_speed(line.split()[-2].replace(",","")+"/s"))
        return data

    def pre_action(self, action):
        """Actions to do before resourceSet iterates through the resources to
           trigger action() on each one
        """

        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        if len(resources) == 0:
            return

        self.pre_sync_check_prd_svc_on_non_prd_node()
        self.pre_sync_check_svc_not_up()

        """ Is there at least one node to sync ?
        """
        targets = set()
        rtargets = {0: set()}
        need_snap = False
        for i, r in enumerate(resources):
            if r.skip or r.is_disabled():
                continue
            rtargets[i] = set()
            if action == "sync_all":
                rtargets[i] |= r.nodes_to_sync('nodes')
                rtargets[i] |= r.nodes_to_sync('drpnodes')
            elif action == "sync_nodes":
                rtargets[i] |= r.nodes_to_sync('nodes')
            elif action == "sync_drp":
                rtargets[i] |= r.nodes_to_sync('drpnodes')
            for node in rtargets[i].copy():
                if r.snap:
                    need_snap = True
        for i in rtargets:
            targets |= rtargets[i]


        if len(targets) == 0:
            if not self.svc.options.cron:
                self.rset.log.info("no nodes to sync")
            raise ex.AbortAction

        if not need_snap:
            self.rset.log.debug("snap not needed")
            return

        Snap = snap_factory()
        try:
            self.rset.snaps = Snap(self.rid)
            self.rset.snaps.set_logger(self.rset.log)
            self.rset.snaps.try_snap(self.rset, action)
        except ex.syncNotSnapable:
            raise ex.Error

    def post_action(self, action):
        """
        Actions to do after resourceSet has iterated through the resources to
        trigger action() on each one
        """
        resources = [r for r in self.rset.resources if \
                     not r.skip and not r.is_disabled() and \
                     r.type == self.type]

        if len(self.rset.resources) == 0:
            return

        if hasattr(self.rset, 'snaps'):
            self.rset.snaps.snap_cleanup(self.rset)

    def remote_postsync(self, nodename):
        """
        Action triggered by a remote master node after sync_nodes and sync_drp.
        Typically make use of files received in var/.
        Use a long waitlock timeout to give a chance to remote syncs to finish.
        """
        self.svc.daemon_service_action(action="postsync", options={"waitlock": "1h"},
                                       server=nodename, sync=False,
                                       collect=False)

    def sync_nodes(self):
        self.pre_sync_check_svc_not_up()
        try:
            self.sync("nodes")
        except ex.syncNoFilesToSync:
            if not self.svc.options.cron:
                self.log.info("no files to sync")
            pass
        except ex.syncNoNodesToSync:
            if not self.svc.options.cron:
                self.log.info("no nodes to sync")
            pass

    def sync_drp(self):
        self.pre_sync_check_svc_not_up()
        try:
            self.sync("drpnodes")
        except ex.syncNoFilesToSync:
            if not self.svc.options.cron:
                self.log.info("no files to sync")
            pass
        except ex.syncNoNodesToSync:
            if not self.svc.options.cron:
                self.log.info("no nodes to sync")
            pass

    def sync_status(self, verbose=False):
        """ mono-node service should return n/a as a sync state
        """
        if len(self.src) == 0:
            self.status_log("no files to sync", "info")
            return core.status.NA

        target = set()
        for i in self.target:
            target |= self.target_nodes(i)
        if len(target - set([Env.nodename])) == 0:
            self.status_log("no destination nodes", "info")
            return core.status.NA

        try:
            options = [] + self.full_options
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN

        """ sync state on nodes where the service is not UP
        """
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if s['avail'].status != core.status.UP or \
           (self.svc.topology == 'flex' and \
            Env.nodename != self.svc.flex_primary and \
            s['avail'].status == core.status.UP):
            if Env.nodename not in target:
                self.status_log("passive node not in destination nodes", "info")
                return core.status.NA
            if self.node_need_sync(Env.nodename):
                self.status_log("passive node needs update")
                return core.status.WARN
            else:
                return core.status.UP

        """ sync state on DRP nodes where the service is UP
        """
        if 'drpnodes' in self.target and Env.nodename in self.target_nodes('drpnodes'):
            self.status_log("service up on drp node, sync disabled", "info")
            return core.status.NA

        """ sync state on nodes where the service is UP
        """
        nodes = []
        nodes += self.nodes_to_sync('nodes', state="late", status=True)
        nodes += self.nodes_to_sync('drpnodes', state="late", status=True)
        if len(nodes) == 0:
            return core.status.UP

        self.status_log("%s need update"%', '.join(sorted(nodes)))
        return core.status.DOWN

    @lazy
    def full_options(self):
        if self.reset_options:
            options = self.options
        else:
            options = ["-HAXpogDtrlvx", "--stats", "--delete", "--force"] + self.options
        if not self.has_capability("sync.rsync.xattrs"):
            options = drop_option("-X", options)
        if not self.has_capability("sync.rsync.acls"):
            options = drop_option("-A", options)
        options += ["--timeout=%s" % self.timeout]
        return options

    def add_resource_files_to_sync(self):
        if self.rid != "sync#i0":
            return
        for resource in self.svc.get_resources():
            self.src += resource.files_to_sync()

    def _info(self):
        self.add_resource_files_to_sync()
        data = [
          ["src", " ".join(self.src)],
          ["dst", self.dst],
          ["dstfs", self.dstfs if self.dstfs else ""],
          ["bwlimit", self.bwlimit if self.bwlimit else ""],
          ["snap", str(self.snap).lower()],
          ["timeout", str(self.timeout)],
          ["target", " ".join(sorted(self.target))],
          ["options", " ".join(self.options)],
          ["reset_options", str(self.reset_options)],
        ]
        data += self.stats_keys()
        return data

    @notify
    def sync_all(self):
        self.sync_nodes()
        self.sync_drp()
