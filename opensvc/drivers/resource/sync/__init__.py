import datetime
import json
import os

import core.exceptions as ex
import core.status
from utilities.converters import convert_speed, print_size
from env import Env
from core.scheduler import SchedOpts
from utilities.lazy import lazy
from core.resource import Resource
from utilities.string import bdecode

def notify(func):
    """
    A decorator in charge of notifying the daemon of sync task
    termination.
    """
    def _func(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        finally:
            self.notify_done()
    return _func

class Sync(Resource):
    default_optional = True

    def __init__(self,
                 sync_max_delay=None,
                 schedule=None,
                 **kwargs):
        self.pausable = True
        if sync_max_delay is None:
            self.sync_max_delay = 1500
        else:
            self.sync_max_delay = sync_max_delay

        if schedule is None:
            self.schedule = "03:59-05:59@121"
        else:
            self.schedule = schedule

        self.stats = {
            "bytes": 0,
            "speed": 0,
            "targets": {},
        }

        Resource.__init__(self, **kwargs)

    def target_nodes(self, target):
        """
        Validate the target (either nodes or drpnodes), and return the
        corresponding set from the parent Svc object properties with the same
        name.
        """
        if target not in ("nodes", "drpnodes"):
            raise ex.Error("invalid target: %s" % target)
        return set([node for node in getattr(self.svc, target)])

    def can_sync(self, target):
        return True

    def check_timestamp(self, ts, comp='more', delay=600):
        """ Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval
        """
        if delay == 0:
            raise ex.Error("sync_max_delay cannot be 0")
        limit = ts + datetime.timedelta(seconds=delay)
        if comp == "more" and datetime.datetime.now() < limit:
            return False
        elif comp == "less" and datetime.datetime.now() < limit:
            return False
        else:
            return True
        return True

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
        try:
            dst = getattr(self, "dst")
        except AttributeError:
            raise ex.Error("the 'dst' attribute is not set")
        try:
            dstfs = getattr(self, "dstfs")
        except AttributeError:
            raise ex.Error("the 'dstfs' attribute is not set")
        if dstfs is None:
            # No dstfs check has been configured. Assume the admin knows better.
            return True
        ruser = self.svc.node.get_ruser(node)
        cmd = Env.rsh.split(' ')+['-l', ruser, node, '--', 'LANG=C', 'df', dstfs]
        (ret, out, err) = self.call(cmd, cache=True, errlog=False)
        if ret != 0:
            raise ex.Error

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
        if dstfs+'(' not in out and dstfs not in out.split():
            self.log.error("The destination fs %s is not mounted on node %s. refuse to sync %s to protect parent fs"%(dstfs, node, dst))
            return False
        return True

    def pre_sync_check_svc_not_up(self):
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if s['avail'].status == core.status.UP:
            return
        if s['avail'].status == core.status.NA and \
           s['overall'].status == core.status.UP:
            return
        if self.svc.options.force and \
           s['avail'].status not in (core.status.DOWN, core.status.NA) and \
           s['overall'].status not in (core.status.DOWN, core.status.NA):
            self.log.info("allow sync, even though reference resources "
                          "aggregated status is %s/%s, because --force is set"
                          "" % (s['avail'], s['overall']))
            return
        if not self.svc.options.cron:
            self.log.info("skip: reference resources aggregated status "
                          "is %s/%s" % (s['avail'], s['overall']))
        raise ex.AbortAction

    def pre_sync_check_flex_primary(self):
        """ Refuse to sync from a flex non-primary node
        """
        if self.svc.topology == "flex" and \
           self.svc.flex_primary != Env.nodename:
            if self.svc.options.cron:
                self.log.debug("won't sync this resource from a flex non-primary node")
            else:
                self.log.info("won't sync this resource from a flex non-primary node")
            raise ex.AbortAction

    def pre_sync_check_prd_svc_on_non_prd_node(self):
        if self.svc.svc_env == 'PRD' and self.svc.node.env != 'PRD':
            if self.svc.options.cron:
                self.log.debug("won't sync a PRD service running on a !PRD node")
            else:
                self.log.info("won't sync a PRD service running on a !PRD node")
            raise ex.AbortAction

    def sync_status(self, *args, **kwargs):
        """
        Placeholder
        """
        return core.status.UNDEF

    def _status(self, **kwargs):
        if self.svc.running_action in ("stop", "shutdown") and not self.svc.command_is_scoped():
            return core.status.NA
        if not self.svc.running_action and self.paused():
            # status eval
            return core.status.NA
        return self.sync_status(**kwargs)

    def paused(self):
        """
        Return True if the aggregated service status is not up, in which
        case we don't care about computing a status for the sync resource.

        Drivers with pausable=False are never paused.
        """
        if not self.pausable:
            return False
        try:
            data = self.svc.node._daemon_status(selector=self.svc.path)
        except Exception:
            data = None
        try:
            paths = data["monitor"]["services"]
        except (KeyError, TypeError):
            # the daemon is not returning proper status data
            paths = {}
        if self.svc.path in paths:
            avail = paths[self.svc.path]["avail"]
            if avail != "up":
                self.status_log("paused, service not up", "info")
                return True
        return False

    @lazy
    def last_stats_file(self):
        return os.path.join(self.var_d, "last_stats")

    def parse_dd(self, buff):
        """
        Extract normalized speed and transfered data size from the dd output
        """
        data = {}
        if not buff:
            return data
        words = bdecode(buff).split()
        if "bytes" in words:
            data["bytes"] = int(words[words.index("bytes")-1])
        if words[-1].endswith("/s"):
            data["speed"] = int(convert_speed("".join(words[-2:])))
        return data

    def update_stats(self, *args, **kwargs):
        try:
            self._update_stats(*args, **kwargs)
        except (KeyError, AttributeError):
            pass

    def _update_stats(self, data, target=None):
        self.log.info("transfered %s at %s",
            print_size(data["bytes"], unit="B"),
            print_size(data["speed"], unit="B")+"/s"
        )
        # aggregate stats
        self.stats["bytes"] += data["bytes"]
        n_targets = len(self.stats["targets"])
        self.stats["speed"] = (data["speed"]*n_targets+data["speed"])/(n_targets+1)
        self.stats["targets"][target] = data

    def load_stats(self):
        try:
            with open(self.last_stats_file, "r") as ofile:
                return json.load(ofile)
        except Exception:
            return {}

    def write_stats(self):
        with open(self.last_stats_file, "w") as ofile:
            json.dump(self.stats, ofile)
        if len(self.stats["targets"]) < 2:
            return
        self.log.info("total transfered %s at %s to %d targets",
            print_size(self.stats["bytes"], unit="B"),
            print_size(self.stats["speed"], unit="B")+"/s",
            len(self.stats["targets"])
        )

    def stats_keys(self):
        stats = self.load_stats()
        data = []
        for key, val in stats.items():
            if not isinstance(val, (int, float)):
                continue
            data.append([key, str(val)])
        return data

    def notify_done(self):
        self.svc.notify_done("sync_all", rids=[self.rid])

    def schedule_options(self):
        return {
            "sync_all": SchedOpts(
                self.rid,
                fname="last_syncall_"+self.rid,
                schedule_option="sync_schedule" if self.rid != "sync#i0" else "sync#i0_schedule"
            )
        }
