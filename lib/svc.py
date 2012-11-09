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
# To change this template, choose Tools | Templates
# and open the template in the editor.

from resources import Resource, ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import justcall
import rcExceptions as ex
import xmlrpcClient
import os
import signal
import lock
import rcLogger
import logging
import datetime
import node

def signal_handler(signum, frame):
    raise ex.excSignal

def dblogger(self, action, begin, end, actionlogfile, sync=False):
    self.node.collector.call('end_action', self, action, begin, end, actionlogfile, sync=sync)
    g_vars, g_vals, r_vars, r_vals = self.svcmon_push_lists()
    self.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals, sync=sync)
    os.unlink(actionlogfile)
    try:
        logging.shutdown()
    except:
        pass

class Options(object):
    def __init__(self):
        self.slave = False
        self.master = False
        self.cron = False
        self.force = False
        self.ignore_affinity = False
        self.debug = False
        self.moduleset = ""
        self.module = ""
        self.ruleset_date = ""
        os.environ['LANG'] = 'C'

class Svc(Resource, Freezer):
    """Service class define a Service Resource
    It contain list of ResourceSet where each ResourceSets contain same resource
    type
    """

    def __init__(self, svcname=None, type="hosted", optional=False, disabled=False, tags=set([])):
        """usage : aSvc=Svc(type)"""
        self.encap = False
        self.options = Options()
        self.node = None
        self.ha = False
        self.sync_dblogger = False
        self.svcname = svcname
        self.vmname = ""
        self.containerize = True
        self.hostid = rcEnv.nodename
        self.resSets = []
        self.type2resSets = {}
        self.disks = set([])
        self.devs = set([])
        self.cron = False
        self.force = False
        self.cluster = False
        self.pathenv = os.path.join(rcEnv.pathetc, self.svcname+'.env')
        self.push_flag = os.path.join(rcEnv.pathvar, svcname+'.push')
        self.status_types = ["container.hpvm",
                             "container.kvm",
                             "container.xen",
                             "container.esx",
                             "container.ovm",
                             "container.lxc",
                             "container.vz",
                             "container.zone",
                             "container.jail",
                             "container.ldom",
                             "container.vbox",
                             "disk.drbd",
                             "disk.loop",
                             "disk.scsireserv",
                             "disk.vg",
                             "disk.zpool",
                             "fs",
                             "ip",
                             "sync.rsync",
                             "sync.symclone",
                             "sync.evasnap",
                             "sync.dcssnap",
                             "sync.dcsckpt",
                             "sync.dds",
                             "sync.zfs",
                             "sync.btrfs",
                             "sync.netapp",
                             "sync.nexenta",
                             "app",
                             "hb.openha",
                             "hb.sg",
                             "hb.rhcs",
                             "hb.ovm",
                             "hb.linuxha"]
        Resource.__init__(self, type=type, optional=optional,
                          disabled=disabled, tags=tags)
        self.log = rcLogger.initLogger(self.svcname.upper())
        Freezer.__init__(self, svcname)
        self.scsirelease = self.prstop
        self.scsireserv = self.prstart
        self.scsicheckreserv = self.prstatus
        self.runmethod = []
        self.resources_by_id = {}
        self.rset_status_cache = None
        self.print_status_fmt = "%-14s %-8s %s"
        self.presync_done = False
        self.presnap_trigger = None
        self.postsnap_trigger = None
        self.lockfd = None
        self.action_start_date = datetime.datetime.now()
        self.monitor_action = None
        self.group_status_cache = None

    def __cmp__(self, other):
        """order by service name
        """
        return cmp(self.svcname, other.svcname)

    def __iadd__(self, r):
        """svc+=aResourceSet
        svc+=aResource
        """
        if r.type in self.type2resSets:
            self.type2resSets[r.type] += r

        elif isinstance(r, ResourceSet):
            self.resSets.append(r)
            self.type2resSets[r.type] = r

        elif isinstance(r, Resource):
            R = ResourceSet(r.type, [r])
            self.__iadd__(R)

        else:
            # Error
            pass

        if isinstance(r, Resource):
            self.resources_by_id[r.rid] = r

        if r.rid in rcEnv.vt_supported:
            self.resources_by_id["container"] = r

        r.svc = self
        import logging
        if r.rid is not None:
            r.log = logging.getLogger(str(self.svcname+'.'+str(r.rid)).upper())
        else:
            r.log = logging.getLogger(str(self.svcname+'.'+str(r.type)).upper())

        if r.type.startswith("hb"):
            self.ha = True

        if hasattr(r, "on_add"):
            r.on_add()

        return self

    def svclock(self, action=None, timeout=30, delay=5):
        suffix = None
        list_actions_no_lock = [
          'push',
          'push_appinfo',
          'print_status',
          'status',
          'freeze',
          'frozen',
          'thaw',
          'get',
          'freezestop',
          'print_disklist',
          'print_devlist',
          'json_status',
          'json_disklist',
          'json_devlist'
        ]
        if action in list_actions_no_lock:
            # no need to serialize this action
            return
        if action.startswith("collector"):
            # no need to serialize collector gets
            return
        if action.startswith("compliance"):
            # compliance modules are allowed to execute actions on the service
            # so give them their own lock
            suffix = "compliance"
        if self.lockfd is not None:
            # already acquired
            return
        lockfile = os.path.join(rcEnv.pathlock, self.svcname)
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            self.log.error("timed out waiting for lock")
            raise ex.excError
        except lock.lockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.excError
        except lock.lockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.excError
        except lock.lockAcquire as e:
            self.log.warn("another action is currently running (pid=%s)"%e.pid)
            raise ex.excError
        except ex.excSignal:
            self.log.error("interrupted by signal")
            raise ex.excError
        except:
            self.log.error("unexpected locking error")
            import traceback
            traceback.print_exc()
            raise ex.excError
        if lockfd is not None:
            self.lockfd = lockfd

    def svcunlock(self):
        lock.unlock(self.lockfd)
        self.lockfd = None

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_resources(self, type=None):
         if type is None:
             rsets = self.resSets
         else:
             rsets = self.get_res_sets(type)

         resources = []
         for rs in rsets:
             for r in rs.resources:
                 if not self.encap and 'encap' in r.tags:
                     continue
                 if r.disabled:
                     continue
                 resources.append(r)
         return resources

    def get_res_sets(self, type):
         if not isinstance(type, list):
             l = [type]
         else:
             l = type
         rsets = []
         for t in l:
             rsets += [ r for r in self.resSets if r.type == t ]
         return rsets

    def has_res_set(self, type):
        if len(self.get_res_sets(type)) > 0:
            return True
        else:
            return False

    def all_set_action(self, action=None, tags=set([])):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.resSets, action=action, tags=tags)

    def sub_set_action(self, type=None, action=None, tags=set([])):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.get_res_sets(type), action=action, tags=tags)

    def need_snap_trigger(self, sets, action):
        if action not in ["syncnodes", "syncdrp", "syncresync", "syncupdate"]:
            return False
        for rs in sets:
            for r in rs.resources:
                """ avoid to run pre/post snap triggers when there is no
                    resource flagged for snap and on drpnodes
                """
                if hasattr(r, "snap") and r.snap is True and \
                   rcEnv.nodename in self.nodes:
                    return True
        return False

    def set_action(self, sets=[], action=None, tags=set([])):
        """ TODO: r.is_optional() not doing what's expected if r is a rset
        """
        list_actions_no_pre_action = [
          "status",
          "print_status",
          "print_disklist",
          "print_devlist",
          "json_disklist",
          "json_devlist",
          "push_appinfo",
          "push",
          "json_status",
          "json_disklist",
          "group_status",
          "presync",
          "postsync",
          "freezestop",
          "resource_monitor"
        ]
        list_actions_no_post_action = list_actions_no_pre_action

        ns = self.need_snap_trigger(sets, action)

        """ snapshots are created in pre_action and destroyed in post_action
            place presnap and postsnap triggers around pre_action
        """
        if ns and self.presnap_trigger is not None:
            (ret, out, err) = self.vcall(self.presnap_trigger)
            if ret != 0:
                raise ex.excError

        for r in sets:
            if action in list_actions_no_pre_action:
                break
            try:
                r.log.debug("start %s pre_action"%r.type)
                r.pre_action(r, action)
            except ex.excError:
                if r.is_optional():
                    pass
                else:
                    raise
            except ex.excAbortAction:
                if r.is_optional():
                    pass
                else:
                    break
            except:
                self.save_exc()
                raise ex.excError

        if ns and self.postsnap_trigger is not None:
            (ret, out, err) = self.vcall(self.postsnap_trigger)
            if ret != 0:
                raise ex.excError

        for r in sets:
            self.log.debug('set_action: action=%s rset=%s'%(action, r.type))
            try:
                r.action(action, tags=tags)
            except ex.excError:
                if r.is_optional():
                    pass
                else:
                    raise
            except ex.excAbortAction:
                if r.is_optional():
                    pass
                else:
                    break

        for r in sets:
            if action in list_actions_no_post_action:
                break
            try:
                r.post_action(r, action)
            except ex.excError:
                if r.is_optional():
                    pass
                else:
                    raise ex.excError
            except ex.excAbortAction:
                if r.is_optional():
                    pass
                else:
                    break
            except:
                self.save_exc()
                raise ex.excError


    def __str__(self):
        output="Service %s available resources:" % (Resource.__str__(self))
        for k in self.type2resSets.keys() : output += " %s" % k
        output+="\n"
        for r in self.resSets:  output+= "  [%s]" % (r.__str__())
        return output

    def status(self):
        """aggregate status a service
        """
        ss = rcStatus.Status()
        for r in self.get_res_sets(self.status_types):
            if not self.encap and 'encap' in r.tags:
                continue
            if "sync." not in r.type:
                ss += r.status()
            else:
                """ sync are expected to be up
                """
                s = r.status()
                if s == rcStatus.UP:
                    ss += rcStatus.UNDEF
                elif s in [rcStatus.NA, rcStatus.UNDEF, rcStatus.TODO]:
                    ss += s
                else:
                    ss += rcStatus.WARN
        if ss.status == rcStatus.STDBY_UP_WITH_UP:
            ss.status = rcStatus.UP
        elif ss.status == rcStatus.STDBY_UP_WITH_DOWN:
            ss.status = rcStatus.STDBY_UP
        return ss.status

    def json_status(self):
        import json
        d = {
              'resources': {},
            }
        try:
            encap_res_status = self.encap_json_status()['resources']
        except:
            encap_res_status = {}

        for rs in self.get_res_sets(self.status_types):
            for r in rs.resources:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                if rid in encap_res_status:
                    status = encap_res_status[rid]['status']
                d['resources'][rid] = {'status': status,
                                       'label': label,
                                       'log':log,
                                       'monitor':monitor,
                                       'disable': disable,
                                       'optional': optional,
                                       'encap': encap}
        ss = self.group_status()
        for g in ss:
            d[g] = str(ss[g])
        print json.dumps(d)

    def print_status(self):
        """print each resource status for a service
        """
        from textwrap import wrap

        def print_res(e, fmt, pfx):
            rid, status, label, log, monitor, disabled, optional, encap = e
            flags = ''
            flags += 'M' if monitor else '.'
            flags += 'D' if disabled else '.'
            flags += 'O' if optional else '.'
            flags += 'E' if encap else '.'
            print fmt%(rid, flags, status, label)
            if len(log) > 0:
                print '\n'.join(wrap(log,
                                     initial_indent = pfx,
                                     subsequent_indent = pfx,
                                     width=78
                                    )
                               )

        print self.svcname
        fmt = "%-20s %4s %-8s %s"
        print fmt%("overall", '', str(self.group_status()['overall']), "\n"),
        fmt = "|- %-17s %4s %-8s %s"
        print fmt%("avail", '', str(self.group_status()['avail']), "\n"),

        try:
            encap_res_status = self.encap_json_status()['resources']
        except:
            encap_res_status = {}

        l = []
        for rs in self.get_res_sets(self.status_types):
            for r in [_r for _r in rs.resources if not _r.rid.startswith('sync') and not _r.rid.startswith('hb')]:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                if rid in encap_res_status:
                    status = rcStatus.Status(rcStatus.status_value(encap_res_status[rid]['status']))
                l.append((rid, status, label, log, monitor, disable, optional, encap))
        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = "|  '- %-14s %4s %-8s %s"
                    pfx = "|     %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "|  |- %-14s %4s %-8s %s"
                    pfx = "|  |  %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)

        fmt = "|- %-17s %4s %-8s %s"
        print fmt%("sync", '', str(self.group_status()['sync']), "\n"),

        l = []
        for rs in self.get_res_sets(self.status_types):
            for r in [_r for _r in rs.resources if _r.rid.startswith('sync')]:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                if rid in encap_res_status:
                    status = rcStatus.Status(rcStatus.status_value(encap_res_status[rid]['status']))
                l.append((rid, status, label, log, monitor, disable, optional, encap))
        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = "|  '- %-14s %4s %-8s %s"
                    pfx = "|     %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "|  |- %-14s %4s %-8s %s"
                    pfx = "|  |  %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)

        fmt = "'- %-17s %4s %-8s %s"
        print fmt%("hb", '', str(self.group_status()['hb']), "\n"),

        l = []
        for rs in self.get_res_sets(self.status_types):
            for r in [_r for _r in rs.resources if _r.rid.startswith('hb')]:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                if rid in encap_res_status:
                    status = rcStatus.Status(rcStatus.status_value(encap_res_status[rid]['status']))
                l.append((rid, status, label, log, monitor, disable, optional, encap))
        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = "   '- %-14s %4s %-8s %s"
                    pfx = "      %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "   |- %-14s %4s %-8s %s"
                    pfx = "   |  %-14s %4s %-8s "%('','','')
                    print_res(e, fmt, pfx)

    def svcmon_push_lists(self, status=None):
        if status is None:
            status = self.group_status()

        try:
            encap_res_status = self.encap_json_status(refresh=True)['resources']
        except:
            encap_res_status = {}

        if self.frozen():
            frozen = "1"
        else:
            frozen = "0"

        r_vars=["svcname",
                "nodename",
                "rid",
                "res_desc",
                "res_status",
                "updated",
                "res_log"]
        r_vals = []
        import datetime
        now = datetime.datetime.now()
        for rs in self.resSets:
            for r in rs.resources:
                if r.rid in encap_res_status:
                    rstatus = encap_res_status[r.rid]['status']
                else:
                    rstatus = rcStatus.status_str(r.rstatus)
                r_vals.append([repr(self.svcname),
                             repr(rcEnv.nodename),
                             repr(r.rid),
                             repr(r.label),
                             repr(str(rstatus)),
                             repr(str(now)),
                             r.status_log_str])

        g_vars=["mon_svcname",
                "mon_svctype",
                "mon_nodname",
                "mon_nodtype",
                "mon_ipstatus",
                "mon_diskstatus",
                "mon_syncstatus",
                "mon_hbstatus",
                "mon_containerstatus",
                "mon_fsstatus",
                "mon_appstatus",
                "mon_availstatus",
                "mon_overallstatus",
                "mon_updated",
                "mon_prinodes",
                "mon_frozen"]
        g_vals=[self.svcname,
                self.svctype,
                rcEnv.nodename,
                rcEnv.host_mode,
                str(status["ip"]),
                str(status["disk"]),
                str(status["sync"]),
                str(status["hb"]),
                str(status["container"]),
                str(status["fs"]),
                str(status["app"]),
                str(status["avail"]),
                str(status["overall"]),
                str(now),
                ' '.join(self.nodes),
                frozen]
        return g_vars, g_vals, r_vars, r_vals

    def get_rset_status(self, groups):
        self.setup_environ()
        rset_status = {}
        for t in self.status_types:
            g = t.split('.')[0]
            if g not in groups:
                continue
            for rs in self.get_res_sets(t):
                rset_status[rs.type] = rs.status()
        return rset_status

    def resource_monitor(self):
        if self.group_status_cache is None:
            self.group_status(excluded_groups=set(['sync']))
        has_hb = False
        for r in self.get_resources(['hb.ovm', 'hb.openha', 'hb.linuxha', 'hb.sg', 'hb.rhcs']):
            if not r.disabled:
                has_hb = True
        if not has_hb:
            self.log.debug("no active heartbeat resource. no need to check monitored resources.")
            return
        hb_status = self.group_status_cache['hb']
        if hb_status.status != rcStatus.UP:
            self.log.debug("heartbeat status is not up. no need to check monitored resources.")
            return

        monitored_resources = []
        for r in self.get_resources():
            if r.monitor:
                monitored_resources.append(r)

        if len(monitored_resources) == 0:
            self.log.debug("no monitored resource")
            return

        for r in monitored_resources:
            if r.rstatus != rcStatus.UP:
                if self.monitor_action is not None and \
                   hasattr(self, self.monitor_action):
                    if len(r.status_log_str) > 0:
                        rstatus_log = ''.join((' ', '(', r.status_log_str.strip().strip("# "), ')'))
                    else:
                        rstatus_log = ''
                    self.log.info("monitored resource %s is in state %s%s"%(r.rid, rcStatus.status_str(r.rstatus), rstatus_log))
                    raise self.exMonitorAction
                else:
                    self.log.info("Would TOC but no (or unknown) resource monitor action set.")
                return

        self.log.debug("monitored resources are up")

    class exMonitorAction(Exception):
        pass

    def reboot(self):
        self.node.os.reboot()

    def crash(self):
        self.node.os.crash()

    def freezestop(self):
        self.sub_set_action('hb.openha', 'freezestop')

    def stonith(self):
        self.sub_set_action('stonith.ilo', 'start')
        self.sub_set_action('stonith.callout', 'start')
        
    def toc(self):
        self.log.info("start monitor action '%s'"%self.monitor_action)
        
    def has_encap_resources(self):
        for t in self.status_types:
            for rs in self.get_res_sets(t):
                if rs.has_encap_resources():
                    return True
        return False 

    def encap_cmd(self, cmd, verbose=False):
        if len(self.runmethod) == 0:
            return None
        if not self.has_encap_resources():
            return None
        cmd = self.runmethod + ['/opt/opensvc/bin/svcmgr', '-s', self.svcname] + cmd
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("failed to execute encap service command: %d\n%s\n%s"%(ret, out, err))
        if verbose:
            self.log.info('logs from %s child service:'%self.vmname)
            print out
            if len(err) > 0:
                print err
        return out, err, ret

    def encap_json_status(self, refresh=False):
        if not refresh and hasattr(self, 'encap_json_status_cache'):
            return self.encap_json_status_cache
        container = self.resources_by_id["container"]
        if self.guestos == 'Windows':
            raise ex.excNotAvailable
        if container.status() == rcStatus.DOWN:
            """
               passive node for the vservice => forge encap resource status
                 - encap sync are n/a
                 - other encap res are down
            """
            gs = {
              'sync': 'n/a',
              'resources': {},
            }
            groups = set(["container", "ip", "disk", "fs", "hb"])
            for g in groups:
                gs[g] = 'down'
            for rs in self.get_res_sets(self.status_types):
                g = rs.type.split('.')[0]
                if g not in groups:
                    continue
                for r in rs.resources:
                    if not self.encap and 'encap' in r.tags:
                        gs['resources'][r.rid] = {'status': 'down'}
            
            groups = set(["app", "sync"])
            for g in groups:
                gs[g] = 'n/a'
            for rs in self.get_res_sets(groups):
                g = rs.type.split('.')[0]
                if g not in groups:
                    continue
                for r in rs.resources:
                    if not self.encap and 'encap' in r.tags:
                        gs['resources'][r.rid] = {'status': 'n/a'}

            return gs

        cmd = ['json', 'status']
        out, err, ret = self.encap_cmd(cmd)
        import json
        gs = json.loads(out)
        self.encap_json_status_cache = gs
        return gs
        
    def group_status(self,
                     groups=set(["container", "ip", "disk", "fs", "sync", "app", "hb"]),
                     excluded_groups=set([])):
        from copy import copy
        """print each resource status for a service
        """
        status = {}
        moregroups = groups | set(["overall", "avail"])
        groups = groups.copy() - excluded_groups
        rset_status = self.get_rset_status(groups)

        # initialise status of each group
        for group in moregroups:
            status[group] = rcStatus.Status(rcStatus.NA)

        try:
            encap_rset_status = self.encap_json_status()
        except:
            encap_rset_status = {}

        for t in [_t for _t in self.status_types if not _t.startswith('sync') and not _t.startswith('hb')]:
            group = t.split('.')[0]
            if group not in groups:
                continue
            for r in self.get_res_sets(t):
                s = rcStatus.Status(rset_status[r.type])
                if not self.encap and group in encap_rset_status:
                    s += rcStatus.Status(rcStatus.status_value(encap_rset_status[group]))
                status[group] += s
                status["avail"] += s

        if status["avail"].status == rcStatus.STDBY_UP_WITH_UP:
            status["avail"].status = rcStatus.UP
            # now that we now the avail status we can promote
            # stdbyup to up
            for g in status:
                if status[g] == rcStatus.STDBY_UP:
                    status[g] = rcStatus.UP
        elif status["avail"].status == rcStatus.STDBY_UP_WITH_DOWN:
            status["avail"].status = rcStatus.STDBY_UP

        # overall status is avail + sync status
        # seed overall with avail
        status["overall"] = copy(status["avail"])

        for t in [_t for _t in self.status_types if _t.startswith('hb')]:
            if 'hb' not in groups:
                continue
            for r in self.get_res_sets(t):
                if not self.encap and 'encap' in r.tags and r.type in encap_rset_status:
                    s = rcStatus.Status(rcStatus.status_value(encap_rset_status[r.type]))
                else:
                    s = rset_status[r.type]
                status['hb'] += s
                status["overall"] += s

        for t in [_t for _t in self.status_types if _t.startswith('sync')]:
            if 'sync' not in groups:
                continue
            for r in self.get_res_sets(t):
                """ sync are expected to be up
                """
                if not self.encap and 'encap' in r.tags and r.type in encap_rset_status:
                    s = rcStatus.Status(rcStatus.status_value(encap_rset_status[r.type]))
                else:
                    s = rset_status[r.type]
                status['sync'] += s
                if s == rcStatus.UP:
                    status["overall"] += rcStatus.UNDEF
                elif s in [rcStatus.NA, rcStatus.UNDEF, rcStatus.TODO]:
                    status["overall"] += s
                else:
                    status["overall"] += rcStatus.WARN

        self.group_status_cache = status
        return status

    def print_disklist(self):
        print '\n'.join(self.disklist())

    def print_devlist(self):
        print '\n'.join(self.devlist())

    def json_disklist(self):
        import json
        print json.dumps(list(self.disklist()))

    def json_devlist(self):
        import json
        print json.dumps(list(self.devlist()))

    def disklist(self):
        if len(self.disks) == 0:
            self.disks = self._disklist()
        return self.disks

    def _disklist(self):
        """List all disks held by all resources of this service
        """
        disks = set()
        for r in self.get_resources():
            disks |= r.disklist()
        self.log.debug("found disks %s held by service" % disks)
        return disks

    def devlist(self):
        if len(self.devs) == 0:
            self.devs = self._devlist()
        return self.devs

    def _devlist(self):
        """List all devs held by all resources of this service
        """
        devs = set()
        for r in self.get_resources():
            devs |= r.devlist()
        self.log.debug("found devs %s held by service" % devs)
        return devs

    def get_non_affine_svc(self):
        if not hasattr(self, "anti_affinity"):
            return []
        self.node.build_services(svcnames=self.anti_affinity)
        running_af_svc = []
        for svc in self.node.svcs:
            if svc.svcname == self.svcname:
                continue
            avail = svc.group_status()['avail']
            if str(avail) != "down":
                running_af_svc.append(svc.svcname)
        return running_af_svc

    def boot(self):
        if rcEnv.nodename in self.autostart_node:
            try:
                self.start()
            except ex.excError:
                self.log.info("start failed. try to start standby")
                self.startstandby()
        else:
            self.cluster = True
            self.startstandby()

    def shutdown(self):
        # don't loose the action log on node shutdown
        self.sync_dblogger = True
        self.force = True
        self.stop()

    def _slave_action(fn):
        def _fn(self):
            if self.encap or not self.has_encap_resources():
                return
            if fn.__name__ not in ('start', 'stop', 'startstandby', 'stopstandby') and \
               (not self.options.master and not self.options.slave):
                raise ex.excAbortAction("specify either --master, --slave or both")
            if self.options.slave or \
               (not self.options.master and not self.options.slave):
                fn(self)
        return _fn

    def _master_action(fn):
        def _fn(self):
            if not self.encap and \
               fn.__name__ not in ('start', 'stop', 'startstandby', 'stopstandby') and \
               self.has_encap_resources() and \
               (not self.options.master and not self.options.slave):
                raise ex.excAbortAction("specify either --master, --slave or both")
            if self.options.master or \
               (not self.options.master and not self.options.slave):
                fn(self)
        return _fn

    def start(self):
        af_svc = self.get_non_affine_svc()
        if len(af_svc) != 0:
            try:
                self.checkip()
            except ex.excError:
                self.log.error("ip address is already up on another host")
                return
            if self.options.ignore_affinity:
                self.log.error("force start of %s on the same node as %s despite anti-affinity settings"%(self.svcname, ', '.join(af_svc)))
            else:
                self.log.error("refuse to start %s on the same node as %s"%(self.svcname, ', '.join(af_svc)))
                return
        self.master_startip()
        self.master_mount()
        self.startcontainer()
        self.master_startapp()
        self.encap_cmd(['start'], verbose=True)
        self.master_starthb()

    def rollback(self):
        self.rollbackhb()
        self.encap_cmd(['rollback'], verbose=True)
        try:
            self.rollbackapp()
        except ex.excError:
            pass
        self.rollbackcontainer()
        self.rollbackmount()
        self.rollbackip()

    def stop(self):
        self.master_stophb()
        self.encap_cmd(['stop'], verbose=True)
        try:
            self.master_stopapp()
        except ex.excError:
            pass
        self.stopcontainer()
        self.master_umount()
        self.master_stopip()

    def cluster_mode_safety_net(self):
        if not self.has_res_set(['hb.ovm', 'hb.openha', 'hb.linuxha', 'hb.sg', 'hb.rhcs']):
            return
        all_disabled = True
        for r in self.get_resources(['hb.ovm', 'hb.openha', 'hb.linuxha', 'hb.sg', 'hb.rhcs']):
            if not r.disabled:
                all_disabled = False
        if all_disabled:
            return
        if not self.cluster:
            self.log.info("this service is managed by a clusterware, thus direct service manipulation is disabled. the --cluster option circumvent this safety net.")
            raise ex.excError

    def starthb(self):
        self.master_starthb()
        self.slave_starthb()

    @_slave_action
    def slave_starthb(self):
        self.encap_cmd(['starthb'], verbose=True)

    @_master_action
    def master_starthb(self):
        self.sub_set_action("hb.ovm", "start")
        self.sub_set_action("hb.openha", "start")
        self.sub_set_action("hb.linuxha", "start")
        self.sub_set_action("hb.sg", "start")
        self.sub_set_action("hb.rhcs", "start")

    def stophb(self):
        self.slave_stophb()
        self.master_stophb()

    @_slave_action
    def slave_stophb(self):
        self.encap_cmd(['stophb'], verbose=True)

    @_master_action
    def master_stophb(self):
        self.sub_set_action("hb.ovm", "stop")
        self.sub_set_action("hb.openha", "stop")
        self.sub_set_action("hb.linuxha", "stop")
        self.sub_set_action("hb.sg", "stop")
        self.sub_set_action("hb.rhcs", "stop")

    def rollbackhb(self):
        self.sub_set_action("hb.ovm", "rollback")
        self.sub_set_action("hb.openha", "rollback")
        self.sub_set_action("hb.linuxha", "rollback")
        self.sub_set_action("hb.sg", "rollback")
        self.sub_set_action("hb.rhcs", "rollback")

    def startdrbd(self):
        self.master_startdrbd()
        self.slave_startdrbd()

    @_slave_action
    def slave_startdrbd(self):
        self.encap_cmd(['startdrbd'], verbose=True)

    @_master_action
    def master_startdrbd(self):
        self.sub_set_action("disk.drbd", "start")

    def stopdrbd(self):
        self.slave_stopdrbd()
        self.master_stopdrbd()

    @_slave_action
    def slave_stopdrbd(self):
        self.encap_cmd(['stopdrbd'], verbose=True)

    @_master_action
    def master_stopdrbd(self):
        self.sub_set_action("disk.drbd", "stop")

    def startloop(self):
        self.master_startloop()
        self.slave_startloop()

    @_slave_action
    def slave_startloop(self):
        self.encap_cmd(['startloop'], verbose=True)

    @_master_action
    def master_startloop(self):
        self.sub_set_action("disk.loop", "start")

    def stoploop(self):
        self.slave_stoploop()
        self.master_stoploop()

    @_slave_action
    def slave_stoploop(self):
        self.encap_cmd(['stoploop'], verbose=True)

    @_master_action
    def master_stoploop(self):
        self.sub_set_action("disk.loop", "stop")

    def stopvg(self):
        self.slave_stopvg()
        self.master_stopvg()

    @_slave_action
    def slave_stopvg(self):
        self.encap_cmd(['stopvg'], verbose=True)

    @_master_action
    def master_stopvg(self):
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.scsireserv", "stop")

    def startvg(self):
        self.master_startvg()
        self.slave_startvg()

    @_slave_action
    def slave_startvg(self):
        self.encap_cmd(['startvg'], verbose=True)

    @_master_action
    def master_startvg(self):
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.vg", "start")

    def startpool(self):
        self.master_startpool()
        self.slave_startpool()

    @_slave_action
    def slave_startpool(self):
        self.encap_cmd(['startpool'], verbose=True)

    @_master_action
    def master_startpool(self):
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.zpool", "start")

    def stoppool(self):
        self.slave_stoppool()
        self.master_stoppool()

    @_slave_action
    def slave_stoppool(self):
        self.encap_cmd(['stoppool'], verbose=True)

    @_master_action
    def master_stoppool(self):
        self.sub_set_action("disk.zpool", "stop")
        self.sub_set_action("disk.scsireserv", "stop")

    def startdisk(self):
        self.master_startdisk()
        self.slave_startdisk()

    @_slave_action
    def slave_startdisk(self):
        self.encap_cmd(['startdisk'], verbose=True)

    @_master_action
    def master_startdisk(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("sync.dcsckpt", "start")
        self.sub_set_action("sync.nexenta", "start")
        self.sub_set_action("sync.symclone", "start")
        self.sub_set_action("disk.loop", "start")
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.drbd", "start", tags=set(['prevg']))
        self.sub_set_action("disk.zpool", "start")
        self.sub_set_action("disk.vg", "start")
        self.sub_set_action("disk.drbd", "start", tags=set(['postvg']))

    def stopdisk(self):
        self.slave_stopdisk()
        self.master_stopdisk()

    @_slave_action
    def slave_stopdisk(self):
        self.encap_cmd(['stopdisk'], verbose=True)

    @_master_action
    def master_stopdisk(self):
        self.sub_set_action("disk.drbd", "stop", tags=set(['postvg']))
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.zpool", "stop")
        self.sub_set_action("disk.drbd", "stop", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "stop")
        self.sub_set_action("disk.loop", "stop")

    def rollbackdisk(self):
        self.sub_set_action("disk.drbd", "rollback", tags=set(['postvg']))
        self.sub_set_action("disk.vg", "rollback")
        self.sub_set_action("disk.zpool", "rollback")
        self.sub_set_action("disk.drbd", "rollback", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "rollback")
        self.sub_set_action("disk.loop", "rollback")

    def checkip(self):
        self.sub_set_action("ip", "check_not_ping_raise")

    def startip(self):
        self.master_startip()
        self.slave_startip()

    @_slave_action
    def slave_startip(self):
        self.encap_cmd(['startip'], verbose=True)

    @_master_action
    def master_startip(self):
        self.sub_set_action("ip", "start")

    def stopip(self):
        self.slave_stopip()
        self.master_stopip()

    @_slave_action
    def slave_stopip(self):
        self.encap_cmd(['stopip'], verbose=True)

    @_master_action
    def master_stopip(self):
        self.sub_set_action("ip", "stop")

    def rollbackip(self):
        self.sub_set_action("ip", "rollback")

    def mount(self):
        self.master_mount()
        self.slave_mount()

    @_master_action
    def master_mount(self):
        self.master_startdisk()
        self.sub_set_action("fs", "start")

    @_slave_action
    def slave_mount(self):
        self.encap_cmd(['mount'], verbose=True)

    def umount(self):
        self.slave_umount()
        self.master_umount()

    @_master_action
    def master_umount(self):
        self.sub_set_action("fs", "stop")
        self.master_stopdisk()

    @_slave_action
    def slave_umount(self):
        self.encap_cmd(['umount'], verbose=True)

    def rollbackmount(self):
        self.sub_set_action("fs", "rollback")
        self.rollbackdisk()

    def startcontainer(self):
        self.sub_set_action("container.lxc", "start")
        self.sub_set_action("container.vz", "start")
        self.sub_set_action("container.jail", "start")
        self.sub_set_action("container.kvm", "start")
        self.sub_set_action("container.xen", "start")
        self.sub_set_action("container.esx", "start")
        self.sub_set_action("container.ovm", "start")
        self.sub_set_action("container.hpvm", "start")
        self.sub_set_action("container.ldom", "start")
        self.sub_set_action("container.vbox", "start")
        self.refresh_ip_status()

    def refresh_ip_status(self):
        """ Used after start/stop container because the ip resource
            status change after its own start/stop
        """
        for r in self.get_resources("ip"):
            r.status(refresh=True)

    def stopcontainer(self):
        self.sub_set_action("container.vbox", "stop")
        self.sub_set_action("container.ldom", "stop")
        self.sub_set_action("container.hpvm", "stop")
        self.sub_set_action("container.xen", "stop")
        self.sub_set_action("container.esx", "stop")
        self.sub_set_action("container.ovm", "stop")
        self.sub_set_action("container.kvm", "stop")
        self.sub_set_action("container.jail", "stop")
        self.sub_set_action("container.lxc", "stop")
        self.sub_set_action("container.vz", "stop")
        self.refresh_ip_status()

    def rollbackcontainer(self):
        self.sub_set_action("container.vbox", "rollback")
        self.sub_set_action("container.ldom", "rollback")
        self.sub_set_action("container.hpvm", "rollback")
        self.sub_set_action("container.xen", "rollback")
        self.sub_set_action("container.esx", "rollback")
        self.sub_set_action("container.ovm", "rollback")
        self.sub_set_action("container.kvm", "rollback")
        self.sub_set_action("container.jail", "rollback")
        self.sub_set_action("container.lxc", "rollback")
        self.sub_set_action("container.vz", "rollback")
        self.refresh_ip_status()

    def provision(self):
        self.sub_set_action("disk.loop", "provision")
        self.sub_set_action("disk.vg", "provision")
        self.sub_set_action("fs", "provision")
        self.sub_set_action("container.lxc", "provision")
        self.sub_set_action("container.kvm", "provision")
        self.sub_set_action("container.zone", "provision")
        self.sub_set_action("ip", "provision")
        self.sub_set_action("fs", "start", tags=set(['postboot']))
        self.push()

    def startapp(self):
        self.master_startapp()
        self.slave_startapp()

    @_slave_action
    def slave_startapp(self):
        self.encap_cmd(['startapp'], verbose=True)

    @_master_action
    def master_startapp(self):
        self.sub_set_action("app", "start")

    def stopapp(self):
        self.slave_stopapp()
        self.master_stopapp()

    @_slave_action
    def slave_stopapp(self):
        self.encap_cmd(['stopapp'], verbose=True)

    @_master_action
    def master_stopapp(self):
        self.sub_set_action("app", "stop")

    def rollbackapp(self):
        self.sub_set_action("app", "rollback")

    def prstop(self):
        self.slave_prstop()
        self.master_prstop()

    @_slave_action
    def slave_prstop(self):
        self.encap_cmd(['prstop'], verbose=True)

    @_master_action
    def master_prstop(self):
        self.sub_set_action("disk.scsireserv", "scsirelease")

    def prstart(self):
        self.master_prstart()
        self.slave_prstart()

    @_slave_action
    def slave_prstart(self):
        self.encap_cmd(['prstart'], verbose=True)

    @_master_action
    def master_prstart(self):
        self.sub_set_action("disk.scsireserv", "scsireserv")

    def prstatus(self):
        self.sub_set_action("disk.scsireserv", "scsicheckreserv")

    def startstandby(self):
        self.sub_set_action("ip", "startstandby")
        self.sub_set_action("disk.loop", "startstandby")
        self.sub_set_action("disk.scsireserv", "startstandby")
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['prevg']))
        self.sub_set_action("disk.vg", "startstandby")
        self.sub_set_action("disk.zpool", "startstandby")
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['postvg']))
        self.sub_set_action("fs", "startstandby")
        self.sub_set_action("app", "startstandby")
        self.encap_cmd(['startstandaby'], verbose=True)

    def postsync(self):
        """ action triggered by a remote master node after
            syncnodes and syncdrp. Typically make use of files
            received in var/
        """
        self.sync_dblogger = True
        self.all_set_action("postsync")

    def remote_postsync(self):
	""" run the remote exec of postsync async because the
            waitlock timeout is long, and we ourselves still
            hold the service lock we want to release early.
	"""
        """ action triggered by a remote master node after
            syncnodes and syncdrp. Typically make use of files
            received in var/.
            use a long waitlock timeout to give a chance to
            remote syncs to finish
        """
        for n in self.need_postsync:
            self.remote_action(n, 'postsync', waitlock=3600)

        self.need_postsync = set([])

    def remote_action(self, node, action, waitlock=60):
        rcmd = [os.path.join(rcEnv.pathetc, self.svcname)]
        if self.cluster:
            rcmd += ['--cluster']
        if self.cron:
            rcmd += ['--cron']
        rcmd += ['--waitlock', str(waitlock), action]
        self.log.info("exec '%s' on node %s"%(' '.join(rcmd), node))
        cmd = rcEnv.rsh.split() + [node] + rcmd
        self.node.cmdworker.enqueue(cmd)

    def presync(self):
        """ prepare files to send to slave nodes in var/.
            Each resource can prepare its own set of files.
        """
        self.need_postsync = set([])
        if self.presync_done:
            return
        self.all_set_action("presync")
        self.presync_done = True

    def syncnodes(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncnodes")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncnodes")
        self.sub_set_action("sync.dds", "syncnodes")
        self.remote_postsync()

    def syncdrp(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncdrp")
        self.sub_set_action("sync.zfs", "syncdrp")
        self.sub_set_action("sync.btrfs", "syncdrp")
        self.sub_set_action("sync.dds", "syncdrp")
        self.remote_postsync()

    def syncswap(self):
        self.sub_set_action("sync.netapp", "syncswap")
        self.sub_set_action("sync.nexenta", "syncswap")

    def syncresume(self):
        self.sub_set_action("sync.netapp", "syncresume")
        self.sub_set_action("sync.dcsckpt", "syncresume")
        self.sub_set_action("sync.nexenta", "syncresume")

    def syncquiesce(self):
        self.sub_set_action("sync.netapp", "syncquiesce")
        self.sub_set_action("sync.nexenta", "syncquiesce")

    def resync(self):
        self.stop()
        self.syncresync()
        self.start()

    def syncresync(self):
        self.sub_set_action("sync.netapp", "syncresync")
        self.sub_set_action("sync.nexenta", "syncresync")
        self.sub_set_action("sync.symclone", "syncresync")
        self.sub_set_action("sync.evasnap", "syncresync")
        self.sub_set_action("sync.dcssnap", "syncresync")
        self.sub_set_action("sync.dds", "syncresync")

    def syncbreak(self):
        self.sub_set_action("sync.netapp", "syncbreak")
        self.sub_set_action("sync.nexenta", "syncbreak")
        self.sub_set_action("sync.symclone", "syncbreak")
        self.sub_set_action("sync.dcsckpt", "syncbreak")

    def syncupdate(self):
        self.sub_set_action("sync.netapp", "syncupdate")
        self.sub_set_action("sync.nexenta", "syncupdate")
        self.sub_set_action("sync.dcsckpt", "syncupdate")
        self.sub_set_action("sync.dds", "syncupdate")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncnodes")

    def syncfullsync(self):
        self.sub_set_action("sync.dds", "syncfullsync")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncnodes")

    def syncverify(self):
        self.sub_set_action("sync.dds", "syncverify")

    def printsvc(self):
        print str(self)

    def can_sync(self, target=None):
        ret = False
        rtypes = ["sync.netapp", "sync.nexenta", "sync.dds", "sync.zfs",
                  "sync.rsync", "sync.btrfs"]
        for rt in rtypes:
            for r in self.get_resources(rt):
                try:
                    ret |= r.can_sync(target)
                except ex.excError:
                    return False
                if ret: return True
        return False

    def syncall(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncnodes")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncnodes")
        self.sub_set_action("sync.dds", "syncnodes")
        self.sub_set_action("sync.rsync", "syncdrp")
        self.sub_set_action("sync.zfs", "syncdrp")
        self.sub_set_action("sync.btrfs", "syncdrp")
        self.sub_set_action("sync.dds", "syncdrp")
        self.syncupdate()
        self.remote_postsync()

    def push_appinfo(self):
        self.node.collector.call('push_appinfo', [self])

    def push(self):
        if self.encap:
            return
        self.push_encap_env()
        self.node.collector.call('push_all', [self])
        print "send %s to collector ... OK"%self.pathenv
        try:
            import time
            with open(self.push_flag, 'w') as f:
                f.write(str(time.time()))
            ret = "OK"
        except:
            ret = "ERR"
        print "update %s timestamp"%self.push_flag, "...", ret

    def push_encap_env(self):
        if self.encap or not self.has_encap_resources():
            return

        cmd = rcEnv.rcp.split() + [self.pathenv, self.vmname+':'+rcEnv.pathetc+'/']
        out, err, ret = justcall(cmd)
        print "send %s to %s ..."%(self.pathenv, self.vmname), "OK" if ret == 0 else "ERR\n%s"%err
        if ret != 0:
            raise ex.excError()

        cmd = ['install', '--envfile', self.pathenv]
        out, err, ret = self.encap_cmd(cmd)
        print "install %s slave service ..."%self.vmname, "OK" if ret == 0 else "ERR\n%s"%err
        if ret != 0:
            raise ex.excError()

    def tag_match(self, rtags, keeptags):
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def disable_resources(self, keeprid=[], keeptags=set([])):
        if len(keeprid) > 0:
            ridfilter = True
        else:
            ridfilter = False

        if len(keeptags) > 0:
            tagsfilter = True
        else:
            tagsfilter = False

        if not tagsfilter and not ridfilter:
            return

        for r in self.get_resources():
            if ridfilter and r.rid in keeprid:
                continue
            if tagsfilter and self.tag_match(r.tags, keeptags):
                continue
            r.disable()

    def setup_environ(self):
        """ Those are available to startup scripts and triggers
        """
        os.environ['OPENSVC_SVCNAME'] = self.svcname
        for r in self.get_resources():
            r.setup_environ()

    def action(self, action, rid=[], tags=set([]), waitlock=60):
        if self.node is None:
            self.node = node.Node()
        self.action_start_date = datetime.datetime.now()
        if self.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
            self.log.error("Abort action for non PRD service on PRD node")
            return 1

        actions_list_allow_on_frozen = [
          'get',
          'set',
          'update',
          'enable',
          'disable',
          'delete',
          'thaw',
          'status',
          'frozen',
          'push',
          'push_appinfo',
          'print_status',
          'print_disklist',
          'print_devlist',
          'json_status',
          'json_disklist'
          'json_devlist'
        ]
        actions_list_allow_on_cluster = actions_list_allow_on_frozen + [
          'resource_monitor',
          'presync',
          'postsync',
          'syncall'
        ]
        if action not in actions_list_allow_on_frozen and \
           'compliance' not in action and \
           'collector' not in action:
            if self.frozen():
                self.log.info("Abort action for frozen service")
                return 1
            try:
                if action not in actions_list_allow_on_cluster:
                    self.cluster_mode_safety_net()
            except ex.excError:
                return 1

        self.setup_environ()
        self.setup_signal_handlers()
        self.disable_resources(keeprid=rid, keeptags=tags)
        actions_list_no_log = [
          'get',
          'set',
          'push',
          'push_appinfo',
          'enable',
          'disable',
          'delete',
          'print_status',
          'print_disklist',
          'print_devlist',
          'json_status',
          'json_disklist',
          'json_devlist',
          'status',
          'group_status',
          'resource_monitor'
        ]
        if action in actions_list_no_log or \
           'compliance' in action or \
           'collector' in action:
            err = self.do_action(action, waitlock=waitlock)
        elif action in ["syncall", "syncdrp", "syncnodes", "syncupdate"]:
            if action == "syncall" or "syncupdate": kwargs = {}
            elif action == "syncnodes": kwargs = {'target': 'nodes'}
            elif action == "syncdrp": kwargs = {'target': 'drpnodes'}
            if self.can_sync(**kwargs):
                err = self.do_logged_action(action, waitlock=waitlock)
            else:
                err = 0
                self.log.debug("nothing to sync for the service for now")
        else:
            err = self.do_logged_action(action, waitlock=waitlock)
        return err

    def do_action(self, action, waitlock=60):
        """Trigger action
        """
        err = 0
        try:
            self.svclock(action, timeout=waitlock)
        except:
            return 1

        try:
            if action.startswith("compliance_"):
                from compliance import Compliance
                o = Compliance(self.node.skip_action, self.options, self.node.collector, self.svcname)
                getattr(o, action)()
            elif action.startswith("collector_"):
                from collector import Collector
                o = Collector(self.options, self.node.collector, self.svcname)
                getattr(o, action)()
            elif hasattr(self, action):
                getattr(self, action)()
            else:
                self.log.error("unsupported action")
                err = 1
        except ex.excAbortAction, e:
            s = "'%s' action stopped on execution error"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.error(s)
            err = 1
        except ex.excError, e:
            s = "'%s' action stopped on execution error"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.error(s)
            err = 1
            if 'start' in action:
                try:
                    self.log.info("trying to rollback %s"%action)
                    self.rollback()
                except:
                    self.log.error("rollback %s failed"%action)
                    pass
        except ex.excSignal:
            self.log.error("interrupted by signal")
            err = 1
        except self.exMonitorAction:
            self.svcunlock()
            raise
        except:
            err = 1
            self.save_exc()

        self.svcunlock()
        return err

    def do_logged_action(self, action, waitlock=60):
        from datetime import datetime
        import tempfile
        import logging
        begin = datetime.now()

        """Provision a database entry to store action log later
        """
        self.node.collector.call('begin_action', self, action, begin, sync=self.sync_dblogger)

        """Per action logfile to push to database at the end of the action
        """
        f = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.pathtmp, prefix=self.svcname+'.'+action)
        actionlogfile = f.name
        f.close()
        log = logging.getLogger()
        actionlogformatter = logging.Formatter("%(asctime)s;;%(name)s;;%(levelname)s;;%(message)s;;%(process)d;;EOL")
        actionlogfilehandler = logging.FileHandler(actionlogfile)
        actionlogfilehandler.setFormatter(actionlogformatter)
        log.addHandler(actionlogfilehandler)

        err = self.do_action(action, waitlock=waitlock)

        """ Push result and logs to database
        """
        actionlogfilehandler.close()
        log.removeHandler(actionlogfilehandler)
        end = datetime.now()
        dblogger(self, action, begin, end, actionlogfile, self.sync_dblogger)
        return err

    def restart(self):
	""" stop then start service
        """
        self.stop()
        self.start()

    def migrate(self):
        if not hasattr(self, "destination_node"):
            self.log.error("a destination node must be provided for the switch action")
            raise ex.excError
        if self.destination_node not in self.nodes:
            self.log.error("destination node %s is not in service node list"%self.destination_node)
            raise ex.excError
        if not hasattr(self, '_migrate'):
            self.log.error("the 'migrate' action is not supported with %s service mode"%self.svcmode)
            raise ex.excError
        self.prstop()
        try:
            self.remote_action(node=self.destination_node, action='mount')
            self._migrate()
        except:
            if self.has_res_set(['disk.scsireserv']):
                self.log.error("scsi reservations where dropped. you have to acquire them now using the 'prstart' action either on source node or destination node, depending on your problem analysis.")
            raise
        self.umount()
	self.remote_action(node=self.destination_node, action='prstart')

    def switch(self):
	""" stop then start service
        """
        if not hasattr(self, "destination_node"):
            self.log.error("a destination node must be provided for the switch action")
            raise ex.excError
        if self.destination_node not in self.nodes:
            self.log.error("destination node %s is not in service node list"%self.destination_node)
            raise ex.excError
        self.stop()
	self.remote_action(node=self.destination_node, action='start')

    def collector_outdated(self):
        """ return True if the env file has changed since last push
            else return False
        """
        import datetime
        if not os.path.exists(self.push_flag):
            self.log.debug("no last push timestamp found")
            return True
        try:
            mtime = os.stat(self.pathenv).st_mtime
            f = open(self.push_flag)
            last_push = float(f.read())
            f.close()
        except:
            self.log.error("can not read timestamp from %s or %s"%(self.pathenv, self.push_flag))
            return True
        if mtime > last_push:
            self.log.debug("env file changed since last push")
            return True
        return False

    def write_config(self):
        try:
            fp = open(self.pathenv, 'w')
            self.config.write(fp)
            fp.close()
        except:
            print >>sys.stderr, "failed to write new %s"%self.nodeconf
            raise ex.excError()

    def load_config(self):
        import ConfigParser
        self.config = ConfigParser.RawConfigParser()
        self.config.read(self.pathenv)

    def unset(self):
        self.load_config()
        import sys
        if self.options.param is None:
            print >>sys.stderr, "no parameter. set --param"
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print >>sys.stderr, "malformed parameter. format as 'section.key'"
            return 1
        section, option = l
        if section != 'DEFAULT' and not self.config.has_section(section):
            print >>sys.stderr, "section '%s' not found"%section
            return 1
        if not self.config.has_option(section, option):
            print >>sys.stderr, "option '%s' not found in section '%s'"%(option, section)
            return 1
        try:
            self.config.remove_option(section, option)
            self.write_config()
        except:
            return 1
        return 0

    def get(self):
        self.load_config()
        import sys
        if self.options.param is None:
            print >>sys.stderr, "no parameter. set --param"
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print >>sys.stderr, "malformed parameter. format as 'section.key'"
            return 1
        section, option = l
        if section != 'DEFAULT' and not self.config.has_section(section):
            print >>sys.stderr, "section '%s' not found"%section
            return 1
        if not self.config.has_option(section, option):
            print >>sys.stderr, "option '%s' not found in section '%s'"%(option, section)
            return 1
        print self.config.get(section, option)
        return 0

    def set(self):
        self.load_config()
        import sys
        if self.options.param is None:
            print >>sys.stderr, "no parameter. set --param"
            return 1
        if self.options.value is None:
            print >>sys.stderr, "no value. set --value"
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print >>sys.stderr, "malformed parameter. format as 'section.key'"
            return 1
        section, option = l
        if section != 'DEFAULT' and not self.config.has_section(section):
            self.config.add_section(section)
        if self.config.has_option(section, option) and \
           self.config.get(section, option) == self.options.value:
            return
        self.config.set(section, option, self.options.value)
        try:
            self.write_config()
        except:
            return 1
        return 0

if __name__ == "__main__" :
    for c in (Svc,) :
        help(c)
    print """s1=Svc("Zone")"""
    s1=Svc("Zone")
    print "s1=",s1
    print """s2=Svc("basic")"""
    s2=Svc("basic")
    print "s2=",s2
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print "s1=",s1
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print """s1+=Resource("fs")"""
    s1+=Resource("fs")
    print """s1+=Resource("fs")"""
    s1+=Resource("fs")
    print "s1=",s1

    print """s1.action("status")"""
    s1.action("status")
