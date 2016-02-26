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

from __future__ import print_function
from resources import Resource, ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import justcall
from svcBuilder import conf_get_string_scope, conf_get_boolean_scope, get_pg_settings
import rcExceptions as ex
import xmlrpcClient
import sys
import os
import signal
import lock
import rcLogger
import logging
import datetime
import node
from rcScheduler import *

def signal_handler(signum, frame):
    raise ex.excSignal

class Options(object):
    def __init__(self):
        self.slaves = False
        self.slave = None
        self.master = False
        self.cron = False
        self.force = False
        self.remote = False
        self.ignore_affinity = False
        self.debug = False
        self.disable_rollback = False
        self.show_disabled = False
        self.moduleset = ""
        self.module = ""
        self.ruleset_date = ""
        self.dry_run = False
        self.refresh = False
        self.parm_rid = None
        self.parm_tags = None
        self.parm_subsets = None
        os.environ['LANG'] = 'C'

class Svc(Resource, Scheduler):
    """Service class define a Service Resource
    It contain list of ResourceSet where each ResourceSets contain same resource
    type
    """

    def __init__(self, svcname=None, type="hosted", optional=False, disabled=False, tags=set([])):
        """usage : aSvc=Svc(type)"""
        self.encap = False
        self.has_encap_resources = False
        self.options = Options()
        self.node = None
        self.ha = False
        self.sync_dblogger = False
        self.svcname = svcname
        self.create_pg = True
        self.hostid = rcEnv.nodename
        self.resSets = []
        self.type2resSets = {}
        self.disks = set([])
        self.devs = set([])
        self.cron = False
        self.force = False
        self.cluster = False
        self.disable_rollback = False
        self.pathenv = os.path.join(rcEnv.pathetc, self.svcname+'.env')
        self.push_flag = os.path.join(rcEnv.pathvar, svcname+'.push')
        self.disk_types = [
         "disk.loop",
         "disk.raw",
         "disk.rados",
         "disk.gandi",
         "disk.drbd",
         "disk.gce",
         "disk.md",
         "disk.zpool",
         "disk.lock",
         "disk.vg",
        ]
        self.status_types = ["container.hpvm",
                             "container.kvm",
                             "container.amazon",
                             "container.openstack",
                             "container.vcloud",
                             "container.xen",
                             "container.esx",
                             "container.ovm",
                             "container.lxc",
                             "container.docker",
                             "container.vz",
                             "container.srp",
                             "container.zone",
                             "container.jail",
                             "container.ldom",
                             "container.vbox",
                             "disk.drbd",
                             "disk.gce",
                             "disk.loop",
                             "disk.gandi",
                             "disk.raw",
                             "disk.rados",
                             "disk.scsireserv",
                             "disk.lock",
                             "disk.vg",
                             "disk.lv",
                             "disk.zpool",
                             "disk.md",
                             "share.nfs",
                             "fs",
                             "ip",
                             "sync.rsync",
                             "sync.symclone",
                             "sync.rados",
                             "sync.symsrdfs",
                             "sync.hp3par",
                             "sync.ibmdssnap",
                             "sync.evasnap",
                             "sync.necismsnap",
                             "sync.btrfssnap",
                             "sync.s3",
                             "sync.dcssnap",
                             "sync.dcsckpt",
                             "sync.dds",
                             "sync.zfs",
                             "sync.btrfs",
                             "sync.docker",
                             "sync.netapp",
                             "sync.nexenta",
                             "app",
                             "stonith.ilo",
                             "stonith.callout",
                             "hb.openha",
                             "hb.sg",
                             "hb.rhcs",
                             "hb.vcs",
                             "hb.ovm",
                             "hb.linuxha"]
        Resource.__init__(self, type=type, optional=optional,
                          disabled=disabled, tags=tags)
        Scheduler.__init__(self)

        self.log = rcLogger.initLogger(self.svcname)
        self.freezer = Freezer(svcname)
        self.scsirelease = self.prstop
        self.scsireserv = self.prstart
        self.scsicheckreserv = self.prstatus
        self.resources_by_id = {}
        self.rset_status_cache = None
        self.presync_done = False
        self.presnap_trigger = None
        self.postsnap_trigger = None
        self.lockfd = None
        self.action_start_date = datetime.datetime.now()
        self.monitor_action = None
        self.group_status_cache = None
        self.config_defaults = {
          'push_schedule': '00:00-06:00@361',
          'sync_schedule': '04:00-06:00@121',
          'comp_schedule': '00:00-06:00@361',
          'mon_schedule': '@9',
          'no_schedule': '',
        }
        self.scheduler_actions = {
         "compliance_auto": SchedOpts("DEFAULT", fname=self.svcname+"_last_comp_check", schedule_option="comp_schedule"),
         "push_env": SchedOpts("DEFAULT", fname=self.svcname+"_last_push_env", schedule_option="push_schedule"),
         "push_service_status": SchedOpts("DEFAULT", fname=self.svcname+"_last_push_service_status", schedule_option="mon_schedule"),
        }

    def __cmp__(self, other):
        """order by service name
        """
        return cmp(self.svcname, other.svcname)

    def scheduler(self):
        self.cron = True
        self.sync_dblogger = True
        for action in self.scheduler_actions:
            try:
                if action == "sync_all":
                    # save the action logging to the collector if sync_all
                    # is not needed
                    self.sched_sync_all()
                else:
                    self.action(action)
            except:
                import traceback
                traceback.print_exc()

    def post_build(self):
        syncs = []
        for r in self.get_resources("sync"):
            syncs += [SchedOpts(r.rid, fname=self.svcname+"_last_syncall_"+r.rid, schedule_option="sync_schedule")]
        if len(syncs) > 0:
            self.scheduler_actions["sync_all"] = syncs

        apps = []
        for r in self.get_resources("app"):
            apps += [SchedOpts(r.rid, fname=self.svcname+"_last_push_appinfo_"+r.rid, schedule_option="push_schedule")]
        if len(apps) > 0:
            self.scheduler_actions["push_appinfo"] = apps

    def purge_status_last(self):
        for rset in self.resSets:
            rset.purge_status_last()

    def get_subset_parallel(self, rtype):
        rtype = rtype.split(".")[0]
        subset_section = 'subset#' + rtype
        if not hasattr(self, "config"):
            self.load_config()
        if not self.config.has_section(subset_section):
            return False
        try:
            return conf_get_boolean_scope(self, self.config, subset_section, "parallel")
        except Exception as e:
            return False

    def __iadd__(self, r):
        """svc+=aResourceSet
        svc+=aResource
        """
        if r.subset is not None:
            # the resource wants to be added to a specific resourceset
            # for action grouping, parallel execution or sub-resource
            # triggers
            base_type = r.type.split(".")[0]
            rtype = "%s:%s" % (base_type, r.subset)
        else:
            rtype = r.type

        if rtype in self.type2resSets:
            # the resource set already exists. add resource or resourceset.
            self.type2resSets[rtype] += r

        elif hasattr(r, 'resources'):
            # new ResourceSet or ResourceSet-derived class
            self.resSets.append(r)
            self.type2resSets[rtype] = r

        elif isinstance(r, Resource):
            parallel = self.get_subset_parallel(rtype)
            if hasattr(r, 'rset_class'):
                R = r.rset_class(type=rtype, resources=[r], parallel=parallel)
            else:
                R = ResourceSet(type=rtype, resources=[r], parallel=parallel)
            R.rid = rtype
            R.svc = self
            R.pg_settings = get_pg_settings(self, "subset#"+rtype)
            self.__iadd__(R)

        else:
            # Error
            pass

        if isinstance(r, Resource):
            self.resources_by_id[r.rid] = r

        r.svc = self
        import logging
        r.log = logging.getLogger(r.log_label())

        if r.type.startswith("hb"):
            self.ha = True

        if not r.disabled and hasattr(r, "on_add"):
            r.on_add()

        return self

    def dblogger(self, action, begin, end, actionlogfile):
        self.node.collector.call('end_action', self, action, begin, end, actionlogfile, sync=self.sync_dblogger)
        g_vars, g_vals, r_vars, r_vals = self.svcmon_push_lists()
        self.node.collector.call('svcmon_update_combo', g_vars, g_vals, r_vars, r_vals, sync=self.sync_dblogger)
        os.unlink(actionlogfile)
        try:
            logging.shutdown()
        except:
            pass

    def svclock(self, action=None, timeout=30, delay=5):
        suffix = None
        list_actions_no_lock = [
          'docker',
          'push',
          'push_env',
          'push_appinfo',
          'print_status',
          'print_resource_status',
          'push_service_status',
          'status',
          'freeze',
          'frozen',
          'thaw',
          'get',
          'freezestop',
          'scheduler',
          'print_schedule',
          'print_env_mtime',
          'print_disklist',
          'print_devlist',
          'print_config',
          'edit_config',
          'json_status',
          'json_disklist',
          'json_devlist',
          'json_env'
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
        elif action.startswith("sync"):
            suffix = "sync"

        if self.lockfd is not None:
            # already acquired
            return
        lockfile = os.path.join(rcEnv.pathlock, self.svcname)
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))

        details = "(timeout %d, delay %d, action %s)" % (timeout, delay, action)
        self.log.debug("acquire service lock %s %s" % (lockfile, details))
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            raise ex.excError("timed out waiting for lock %s" % details)
        except lock.lockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.lockCreateError:
            raise ex.excError("can not create lock file %s %s" % (lockfile, details))
        except lock.lockAcquire as e:
            raise ex.excError("another action is currently running (pid=%s) %s" % (e.pid, details))
        except ex.excSignal:
            raise ex.excError("interrupted by signal %s" % details)
        except:
            import traceback
            traceback.print_exc()
            raise ex.excError("unexpected locking error %s" % details)
        if lockfd is not None:
            self.lockfd = lockfd

    def svcunlock(self):
        lock.unlock(self.lockfd)
        self.lockfd = None

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_resources(self, _type=None, strict=False, discard_disabled=True):
         if _type is None:
             rsets = self.resSets
         else:
             rsets = self.get_res_sets(_type, strict=strict)

         resources = []
         for rs in rsets:
             for r in rs.resources:
                 if not self.encap and 'encap' in r.tags:
                     continue
                 if discard_disabled and r.disabled:
                     continue
                 resources.append(r)
         return resources

    def get_res_sets(self, _type, strict=False):
         if not isinstance(_type, list):
             l = [_type]
         else:
             l = _type
         rsets = {}
         for rs in self.resSets:
             if ':' in rs.type and rs.has_resource_with_types(l, strict=strict):
                 # subset
                 rsets[rs.type] = rs
                 continue
             rs_base_type = rs.type.split(".")[0]
             if rs.type in l:
                 # exact match
                 if rs_base_type not in rsets:
                     rsets[rs_base_type] = type(rs)(type=rs_base_type)
                     rsets[rs_base_type].svc = self
                 rsets[rs_base_type] += rs
             elif rs_base_type in l and not strict:
                 # group match
                 if rs_base_type not in rsets:
                     rsets[rs_base_type] = type(rs)(type=rs_base_type)
                     rsets[rs_base_type].svc = self
                 rsets[rs_base_type] += rs
         rsets = rsets.values()
         rsets.sort()
         return rsets

    def has_res_set(self, type, strict=False):
        if len(self.get_res_sets(type, strict=strict)) > 0:
            return True
        else:
            return False

    def all_set_action(self, action=None, tags=set([])):
        self.set_action(self.resSets, action=action, tags=tags)

    def sub_set_action(self, type=None, action=None, tags=set([]), xtags=set([]), strict=False):
        """ Call action on each member of the subset of specified type
        """
        self.set_action(self.get_res_sets(type, strict=strict), action=action, tags=tags, xtags=xtags)

    def need_snap_trigger(self, sets, action):
        if action not in ["sync_nodes", "sync_drp", "sync_resync", "sync_update"]:
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

    def set_action(self, sets=[], action=None, tags=set([]), xtags=set([]), strict=False):
        """ TODO: r.is_optional() not doing what's expected if r is a rset
        """
        list_actions_no_pre_action = [
          "delete",
          "enable",
          "disable",
          "status",
          'scheduler',
          'pg_freeze',
          'pg_thaw',
          'pg_kill',
          'print_schedule',
          "print_status",
          'print_resource_status',
          "print_disklist",
          "print_devlist",
          'print_config',
          'edit_config',
          "json_disklist",
          "json_devlist",
          "json_status",
          "json_env",
          "push_appinfo",
          "push",
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

        """ Multiple resourcesets of the same type need to be sorted
            so that the start and stop action happen in a predictible order.
            Sort alphanumerically on reseourceset type.

            Exemple, on start:
             app
             app.1
             app.2
            on stop:
             app.2
             app.1
             app
        """
        if "stop" in action or action in ("rollback", "shutdown"):
            reverse = True
        else:
            reverse = False
        sets = sorted(sets, lambda x, y: cmp(x.type, y.type), reverse=reverse)

        for r in sets:
            if action in list_actions_no_pre_action or r.skip:
                break
            try:
                r.log.debug("start %s pre_action"%r.type)
                r.pre_action(r, action)
            except ex.excError:
                raise
            except ex.excAbortAction:
                continue
            except:
                self.save_exc()
                raise ex.excError

        if ns and self.postsnap_trigger is not None:
            (ret, out, err) = self.vcall(self.postsnap_trigger)
            if ret != 0:
                raise ex.excError(err)

        for r in sets:
            self.log.debug('set_action: action=%s rset=%s'%(action, r.type))
            r.action(action, tags=tags, xtags=xtags)

        for r in sets:
            if action in list_actions_no_post_action or r.skip:
                break
            try:
                r.log.debug("start %s post_action"%r.type)
                r.post_action(r, action)
            except ex.excError:
                raise
            except ex.excAbortAction:
                continue
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
        for r in self.get_res_sets(self.status_types, strict=True):
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
              'frozen': self.frozen(),
            }

        containers = self.get_resources('container')
        if len(containers) > 0:
            d['encap'] = {}
            for container in containers:
                if container.name is None or len(container.name) == 0:
                    continue
                try:
                    d['encap'][container.name] = self.encap_json_status(container)
                except:
                    d['encap'][container.name] = {'resources': {}}

        for rs in self.get_res_sets(self.status_types, strict=True):
            for r in rs.resources:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                d['resources'][rid] = {'status': status,
                                       'label': label,
                                       'log':log,
                                       'tags': sorted(list(r.tags)),
                                       'monitor':monitor,
                                       'disable': disable,
                                       'optional': optional,
                                       'encap': encap}
        ss = self.group_status()
        for g in ss:
            d[g] = str(ss[g])
        print(json.dumps(d, indent=4, separators=(',', ': ')))

    def json_env(self):
        import json
        svcenv = {}
        tmp = {}
        self.load_config()
        config = self.config

        defaults = config.defaults()
        for key in defaults.iterkeys():
            tmp[key] = defaults[key]

        svcenv['DEFAULT'] = tmp
        config._defaults = {}

        sections = config.sections()
        for section in sections:
            options = config.options(section)
            tmpsection = {}
            for option in options:
                if config.has_option(section, option):
                    tmpsection[option] = config.get(section, option)
            svcenv[section] = tmpsection
        print(json.dumps(svcenv, indent=4, separators=(',', ': ')))

    def print_resource_status(self):
        if len(self.action_rid) != 1:
            print("only one resource id is allowed", file=sys.stderr)
            return 1
        for rid in self.action_rid:
            if rid not in self.resources_by_id:
                print("resource not found")
                continue
            r = self.resources_by_id[rid]
            print(rcStatus.colorize(rcStatus.status_str(r.status())))
        return 0

    def print_status(self):
        """print() each resource status for a service
        """
        from textwrap import wrap

        def print_res(e, fmt, pfx, subpfx=None):
            if subpfx is None:
                subpfx = pfx
            rid, status, label, log, monitor, disabled, optional, encap = e
            flags = ''
            flags += 'M' if monitor else '.'
            flags += 'D' if disabled else '.'
            flags += 'O' if optional else '.'
            flags += 'E' if encap else '.'
            print(fmt%(rid, flags, rcStatus.colorize(status), label))
            if len(log) > 0:
                print('\n'.join(wrap(log,
                                     initial_indent = subpfx,
                                     subsequent_indent = subpfx,
                                     width=78
                                    )
                               )
                )

        avail_resources = sorted(self.get_resources("ip", discard_disabled=not self.options.show_disabled))
        avail_resources += sorted(self.get_resources("disk", discard_disabled=not self.options.show_disabled))
        avail_resources += sorted(self.get_resources("fs", discard_disabled=not self.options.show_disabled))
        avail_resources += sorted(self.get_resources("container", discard_disabled=not self.options.show_disabled))
        avail_resources += sorted(self.get_resources("share", discard_disabled=not self.options.show_disabled))
        avail_resources += sorted(self.get_resources("app", discard_disabled=not self.options.show_disabled))
        accessory_resources = sorted(self.get_resources("hb", discard_disabled=not self.options.show_disabled))
        accessory_resources += sorted(self.get_resources("stonith", discard_disabled=not self.options.show_disabled))
        accessory_resources += sorted(self.get_resources("sync", discard_disabled=not self.options.show_disabled))
        n_accessory_resources = len(accessory_resources)

        print(self.svcname)
        frozen = 'frozen' if self.frozen() else ''
        fmt = "%-20s %4s %-10s %s"
        print(fmt%("overall", '', rcStatus.colorize(self.group_status()['overall']), frozen))
        if n_accessory_resources == 0:
            fmt = "'- %-17s %4s %-10s %s"
            head_c = " "
        else:
            fmt = "|- %-17s %4s %-10s %s"
            head_c = "|"
        print(fmt%("avail", '', rcStatus.colorize(self.group_status()['avail']), ''))

        encap_res_status = {}
        for container in self.get_resources('container'):
            try:
                js = self.encap_json_status(container)
                encap_res_status[container.rid] = js["resources"]
                if js.get("frozen", False):
                    container.status_log("frozen")
            except ex.excNotAvailable as e:
                encap_res_status[container.rid] = {}
            except Exception as e:
                print(e)
                encap_res_status[container.rid] = {}

        l = []
        cr = {}
	for r in avail_resources:
            rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
            l.append((rid, status, label, log, monitor, disable, optional, encap))
            if rid.startswith("container") and rid in encap_res_status:
                _l = []
                for _rid, val in encap_res_status[rid].items():
                    _l.append((_rid, val['status'], val['label'], val['log'], val['monitor'], val['disable'], val['optional'], val['encap']))
                cr[rid] = _l

        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = head_c+"  '- %-14s %4s %-10s %s"
                    pfx = head_c+"     %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = head_c+"  |- %-14s %4s %-10s %s"
                    pfx = head_c+"  |  %-14s %4s %-10s "%('','','')
                    if e[0] in cr and len(cr[e[0]]) > 0:
                        subpfx = head_c+"  |  |  %-11s %4s %-10s "%('','','')
                    else:
                        subpfx = None
                    print_res(e, fmt, pfx, subpfx=subpfx)
                if e[0] in cr:
                    _last = len(cr[e[0]]) - 1
                    if _last >= 0:
                        for _i, _e in enumerate(cr[e[0]]):
                            if _i == _last:
                                fmt = head_c+"  |  '- %-11s %4s %-10s %s"
                                pfx = head_c+"  |     %-11s %4s %-10s "%('','','')
                                print_res(_e, fmt, pfx)
                            else:
                                fmt = head_c+"  |  |- %-11s %4s %-10s %s"
                                pfx = head_c+"  |  |  %-11s %4s %-10s "%('','','')
                                print_res(_e, fmt, pfx)

        if n_accessory_resources > 0:
            fmt = "'- %-17s %4s %-10s %s"
            print(fmt%("accessory", '', '', ''))

        l = []
        for r in accessory_resources:
            rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
            if rid in encap_res_status:
                status = rcStatus.Status(rcStatus.status_value(encap_res_status[rid]['status']))
            l.append((rid, status, label, log, monitor, disable, optional, encap))

        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = "   '- %-14s %4s %-10s %s"
                    pfx = "      %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "   |- %-14s %4s %-10s %s"
                    pfx = "   |  %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)

    def svcmon_push_lists(self, status=None):
        if status is None:
            status = self.group_status()

        if self.frozen():
            frozen = "1"
        else:
            frozen = "0"

        r_vars=["svcname",
                "nodename",
                "vmname",
                "rid",
                "res_type",
                "res_desc",
                "res_status",
                "res_monitor",
                "res_optional",
                "res_disable",
                "updated",
                "res_log"]
        r_vals = []
        import datetime
        now = datetime.datetime.now()

        for rs in self.resSets:
            for r in rs.resources:
                if 'encap' in r.tags:
                    continue
                rstatus = rcStatus.status_str(r.rstatus)
                r_vals.append([repr(self.svcname),
                               repr(rcEnv.nodename),
                               "",
                               repr(r.rid),
                               repr(r.type),
                               repr(r.label),
                               repr(str(rstatus)),
                               "1" if r.monitor else "0",
                               "1" if r.optional else "0",
                               "1" if r.disabled else "0",
                               repr(str(now)),
                               r.status_log_str])

        g_vars=["mon_svcname",
                "mon_svctype",
                "mon_nodname",
                "mon_vmname",
                "mon_vmtype",
                "mon_nodtype",
                "mon_ipstatus",
                "mon_diskstatus",
                "mon_syncstatus",
                "mon_hbstatus",
                "mon_containerstatus",
                "mon_fsstatus",
                "mon_sharestatus",
                "mon_appstatus",
                "mon_availstatus",
                "mon_overallstatus",
                "mon_updated",
                "mon_prinodes",
                "mon_frozen"]

        containers = self.get_resources('container')
        if len(containers) == 0:
            g_vals=[self.svcname,
                    self.svctype,
                    rcEnv.nodename,
                    "",
                    "hosted",
                    rcEnv.host_mode,
                    str(status["ip"]),
                    str(status["disk"]),
                    str(status["sync"]),
                    str(status["hb"]),
                    str(status["container"]),
                    str(status["fs"]),
                    str(status["share"]),
                    str(status["app"]),
                    str(status["avail"]),
                    str(status["overall"]),
                    str(now),
                    ' '.join(self.nodes),
                    frozen]
        else:
            g_vals = []
            for container in containers:
                encap_res_status = {}
                try:
                    encap_res_status = self.encap_json_status(container)
                except ex.excNotAvailable as e:
                    encap_res_status = {'resources': [],
                                        'ip': 'n/a',
                                        'disk': 'n/a',
                                        'sync': 'n/a',
                                        'hb': 'n/a',
                                        'container': 'n/a',
                                        'fs': 'n/a',
                                        'share': 'n/a',
                                        'app': 'n/a',
                                        'avail': 'n/a',
                                        'overall': 'n/a'}
                except Exception as e:
                    print(e)
                    continue

                for rid in encap_res_status['resources']:
                    rstatus = encap_res_status['resources'][rid]['status']
                    r_vals.append([repr(self.svcname),
                                   repr(rcEnv.nodename),
                                   repr(container.name),
                                   repr(str(r.type)),
                                   repr(str(rid)),
                                   repr(str(encap_res_status['resources'][rid]['label'])),
                                   repr(str(rstatus)),
                                   "1" if encap_res_status['resources'][rid].get('monitor', False) else "0",
                                   "1" if encap_res_status['resources'][rid].get('optional', False) else "0",
                                   "1" if encap_res_status['resources'][rid].get('disabled', False) else "0",
                                   repr(str(now)),
                                   repr(str(encap_res_status['resources'][rid]['log']))])

                if 'avail' not in status or 'avail' not in encap_res_status:
                    continue

                g_vals.append([self.svcname,
                               self.svctype,
                               rcEnv.nodename,
                               container.name,
                               container.type.replace('container.', ''),
                               rcEnv.host_mode,
                               str(status["ip"]+rcStatus.Status(encap_res_status['ip'])),
                               str(status["disk"]+rcStatus.Status(encap_res_status['disk'])),
                               str(status["sync"]+rcStatus.Status(encap_res_status['sync'])),
                               str(status["hb"]+rcStatus.Status(encap_res_status['hb'])),
                               str(status["container"]+rcStatus.Status(encap_res_status['container'])),
                               str(status["fs"]+rcStatus.Status(encap_res_status['fs'])),
                               str(status["share"]+rcStatus.Status(encap_res_status['share'] if 'share' in encap_res_status else 'n/a')),
                               str(status["app"]+rcStatus.Status(encap_res_status['app'])),
                               str(status["avail"]+rcStatus.Status(encap_res_status['avail'])),
                               str(status["overall"]+rcStatus.Status(encap_res_status['overall'])),
                               str(now),
                               ' '.join(self.nodes),
                               frozen,
                               str(container.name)])

        return g_vars, g_vals, r_vars, r_vals

    def get_rset_status(self, groups):
        self.setup_environ()
        rset_status = {}
        for t in self.status_types:
            g = t.split('.')[0]
            if g not in groups:
                continue
            for rs in self.get_res_sets(t, strict=True):
                if rs.type not in rset_status:
                    rset_status[rs.type] = rs.status()
                else:
                    rset_status[rs.type] = rcStatus._merge(rset_status[rs.type], rs.status())
        return rset_status

    def resource_monitor(self):
        self.options.refresh = True
        if self.group_status_cache is None:
            self.group_status(excluded_groups=set(['sync']))
        if not self.ha:
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

        for r in monitored_resources:
            if r.rstatus not in (rcStatus.UP, rcStatus.STDBY_UP, rcStatus.NA):
                if len(r.status_log_str) > 0:
                    rstatus_log = ''.join((' ', '(', r.status_log_str.strip().strip("# "), ')'))
                else:
                    rstatus_log = ''
                self.log.info("monitored resource %s is in state %s%s"%(r.rid, rcStatus.status_str(r.rstatus), rstatus_log))

                if self.monitor_action is not None and \
                   hasattr(self, self.monitor_action):
                    raise self.exMonitorAction
                else:
                    self.log.info("Would TOC but no (or unknown) resource monitor action set.")
                return

        for container in self.get_resources('container'):
            try:
                encap_status = self.encap_json_status(container)
                res = encap_status["resources"]
            except Exception as e:
                encap_status = {}
                res = {}
            if encap_status.get("frozen"):
                continue
            for rid, r in res.items():
                if not r.get("monitor"):
                    continue
                erid = rid+"@"+container.name
                monitored_resources.append(erid)
                if r.get("status") not in ("up", "n/a"):
                    if len(r.get("log")) > 0:
                        rstatus_log = ''.join((' ', '(', r.get("log").strip().strip("# "), ')'))
                    else:
                        rstatus_log = ''
                    self.log.info("monitored resource %s is in state %s%s"%(erid, r.get("status"), rstatus_log))

                    if self.monitor_action is not None and \
                       hasattr(self, self.monitor_action):
                        raise self.exMonitorAction
                    else:
                        self.log.info("Would TOC but no (or unknown) resource monitor action set.")
                    return

        if len(monitored_resources) == 0:
            self.log.debug("no monitored resource")
        else:
            rids = ','.join([r if type(r) in (str, unicode) else r.rid for r in monitored_resources])
            self.log.debug("monitored resources are up (%s)" % rids)

    class exMonitorAction(Exception):
        pass

    def reboot(self):
        self.node.os.reboot()

    def crash(self):
        self.node.os.crash()

    def pg_freeze(self):
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_freeze')
            self.sub_set_action('container', '_pg_freeze')
        else:
            self._pg_freeze()
            for r in self.get_resources(["app", "container"]):
                r.status(refresh=True, restart=False)

    def pg_thaw(self):
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_thaw')
            self.sub_set_action('container', '_pg_thaw')
        else:
            self._pg_thaw()
            for r in self.get_resources(["app", "container"]):
                r.status(refresh=True, restart=False)

    def pg_kill(self):
        if self.command_is_scoped():
            self.sub_set_action('app', '_pg_kill')
            self.sub_set_action('container', '_pg_kill')
        else:
            self._pg_kill()
            for r in self.get_resources(["app", "container"]):
                r.status(refresh=True, restart=False)

    def freezestop(self):
        self.sub_set_action('hb.openha', 'freezestop')

    def stonith(self):
        self.sub_set_action('stonith.ilo', 'start')
        self.sub_set_action('stonith.callout', 'start')

    def toc(self):
        self.log.info("start monitor action '%s'"%self.monitor_action)
        getattr(self, self.monitor_action)()

    def encap_cmd(self, cmd, verbose=False, error="raise"):
        for container in self.get_resources('container'):
            try:
                out, err, ret = self._encap_cmd(cmd, container, verbose=verbose)
            except ex.excEncapUnjoignable as e:
                if error != "continue":
                    self.log.error("container %s is not joinable to execute action '%s'"%(container.name, ' '.join(cmd)))
                    raise
                elif verbose:
                    self.log.warning("container %s is not joinable to execute action '%s'"%(container.name, ' '.join(cmd)))

    def _encap_cmd(self, cmd, container, verbose=False):
        if container.pg_frozen():
            raise ex.excError("can't join a frozen container. abort encap command.")
        vmhostname = container.vm_hostname()
        try:
            autostart_node = conf_get_string_scope(self, self.config, 'DEFAULT', 'autostart_node', impersonate=vmhostname).split()
        except:
            autostart_node = []
        if cmd == ["start"] and container.booted and vmhostname in autostart_node:
            self.log.info("skip encap service start in container %s: already started on boot"%vmhostname)
            return '', '', 0
        if not self.has_encap_resources:
            self.log.debug("skip encap %s: no encap resource" % ' '.join(cmd))
            return '', '', 0
        if not container.is_up():
            self.log.info("skip encap %s: the container is not running here" % ' '.join(cmd))
            return '', '', 0

        if self.options.slave is not None and not \
           (container.name in self.options.slave or \
            container.rid in self.options.slave):
            # no need to run encap cmd (container not specified in --slave)
            return '', '', 0

        if cmd == ['start'] and not self.need_start_encap(container):
            self.log.info("skip start in container %s: the encap service is configured to start on container boot."%container.name)
            return '', '', 0

        # now we known we'll execute a command in the slave, so purge the encap cache
        self.purge_cache_encap_json_status(container.rid)

        options = []
        if self.options.dry_run:
            options.append('--dry-run')
        if self.options.refresh:
            options.append('--refresh')
        if self.options.disable_rollback:
            options.append('--disable-rollback')
        if self.options.parm_rid:
            options.append('--rid')
            options.append(self.options.parm_rid)
        if self.options.parm_tags:
            options.append('--tags')
            options.append(self.options.parm_tags)
        if self.options.parm_subsets:
            options.append('--subsets')
            options.append(self.options.parm_subsets)

        cmd = ['/opt/opensvc/bin/svcmgr', '-s', self.svcname] + options + cmd

        if container is not None and hasattr(container, "rcmd"):
            out, err, ret = container.rcmd(cmd)
        elif hasattr(container, "runmethod"):
            cmd = container.runmethod + cmd
            out, err, ret = justcall(cmd)
        else:
            raise ex.excEncapUnjoignable("undefined rcmd/runmethod in resource %s"%container.rid)

        if verbose:
            self.log.info('logs from %s child service:'%container.name)
            print(out)
            if len(err) > 0:
                print(err)
        if ret != 0:
            raise ex.excError("error from encap service command '%s': %d\n%s\n%s"%(' '.join(cmd), ret, out, err))
        return out, err, ret

    def get_encap_json_status_path(self, rid):
        return os.path.join(rcEnv.pathvar, self.svcname, "encap.status."+rid)

    def purge_cache_encap_json_status(self, rid):
        if hasattr(self, "encap_json_status_cache") and rid in self.encap_json_status_cache:
            del(self.encap_json_status_cache[rid])
        path = self.get_encap_json_status_path(rid)
        if os.path.exists(path):
            os.unlink(path)

    def put_cache_encap_json_status(self, rid, data):
        if not hasattr(self, 'encap_json_status_cache'):
            self.encap_json_status_cache = {}
        self.encap_json_status_cache[rid] = data
        path = self.get_encap_json_status_path(rid)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with open(path, 'w') as f:
                 gs = f.write(json.dumps(data))
        except:
            os.unlink(path)

    def get_cache_encap_json_status(self, rid):
        if hasattr(self, "encap_json_status_cache") and rid in self.encap_json_status_cache:
            return self.encap_json_status_cache[rid]
        path = self.get_encap_json_status_path(rid)
        try:
            with open(path, 'r') as f:
                 gs = json.loads(f.read())
        except:
            gs = None
        return gs

    def encap_json_status(self, container, refresh=False):
        if container.guestos == 'windows':
            raise ex.excNotAvailable
        if container.status() == rcStatus.DOWN:
            """
               passive node for the vservice => forge encap resource status
                 - encap sync are n/a
                 - other encap res are down
            """
            gs = {
              'avail': 'down',
              'overall': 'down',
              'resources': {},
            }
            groups = set(["container", "ip", "disk", "fs", "share", "hb"])
            for g in groups:
                gs[g] = 'down'
            for rs in self.get_res_sets(self.status_types, strict=True):
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

        if not refresh and not self.options.refresh:
            gs = self.get_cache_encap_json_status(container.rid)
            if gs:
                return gs

        gs = {
          'avail': 'n/a',
          'overall': 'n/a',
          'resources': {},
        }
        groups = set(["container", "ip", "disk", "fs", "share", "hb", "app", "sync"])
        for g in groups:
            gs[g] = 'n/a'

        cmd = ['json', 'status']
        try:
            out, err, ret = self._encap_cmd(cmd, container)
        except ex.excError as e:
            return gs
        except Exception as e:
            print(e)
            return gs

        import json
        try:
            gs = json.loads(out)
        except:
            pass

        self.put_cache_encap_json_status(container.rid, gs)

        return gs

    def group_status(self,
                     groups=set(["container", "ip", "disk", "fs", "share", "sync", "app", "hb", "stonith"]),
                     excluded_groups=set([])):
        from copy import copy
        """print() each resource status for a service
        """
        status = {}
        moregroups = groups | set(["overall", "avail"])
        groups = groups.copy() - excluded_groups
        rset_status = self.get_rset_status(groups)

        # initialise status of each group
        for group in moregroups:
            status[group] = rcStatus.Status(rcStatus.NA)

        for t in [_t for _t in self.status_types if not _t.startswith('sync') and not _t.startswith('hb') and not _t.startswith('stonith')]:
            if t in excluded_groups:
                continue
            group = t.split('.')[0]
            if group not in groups:
                continue
            for r in self.get_resources(t):
                s = r.status()
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

        # overall status is avail + accessory resources status
        # seed overall with avail
        status["overall"] = copy(status["avail"])

        for t in [_t for _t in self.status_types if _t.startswith('stonith')]:
            if 'stonith' not in groups:
                continue
            if t in excluded_groups:
                continue
            for r in self.get_resources(t):
                s = r.status()
                status['stonith'] += s
                status["overall"] += s

        for t in [_t for _t in self.status_types if _t.startswith('hb')]:
            if 'hb' not in groups:
                continue
            if t in excluded_groups:
                continue
            for r in self.get_resources(t):
                s = r.status()
                status['hb'] += s
                status["overall"] += s

        for t in [_t for _t in self.status_types if _t.startswith('sync')]:
            if 'sync' not in groups:
                continue
            if t in excluded_groups:
                continue
            for r in self.get_resources(t):
                """ sync are expected to be up
                """
                s = r.status()
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
        print('\n'.join(self.disklist()))

    def print_devlist(self):
        print('\n'.join(self.devlist()))

    def json_disklist(self):
        import json
        print(json.dumps(list(self.disklist()), indent=4, separators=(',', ': ')))

    def json_devlist(self):
        import json
        print(json.dumps(list(self.devlist()), indent=4, separators=(',', ': ')))

    def disklist(self):
        if len(self.disks) == 0:
            self.disks = self._disklist()
        return self.disks

    def _disklist(self):
        """List all disks held by all resources of this service
        """
        disks = set()
        for r in self.get_resources():
            if r.skip:
                continue
            disks |= r.disklist()
        self.log.debug("found disks %s held by service" % disks)
        return disks

    def devlist(self, filtered=True):
        if len(self.devs) == 0:
            self.devs = self._devlist(filtered=filtered)
        return self.devs

    def _devlist(self, filtered=True):
        """List all devs held by all resources of this service
        """
        devs = set()
        for r in self.get_resources():
            if filtered and r.skip:
                continue
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

    def print_env_mtime(self):
        mtime = os.stat(self.pathenv).st_mtime
        print(mtime)

    def need_start_encap(self, container):
        self.load_config()
        defaults = self.config.defaults()
        if defaults.get('autostart_node@'+container.name) in (container.name, 'encapnodes'):
            return False
        elif defaults.get('autostart_node@encapnodes') in (container.name, 'encapnodes'):
            return False
        elif defaults.get('autostart_node') in (container.name, 'encapnodes'):
            return False
        return True

    def boot(self):
        if rcEnv.nodename not in self.autostart_node:
            self.startstandby()
            return

        l = self.get_resources('hb')
        if len(l) > 0:
            self.log.warning("cluster nodes should not be in autostart_nodes for HA configuration")
            self.startstandby()
            return

        try:
            self.start()
        except ex.excError as e:
            self.log.error(str(e))
            self.log.info("start failed. try to start standby")
            self.startstandby()

    def shutdown(self):
        self.force = True
        self.master_shutdownhb()
        self.slave_shutdown()
        try:
            self.master_shutdownapp()
        except ex.excError:
            pass
        self.shutdowncontainer()
        self.master_shutdownshare()
        self.master_shutdownfs()
        self.master_shutdownip()

    def command_is_scoped(self):
        if self.options.parm_rid is not None or \
           self.options.parm_tags is not None or \
           self.options.parm_subsets is not None:
            return True
        return False

    def _slave_action(fn):
        def _fn(self):
            if self.encap or not self.has_encap_resources:
                return
            if (self.command_is_scoped() or self.running_action not in ('migrate', 'boot', 'shutdown', 'prstart', 'prstop', 'restart', 'start', 'stop', 'startstandby', 'stopstandby')) and \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                raise ex.excError("specify either --master, --slave(s) or both (%s)"%fn.__name__)
            if self.options.slaves or \
               self.options.slave is not None or \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                try:
                    fn(self)
                except Exception as e:
                    raise ex.excError(str(e))
        return _fn

    def _master_action(fn):
        def _fn(self):
            if not self.encap and \
               (self.command_is_scoped() or self.running_action not in ('migrate', 'boot', 'shutdown', 'restart', 'start', 'stop', 'startstandby', 'stopstandby')) and \
               self.has_encap_resources and \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                raise ex.excError("specify either --master, --slave(s) or both (%s)"%fn.__name__)
            if self.options.master or \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                fn(self)
        return _fn

    def start(self):
        self.master_starthb()
        self.abort_start()
        af_svc = self.get_non_affine_svc()
        if len(af_svc) != 0:
            if self.options.ignore_affinity:
                self.log.error("force start of %s on the same node as %s despite anti-affinity settings"%(self.svcname, ', '.join(af_svc)))
            else:
                self.log.error("refuse to start %s on the same node as %s"%(self.svcname, ', '.join(af_svc)))
                return
        self.master_startip()
        self.master_startfs()
        self.master_startshare()
        self.master_startcontainer()
        self.master_startapp()
        self.slave_start()

    @_slave_action
    def slave_start(self):
        self.encap_cmd(['start'], verbose=True)

    def rollback(self):
        self.encap_cmd(['rollback'], verbose=True)
        try:
            self.rollbackapp()
        except ex.excError:
            pass
        self.rollbackcontainer()
        self.rollbackshare()
        self.rollbackfs()
        self.rollbackip()

    def stop(self):
        self.master_stophb()
        self.slave_stop()
        try:
            self.master_stopapp()
        except ex.excError:
            pass
        self.stopcontainer()
        self.master_stopshare()
        self.master_stopfs()
        self.master_stopip()

    @_slave_action
    def slave_shutdown(self):
        self.encap_cmd(['shutdown'], verbose=True, error="continue")

    @_slave_action
    def slave_stop(self):
        self.encap_cmd(['stop'], verbose=True, error="continue")

    def cluster_mode_safety_net(self, action):
        if not self.has_res_set(['hb.ovm', 'hb.openha', 'hb.linuxha', 'hb.sg', 'hb.rhcs', 'hb.vcs']):
            return
        if self.command_is_scoped():
            self.log.debug('stop: called with --rid, --tags or --subset, allow action on ha service.')
            return
        n_hb = 0
        n_hb_enabled = 0
        for r in self.get_resources('hb', discard_disabled=False):
            n_hb += 1
            if not r.disabled:
                n_hb_enabled += 1
        if n_hb > 0 and n_hb_enabled == 0 and self.cluster:
            raise ex.excAbortAction("this service has heartbeat resources, but all disabled. this state is interpreted as a maintenance mode. actions submitted with --cluster are not allowed to inhibit actions triggered by the heartbeat daemon.")
        if n_hb_enabled == 0:
            return
        if not self.cluster:
            for r in self.get_resources("hb"):
                if not r.skip and hasattr(r, action):
                    getattr(r, action)()
            raise ex.excError("this service is managed by a clusterware, thus direct service manipulation is disabled. the --cluster option circumvent this safety net.")

    def starthb(self):
        self.master_starthb()
        self.slave_starthb()

    @_slave_action
    def slave_starthb(self):
        self.encap_cmd(['starthb'], verbose=True)

    @_master_action
    def master_starthb(self):
        self.master_hb('start')

    @_master_action
    def master_startstandbyhb(self):
        self.master_hb('startstandby')

    @_master_action
    def master_shutdownhb(self):
        self.master_hb('shutdown')

    @_master_action
    def master_stophb(self):
        self.master_hb('stop')

    def master_hb(self, action):
        self.sub_set_action("hb", action)

    def stophb(self):
        self.slave_stophb()
        self.master_stophb()

    @_slave_action
    def slave_stophb(self):
        self.encap_cmd(['stophb'], verbose=True)

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
        self.sub_set_action("disk.lock", "stop")
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    def startvg(self):
        self.master_startvg()
        self.slave_startvg()

    @_slave_action
    def slave_startvg(self):
        self.encap_cmd(['startvg'], verbose=True)

    @_master_action
    def master_startvg(self):
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action("disk.lock", "start")
        self.sub_set_action("disk.vg", "start")

    def startpool(self):
        self.master_startpool()
        self.slave_startpool()

    @_slave_action
    def slave_startpool(self):
        self.encap_cmd(['startpool'], verbose=True)

    @_master_action
    def master_startpool(self):
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action("disk.zpool", "start", xtags=set(['zone']))

    def stoppool(self):
        self.slave_stoppool()
        self.master_stoppool()

    @_slave_action
    def slave_stoppool(self):
        self.encap_cmd(['stoppool'], verbose=True)

    @_master_action
    def master_stoppool(self):
        self.sub_set_action("disk.zpool", "stop", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    def startdisk(self):
        self.master_startdisk()
        self.slave_startdisk()

    @_slave_action
    def slave_startdisk(self):
        self.encap_cmd(['startdisk'], verbose=True)

    @_master_action
    def master_startstandbydisk(self):
        self.sub_set_action("sync.netapp", "startstandby")
        self.sub_set_action("sync.dcsckpt", "startstandby")
        self.sub_set_action("sync.nexenta", "startstandby")
        self.sub_set_action("sync.symclone", "startstandby")
        self.sub_set_action("sync.ibmdssnap", "startstandby")
        self.sub_set_action("disk.scsireserv", "startstandby", xtags=set(['zone']))
        self.sub_set_action(self.disk_types, "startstandby", xtags=set(['zone']))

    @_master_action
    def master_startdisk(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("sync.dcsckpt", "start")
        self.sub_set_action("sync.nexenta", "start")
        self.sub_set_action("sync.symclone", "start")
        self.sub_set_action("sync.symsrdfs", "start")
        self.sub_set_action("sync.hp3par", "start")
        self.sub_set_action("sync.ibmdssnap", "start")
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action(self.disk_types, "start", xtags=set(['zone']))

    def stopdisk(self):
        self.slave_stopdisk()
        self.master_stopdisk()

    @_slave_action
    def slave_stopdisk(self):
        self.encap_cmd(['stopdisk'], verbose=True)

    @_master_action
    def master_stopdisk(self):
        self.sub_set_action("sync.btrfssnap", "stop")
        self.sub_set_action(self.disk_types, "stop", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))

    @_master_action
    def master_shutdowndisk(self):
        self.sub_set_action("sync.btrfssnap", "shutdown")
        self.sub_set_action(self.disk_types, "shutdown", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "shutdown", xtags=set(['zone']))

    def rollbackdisk(self):
        self.sub_set_action(self.disk_types, "rollback", xtags=set(['zone']))
        self.sub_set_action("disk.scsireserv", "rollback", xtags=set(['zone']))

    def abort_start(self):
        for r in self.get_resources():
            if r.skip or r.disabled:
                continue
            if hasattr(r, 'abort_start') and r.abort_start():
                raise ex.excError("start aborted due to resource %s conflict"%r.rid)

    def startip(self):
        self.master_startip()
        self.slave_startip()

    @_slave_action
    def slave_startip(self):
        self.encap_cmd(['startip'], verbose=True)

    @_master_action
    def master_startstandbyip(self):
        self.sub_set_action("ip", "startstandby", xtags=set(['zone', 'docker']))

    @_master_action
    def master_startip(self):
        self.sub_set_action("ip", "start", xtags=set(['zone', 'docker']))

    def stopip(self):
        self.slave_stopip()
        self.master_stopip()

    @_slave_action
    def slave_stopip(self):
        self.encap_cmd(['stopip'], verbose=True)

    @_master_action
    def master_stopip(self):
        self.sub_set_action("ip", "stop", xtags=set(['zone', 'docker']))

    @_master_action
    def master_shutdownip(self):
        self.sub_set_action("ip", "shutdown", xtags=set(['zone', 'docker']))

    def rollbackip(self):
        self.sub_set_action("ip", "rollback", xtags=set(['zone', 'docker']))

    def startshare(self):
        self.master_startshare()
        self.slave_startshare()

    @_master_action
    def master_startshare(self):
        self.sub_set_action("share.nfs", "start")

    @_master_action
    def master_startstandbyshare(self):
        self.sub_set_action("share", "startstandby")

    @_slave_action
    def slave_startshare(self):
        self.encap_cmd(['startshare'], verbose=True)

    def stopshare(self):
        self.slave_stopshare()
        self.master_stopshare()

    @_master_action
    def master_stopshare(self):
        self.sub_set_action("share", "stop")

    @_master_action
    def master_shutdownshare(self):
        self.sub_set_action("share", "shutdown")

    @_slave_action
    def slave_stopshare(self):
        self.encap_cmd(['stopshare'], verbose=True)

    def rollbackshare(self):
        self.sub_set_action("share", "rollback")

    def startfs(self):
        self.master_startfs()
        self.slave_startfs()

    @_master_action
    def master_startfs(self):
        self.master_startdisk()
        self.sub_set_action("fs", "start", xtags=set(['zone']))

    @_master_action
    def master_startstandbyfs(self):
        self.master_startstandbydisk()
        self.sub_set_action("fs", "startstandby", xtags=set(['zone']))

    @_slave_action
    def slave_startfs(self):
        self.encap_cmd(['startfs'], verbose=True)

    def stopfs(self):
        self.slave_stopfs()
        self.master_stopfs()

    @_master_action
    def master_stopfs(self):
        self.sub_set_action("fs", "stop", xtags=set(['zone']))
        self.master_stopdisk()

    @_master_action
    def master_shutdownfs(self):
        self.sub_set_action("fs", "shutdown", xtags=set(['zone']))
        self.master_shutdowndisk()

    @_slave_action
    def slave_stopfs(self):
        self.encap_cmd(['stopfs'], verbose=True)

    def rollbackfs(self):
        self.sub_set_action("fs", "rollback", xtags=set(['zone']))
        self.rollbackdisk()

    def startcontainer(self):
        self.abort_start()
        self.master_startcontainer()

    @_master_action
    def master_startstandbycontainer(self):
        self.sub_set_action("container", "startstandby")
        self.refresh_ip_status()

    @_master_action
    def master_startcontainer(self):
        self.sub_set_action("container", "start")
        self.refresh_ip_status()

    def refresh_ip_status(self):
        """ Used after start/stop container because the ip resource
            status change after its own start/stop
        """
        for r in self.get_resources("ip"):
            r.status(refresh=True, restart=False)

    @_master_action
    def shutdowncontainer(self):
        self.sub_set_action("container", "shutdown")
        self.refresh_ip_status()

    @_master_action
    def stopcontainer(self):
        self.sub_set_action("container", "stop")
        self.refresh_ip_status()

    def rollbackcontainer(self):
        self.sub_set_action("container", "rollback")
        self.refresh_ip_status()

    def provision(self):
        self.sub_set_action("ip", "provision", xtags=set(['zone', 'docker']))
        self.sub_set_action("disk", "provision", xtags=set(['zone']))
        self.sub_set_action("fs", "provision", xtags=set(['zone']))
        self.sub_set_action("container", "provision")
        self.push()

    def startapp(self):
        self.master_startapp()
        self.slave_startapp()

    @_slave_action
    def slave_startapp(self):
        self.encap_cmd(['startapp'], verbose=True)

    @_master_action
    def master_startstandbyapp(self):
        self.sub_set_action("app", "startstandby")

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

    @_master_action
    def master_shutdownapp(self):
        self.sub_set_action("app", "shutdown")

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
        self.master_startstandby()
        self.slave_startstandby()

    @_master_action
    def master_startstandby(self):
        self.master_startstandbyip()
        self.master_startstandbyfs()
        self.master_startstandbyshare()
        self.master_startstandbycontainer()
        self.master_startstandbyapp()

    @_slave_action
    def slave_startstandby(self):
        cmd = ['startstandby']
        for container in self.get_resources('container'):
            if not container.is_up() and not rcEnv.nodename in container.always_on:
                # no need to try to startstandby the encap service on a container we not activated
                continue
            try:
                out, err, ret = self._encap_cmd(cmd, container, verbose=True)
            except ex.excError:
                self.log.error("container %s is not joinable to execute action '%s'"%(container.name, ' '.join(cmd)))
                raise

    def postsync(self):
        """ action triggered by a remote master node after
            sync_nodes and sync_drp. Typically make use of files
            received in var/
        """
        self.all_set_action("postsync")

    def remote_postsync(self):
        """ release the svc lock at this point because the
            waitlock timeout is long and we are done touching
            local data.
        """
        """ action triggered by a remote master node after
            sync_nodes and sync_drp. Typically make use of files
            received in var/.
            use a long waitlock timeout to give a chance to
            remote syncs to finish
        """
        self.svcunlock()
        for n in self.need_postsync:
            self.remote_action(n, 'postsync', waitlock=3600)

        self.need_postsync = set([])

    def remote_action(self, node, action, waitlock=60, sync=False):
        if self.cron:
            # the scheduler action runs forked. don't use the cmdworker
            # in this context as it may hang
            sync = True

        rcmd = [os.path.join(rcEnv.pathetc, self.svcname)]
	if self.log.isEnabledFor(logging.DEBUG):
	    rcmd += ['--debug']
        if self.cluster:
            rcmd += ['--cluster']
        if self.cron:
            rcmd += ['--cron']
        rcmd += ['--waitlock', str(waitlock)] + action.split()
        cmd = rcEnv.rsh.split() + [node] + rcmd
        self.log.info("exec '%s' on node %s"%(' '.join(rcmd), node))
        if sync:
            out, err, ret = justcall(cmd)
            return out, err, ret
        else:
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

    def sync_nodes(self):
        if not self.can_sync('nodes'):
            return
        self.presync()
        self.sub_set_action("sync.rsync", "sync_nodes")
        self.sub_set_action("sync.zfs", "sync_nodes")
        self.sub_set_action("sync.btrfs", "sync_nodes")
        self.sub_set_action("sync.docker", "sync_nodes")
        self.sub_set_action("sync.dds", "sync_nodes")
        self.sub_set_action("sync.symsrdfs", "sync_nodes")
        self.remote_postsync()

    def sync_drp(self):
        if not self.can_sync('drpnodes'):
            return
        self.presync()
        self.sub_set_action("sync.rsync", "sync_drp")
        self.sub_set_action("sync.zfs", "sync_drp")
        self.sub_set_action("sync.btrfs", "sync_drp")
        self.sub_set_action("sync.docker", "sync_drp")
        self.sub_set_action("sync.dds", "sync_drp")
        self.sub_set_action("sync.symsrdfs", "sync_drp")
        self.remote_postsync()

    def syncswap(self):
        self.sub_set_action("sync.netapp", "syncswap")
        self.sub_set_action("sync.symsrdfs", "syncswap")
        self.sub_set_action("sync.hp3par", "syncswap")
        self.sub_set_action("sync.nexenta", "syncswap")

    def sync_revert(self):
        self.sub_set_action("sync.hp3par", "sync_revert")

    def sync_resume(self):
        self.sub_set_action("sync.netapp", "sync_resume")
        self.sub_set_action("sync.symsrdfs", "sync_resume")
        self.sub_set_action("sync.hp3par", "sync_resume")
        self.sub_set_action("sync.dcsckpt", "sync_resume")
        self.sub_set_action("sync.nexenta", "sync_resume")

    def sync_quiesce(self):
        self.sub_set_action("sync.netapp", "sync_quiesce")
        self.sub_set_action("sync.nexenta", "sync_quiesce")

    def resync(self):
        self.stop()
        self.sync_resync()
        self.start()

    def sync_resync(self):
        self.sub_set_action("sync.netapp", "sync_resync")
        self.sub_set_action("sync.nexenta", "sync_resync")
        self.sub_set_action("sync.symclone", "sync_resync")
        self.sub_set_action("sync.rados", "sync_resync")
        self.sub_set_action("sync.ibmdssnap", "sync_resync")
        self.sub_set_action("sync.evasnap", "sync_resync")
        self.sub_set_action("sync.necismsnap", "sync_resync")
        self.sub_set_action("sync.dcssnap", "sync_resync")
        self.sub_set_action("sync.dds", "sync_resync")

    def sync_break(self):
        self.sub_set_action("sync.netapp", "sync_break")
        self.sub_set_action("sync.nexenta", "sync_break")
        self.sub_set_action("sync.symclone", "sync_break")
        self.sub_set_action("sync.hp3par", "sync_break")
        self.sub_set_action("sync.ibmdssnap", "sync_break")
        self.sub_set_action("sync.dcsckpt", "sync_break")

    def sync_update(self):
        if not self.can_sync():
            return
        self._sync_update()

    def _sync_update(self):
        self.sub_set_action("sync.netapp", "sync_update")
        self.sub_set_action("sync.hp3par", "sync_update")
        self.sub_set_action("sync.nexenta", "sync_update")
        self.sub_set_action("sync.dcsckpt", "sync_update")
        self.sub_set_action("sync.dds", "sync_update")
        self.sub_set_action("sync.zfs", "sync_nodes")
        self.sub_set_action("sync.btrfssnap", "sync_update")
        self.sub_set_action("sync.s3", "sync_update")

    def sync_full(self):
        self.sub_set_action("sync.dds", "sync_full")
        self.sub_set_action("sync.zfs", "sync_nodes")
        self.sub_set_action("sync.btrfs", "sync_full")
        self.sub_set_action("sync.s3", "sync_full")

    def sync_restore(self):
        self.sub_set_action("sync.s3", "sync_restore")

    def sync_split(self):
        self.sub_set_action("sync.symsrdfs", "sync_split")

    def sync_establish(self):
        self.sub_set_action("sync.symsrdfs", "sync_establish")

    def sync_verify(self):
        self.sub_set_action("sync.dds", "sync_verify")

    def print_config(self):
        try:
            with open(self.pathenv, 'r') as f:
                print(f.read())
        except Exception as e:
            print(s, file=sys.stderr)

    def edit_config(self):
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        from rcUtilities import which
        if not which(editor):
            print("%s not found" % editor, file=sys.stderr)
            return 1
        return os.system(' '.join((editor, self.pathenv)))

    def can_sync(self, target=None):
        ret = False
        rtypes = ["sync.netapp", "sync.nexenta", "sync.dds", "sync.zfs",
                  "sync.rsync", "sync.docker", "sync.btrfs", "sync.hp3par"]
        for rt in rtypes:
            for r in self.get_resources(rt):
                try:
                    ret |= r.can_sync(target)
                except ex.excError as e:
                    return False
                if ret: return True
        self.log.debug("nothing to sync for the service for now")
        return False

    def sched_sync_all(self):
        data = self.skip_action("sync_all", deferred_write_timestamp=True)
        if len(data["keep"]) == 0:
            return
        self._sched_sync_all(data["keep"])

    @scheduler_fork
    def _sched_sync_all(self, sched_options):
        self.action("sync_all", rid=[o.section for o in sched_options])
        self.sched_write_timestamp(sched_options)

    def sync_all(self):
        if not self.can_sync():
            return
        if self.cron:
            self.sched_delay()
        self.presync()
        self.sub_set_action("sync.rsync", "sync_nodes")
        self.sub_set_action("sync.zfs", "sync_nodes")
        self.sub_set_action("sync.btrfs", "sync_nodes")
        self.sub_set_action("sync.docker", "sync_nodes")
        self.sub_set_action("sync.dds", "sync_nodes")
        self.sub_set_action("sync.symsrdfs", "sync_nodes")
        self.sub_set_action("sync.rsync", "sync_drp")
        self.sub_set_action("sync.zfs", "sync_drp")
        self.sub_set_action("sync.btrfs", "sync_drp")
        self.sub_set_action("sync.docker", "sync_drp")
        self.sub_set_action("sync.dds", "sync_drp")
        self._sync_update()
        self.remote_postsync()

    def push_service_status(self):
        if self.skip_action("push_service_status"):
            return
        self.task_push_service_status()

    @scheduler_fork
    def task_push_service_status(self):
        if self.cron:
            self.sched_delay()
        import rcSvcmon
        self.options.refresh = True
        rcSvcmon.svcmon_normal([self])

    def push_appinfo(self):
        data = self.skip_action("push_appinfo", deferred_write_timestamp=True)
        if len(data["keep"]) == 0:
            return
        self.task_push_appinfo()

    @scheduler_fork
    def task_push_appinfo(self):
        if self.cron:
            self.sched_delay()
        self.node.collector.call('push_appinfo', [self])
        self.sched_write_timestamp(self.scheduler_actions["push_appinfo"])

    def push_env(self):
        if self.skip_action("push_env"):
            return
        self.push()

    @scheduler_fork
    def push(self):
        if self.encap:
            return
        if self.cron:
            self.sched_delay()
        self.push_encap_env()
        self.node.collector.call('push_all', [self])
        self.log.handlers[1].setLevel(logging.CRITICAL)
        self.log.info("send %s to collector" % self.pathenv)
        try:
            import time
            with open(self.push_flag, 'w') as f:
                f.write(str(time.time()))
            self.log.info("update %s timestamp" % self.push_flag)
            self.log.handlers[1].setLevel(logging.INFO)
        except:
            self.log.error("failed to update %s timestamp" % self.push_flag)
            self.log.handlers[1].setLevel(logging.INFO)

    def push_encap_env(self):
        if self.encap or not self.has_encap_resources:
            return

        for r in self.get_resources('container'):
            if r.status() not in (rcStatus.STDBY_UP, rcStatus.UP):
                continue
            self._push_encap_env(r)

    def _push_encap_env(self, r):
        cmd = ['print', 'env', 'mtime']
        try:
            out, err, ret = self._encap_cmd(cmd, r)
        except ex.excError:
            ret = 1
        if ret == 0:
            encap_mtime = int(float(out.strip()))
            local_mtime = int(os.stat(self.pathenv).st_mtime)
            if encap_mtime > local_mtime:
                if hasattr(r, 'rcp_from'):
                    out, err, ret = r.rcp_from(self.pathenv, rcEnv.pathetc+'/')
                else:
                    cmd = rcEnv.rcp.split() + [r.name+':'+self.pathenv, rcEnv.pathetc+'/']
                    out, err, ret = justcall(cmd)
                os.utime(self.pathenv, (encap_mtime, encap_mtime))
                print("fetch %s from %s ..."%(self.pathenv, r.name), "OK" if ret == 0 else "ERR\n%s"%err)
                if ret != 0:
                    raise ex.excError()
                return
            elif encap_mtime == local_mtime:
                return

        if hasattr(r, 'rcp'):
            out, err, ret = r.rcp(self.pathenv, rcEnv.pathetc+'/')
        else:
            cmd = rcEnv.rcp.split() + [self.pathenv, r.name+':'+rcEnv.pathetc+'/']
            out, err, ret = justcall(cmd)
        self.log.handlers[1].setLevel(logging.CRITICAL)
        if ret != 0:
            self.log.error("failed to send %s to %s" % (self.pathenv, r.name))
            self.log.handlers[0].setLevel(logging.INFO)
            raise ex.excError()
        self.log.info("send %s to %s" % (self.pathenv, r.name))

        cmd = ['install', '--envfile', self.pathenv]
        out, err, ret = self._encap_cmd(cmd, container=r)
        if ret != 0:
            self.log.error("failed to install %s slave service" % r.name)
            self.log.handlers[1].setLevel(logging.INFO)
            raise ex.excError()
        self.log.info("install %s slave service" % r.name)
        self.log.handlers[1].setLevel(logging.INFO)

    def tag_match(self, rtags, keeptags):
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def set_skip_resources(self, keeprid=[], xtags=set([])):
        if len(keeprid) > 0:
            ridfilter = True
        else:
            ridfilter = False

        if len(xtags) > 0:
            tagsfilter = True
        else:
            tagsfilter = False

        if not tagsfilter and not ridfilter:
            return

        for r in self.get_resources():
            if self.tag_match(r.tags, xtags):
                r.skip = True
            if ridfilter and r.rid in keeprid:
                continue
            r.skip = True

    def setup_environ(self, action=None):
        """ Those are available to startup scripts and triggers
        """
        os.environ['OPENSVC_SVCNAME'] = self.svcname
        if action:
            os.environ['OPENSVC_ACTION'] = action
        for r in self.get_resources():
            r.setup_environ()

    def expand_rid(self, rid):
        l = set([])
        for e in self.resources_by_id.keys():
            if e is None:
                continue
            if '#' not in e:
                if e == rid:
                    l.add(e)
                else:
                    continue
            elif e[:e.index('#')] == rid:
                l.add(e)
        return l

    def expand_rids(self, rid):
        l = set([])
        for e in set(rid):
            if '#' in e:
                if e not in self.resources_by_id:
                    continue
                l.add(e)
                continue
            l |= self.expand_rid(e)
        if len(l) > 0:
            self.log.debug("rids added from --rid %s: %s" % (",".join(rid), ",".join(l)))
        return l

    def expand_subsets(self, subsets):
        l = set([])
        if subsets is None:
            return l
        for r in self.resources_by_id.values():
            if r.subset in subsets:
                l.add(r.rid)
        if len(l) > 0:
            self.log.debug("rids added from --subsets %s: %s" % (",".join(subsets), ",".join(l)))
        return l

    def expand_tags(self, tags):
        l = set([])
        if tags is None:
            return l
        for r in self.resources_by_id.values():
            if len(r.tags & tags) > 0:
                l.add(r.rid)
        if len(l) > 0:
            self.log.debug("rids added from --tags %s: %s" % (",".join(tags), ",".join(l)))
        return l

    def action_translate(self, action):
        translation = {
          "syncnodes": "sync_nodes",
          "syncdrp": "sync_drp",
          "syncupdate": "sync_update",
          "syncresync": "sync_resync",
          "syncall": "sync_all",
          "syncfullsync": "sync_full",
          "syncrestore": "sync_restore",
          "syncquiesce": "sync_quiesce",
          "syncsplit": "sync_split",
          "syncestablish": "sync_establish",
          "syncrevert": "sync_revert",
          "syncbreak": "sync_break",
          "syncresume": "sync_resume",
          "syncverify": "sync_verify",
        }
        if action in translation:
            return translation[action]
        return action

    def action(self, action, rid=[], tags=set([]), subsets=set([]), xtags=set([]), waitlock=60):
        rids = self.expand_rids(rid)
        rids |= self.expand_subsets(subsets)
        rids |= self.expand_tags(tags)
        rids = list(rids)
        self.log.debug("rids retained after all expansions: %s" % ";".join(rids))

        if not self.options.slaves and self.options.slave is None and \
           len(set(rid) | subsets | tags) > 0 and len(rids) == 0:
            self.log.error("no resource match the given --rid, --subset and --tags specifiers")
            return 1

        self.action_rid = rids
        if self.node is None:
            self.node = node.Node()
        self.action_start_date = datetime.datetime.now()
        if self.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
            self.log.error("Abort action for non PRD service on PRD node")
            return 1

        action = self.action_translate(action)

        actions_list_allow_on_frozen = [
          'get',
          'set',
          'unset',
          'update',
          'enable',
          'disable',
          'delete',
          'freeze',
          'thaw',
          'status',
          'frozen',
          'push',
          'push_env',
          'push_appinfo',
          'push_service_status',
          'edit_config',
          'scheduler',
          'print_schedule',
          'print_config',
          'print_env_mtime',
          'print_status',
          'print_resource_status',
          'print_disklist',
          'print_devlist',
          'json_env',
          'json_status',
          'json_disklist',
          'json_devlist'
        ]
        actions_list_allow_on_cluster = actions_list_allow_on_frozen + [
          'docker',
          'boot',
          'toc',
          'startstandby',
          'resource_monitor',
          'presync',
          'postsync',
          'sync_drp',
          'sync_nodes',
          'sync_all'
        ]
        if action not in actions_list_allow_on_frozen and \
           'compliance' not in action and \
           'collector' not in action:
            if self.frozen() and not self.force:
                self.log.info("Abort action '%s' for frozen service. Use --force to override." % action)
                return 1
            try:
                if action not in actions_list_allow_on_cluster:
                    self.cluster_mode_safety_net(action)
            except ex.excAbortAction as e:
                self.log.info(str(e))
                return 0
            except ex.excEndAction as e:
                self.log.info(str(e))
                return 0
            except ex.excError as e:
                self.log.error(str(e))
                return 1
            #
            # here we know we will run a resource state-changing action
            # purge the resource status file cache, so that we don't take
            # decision on outdated information
            #
            if not self.options.dry_run and action != "resource_monitor":
                self.log.debug("purge all resource status file caches")
                self.purge_status_last()

        self.setup_environ(action=action)
        self.setup_signal_handlers()
        self.set_skip_resources(keeprid=rids, xtags=xtags)
        actions_list_no_log = [
          'get',
          'set',
          'push',
          'push_env',
          'push_appinfo',
          'push_service_status',
          'scheduler',
          'print_schedule',
          'print_env_mtime',
          'print_status',
          'print_resource_status',
          'print_disklist',
          'print_devlist',
          'print_config',
          'edit_config',
          'json_status',
          'json_disklist',
          'json_devlist',
          'json_env',
          'status',
          'group_status',
          'resource_monitor'
        ]
        if action in actions_list_no_log or \
           action.startswith("compliance") or \
           action.startswith("collector") or \
           action.startswith("docker") or \
           self.options.dry_run:
            err = self.do_action(action, waitlock=waitlock)
        else:
            err = self.do_logged_action(action, waitlock=waitlock)
        return err

    def do_action(self, action, waitlock=60):
        """Trigger action
        """
        err = 0
        try:
            self.svclock(action, timeout=waitlock)
        except Exception as e:
            self.log.error(str(e))
            return 1

        try:
            if action.startswith("compliance_"):
                from compliance import Compliance
                o = Compliance(self.skip_action, self.options, self.node.collector, self.svcname)
                getattr(o, action)()
            elif action.startswith("collector_"):
                from collector import Collector
                o = Collector(self.options, self.node.collector, self.svcname)
                getattr(o, action)()
            elif hasattr(self, action):
                self.running_action = action
                err = getattr(self, action)()
                if err is None:
                    err = 0
                self.running_action = None
            else:
                self.log.error("unsupported action %s" % action)
                err = 1
        except ex.excEndAction as e:
            s = "'%s' action ended by last resource"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.info(s)
            err = 0
        except ex.excAbortAction as e:
            s = "'%s' action aborted by last resource"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.info(s)
            err = 0
        except ex.excError as e:
            s = "'%s' action stopped on execution error"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.error(s)
            err = 1
            self.rollback_handler(action)
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

        if action == "start" and self.cluster and self.ha:
            """ This situation is typical of a hb-initiated service start.
                While the hb starts the service, its resource status is warn from
                opensvc point of view. So after a successful startup, the hb res
                status would stay warn until the next svcmon.
                To avoid this drawback we can force from here the hb status.
            """
            if err == 0:
                for r in self.get_resources(['hb']):
                    if r.disabled:
                        continue
                    r.force_status(rcStatus.UP)

        return err

    def rollback_handler(self, action):
        if 'start' not in action:
            return
	if self.options.disable_rollback:
            self.log.info("skip rollback %s: as instructed by --disable-rollback"%action)
            return
	if self.disable_rollback:
            self.log.info("skip rollback %s: as instructed by DEFAULT.rollback=false"%action)
            return
        rids = [r.rid for r in self.get_resources() if r.can_rollback]
        if len(rids) == 0:
            self.log.info("skip rollback %s: no resource activated"%action)
            return
        self.log.info("trying to rollback %s on %s"%(action, ', '.join(rids)))
        try:
            self.rollback()
        except:
            self.log.error("rollback %s failed"%action)

    def do_logged_action(self, action, waitlock=60):
        from datetime import datetime
        import tempfile
        import logging
        begin = datetime.now()

        """Provision a database entry to store action log later
        """
        if action in ('postsync', 'shutdown'):
            # don't loose the action log on node shutdown
            # no background dblogger for remotely triggered postsync
            self.sync_dblogger = True
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
        self.log.info(" ".join(sys.argv))

        err = self.do_action(action, waitlock=waitlock)

        """ Push result and logs to database
        """
        actionlogfilehandler.close()
        log.removeHandler(actionlogfilehandler)
        end = datetime.now()
        self.dblogger(action, begin, end, actionlogfile)
        return err

    def restart(self):
        self.stop()
        self.start()

    def _migrate(self):
        self.sub_set_action("container.ovm", "_migrate")
        self.sub_set_action("container.hpvm", "_migrate")
        self.sub_set_action("container.esx", "_migrate")

    @_master_action
    def migrate(self):
        if not hasattr(self, "destination_node"):
            raise ex.excError("a destination node must be provided for the switch action")
        if self.destination_node == rcEnv.nodename:
            raise ex.excError("the destination node is source node")
        if self.destination_node not in self.nodes:
            raise ex.excError("the destination node %s is not in the service node list"%self.destination_node)
        self.master_prstop()
        try:
            self.remote_action(node=self.destination_node, action='startfs --master')
            self._migrate()
        except:
            if self.has_res_set(['disk.scsireserv']):
                self.log.error("scsi reservations were dropped. you have to acquire them now using the 'prstart' action either on source node or destination node, depending on your problem analysis.")
            raise
        self.master_stopfs()
        self.remote_action(node=self.destination_node, action='prstart --master')

    def switch(self):
        self.sub_set_action("hb", "switch")
        if not hasattr(self, "destination_node"):
            raise ex.excError("a destination node must be provided for the switch action")
        if self.destination_node == rcEnv.nodename:
            raise ex.excError("the destination node is source node")
        if self.destination_node not in self.nodes:
            raise ex.excError("the destination node %s is not in the service node list"%self.destination_node)
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
            print("failed to write new %s"%self.nodeconf, file=sys.stderr)
            raise ex.excError()

    def load_config(self):
        import ConfigParser
        self.config = ConfigParser.RawConfigParser()
        self.config.read(self.pathenv)

    def unset(self):
        self.load_config()
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if section != 'DEFAULT' and not self.config.has_section(section):
            print("section '%s' not found"%section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section '%s'"%(option, section), file=sys.stderr)
            return 1
        try:
            self.config.remove_option(section, option)
            self.write_config()
        except:
            return 1
        return 0

    def get(self):
        self.load_config()
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if section != 'DEFAULT' and not self.config.has_section(section):
            print("section '%s' not found"%section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section '%s'"%(option, section), file=sys.stderr)
            return 1
        print(self.config.get(section, option))
        return 0

    def set(self):
        self.load_config()
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        if self.options.value is None:
            print("no value. set --value", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
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

    def set_disable(self, rids=[], disable=True):
        if len(rids) == 0:
            rids = ['DEFAULT']
        for rid in rids:
            if rid != 'DEFAULT' and not self.config.has_section(rid):
                self.log.error("service", svcname, "has not resource", rid)
                continue
            self.log.info("set %s.disable = %s" % (rid, str(disable)))
            self.config.set(rid, "disable", disable)
        try:
            f = open(self.pathenv, 'w')
        except:
            self.log.error("failed to open", self.pathenv, "for writing")
            return 1

        #
        # if we set DEFAULT.disable = True,
        # we don't want res#n.disable = False
        #
        if len(rids) == 0 and disable:
            for s in self.config.sections():
                if self.config.has_option(s, "disable") and \
                   self.config.getboolean(s, "disable") == False:
                    self.log.info("remove %s.disable = false" % s)
                    self.config.remove_option(s, "disable")

        self.config.write(f)
        return 0

    def enable(self):
        return self.set_disable(self.action_rid, False)

    def disable(self):
        return self.set_disable(self.action_rid, True)

    def delete(self):
        from svcBuilder import delete
        return delete([self.svcname], self.action_rid)

    def docker(self):
        import subprocess
        containers = self.get_resources('container')
        if not hasattr(self, "docker_argv"):
            print("no docker command arguments supplied", file=sys.stderr)
            return 1
        for r in containers:
            if hasattr(r, "docker_cmd"):
                r.docker_start(verbose=False)
                cmd = r.docker_cmd + self.docker_argv
                p = subprocess.Popen(cmd)
                p.communicate()
                return p.returncode
        print("this service has no docker resource", file=sys.stderr)
        return 1

    def freeze(self):
        for r in self.get_resources("hb"):
            r.freeze()
        self.freezer.freeze()

    def thaw(self):
        for r in self.get_resources("hb"):
            r.thaw()
        self.freezer.thaw()

    def frozen(self):
        return self.freezer.frozen()

if __name__ == "__main__" :
    for c in (Svc,) :
        help(c)
    print("""s1=Svc("Zone")""")
    s1=Svc("Zone")
    print("s1=",s1)
    print("""s2=Svc("basic")""")
    s2=Svc("basic")
    print("s2=",s2)
    print("""s1+=Resource("ip")""")
    s1+=Resource("ip")
    print("s1=",s1)
    print("""s1+=Resource("ip")""")
    s1+=Resource("ip")
    print("""s1+=Resource("fs")""")
    s1+=Resource("fs")
    print("""s1+=Resource("fs")""")
    s1+=Resource("fs")
    print("s1=",s1)

    print("""s1.action("status")""")
    s1.action("status")
