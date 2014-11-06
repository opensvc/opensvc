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
from svcBuilder import conf_get_string_scope, conf_get_boolean_scope
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

def signal_handler(signum, frame):
    raise ex.excSignal

class Options(object):
    def __init__(self):
        self.slaves = False
        self.slave = None
        self.master = False
        self.cron = False
        self.force = False
        self.ignore_affinity = False
        self.debug = False
        self.disable_rollback = False
        self.moduleset = ""
        self.module = ""
        self.ruleset_date = ""
        self.dry_run = False
        self.refresh = False
        os.environ['LANG'] = 'C'

class Svc(Resource):
    """Service class define a Service Resource
    It contain list of ResourceSet where each ResourceSets contain same resource
    type
    """

    def __init__(self, svcname=None, type="hosted", optional=False, disabled=False, tags=set([])):
        """usage : aSvc=Svc(type)"""
        self.encap = False
        self.encapnodes = set([])
        self.has_encap_resources = False
        self.options = Options()
        self.node = None
        self.ha = False
        self.sync_dblogger = False
        self.svcname = svcname
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
                             "disk.loop",
                             "disk.gandi",
                             "disk.scsireserv",
                             "disk.vg",
                             "disk.lv",
                             "disk.zpool",
                             "share.nfs",
                             "fs",
                             "ip",
                             "sync.rsync",
                             "sync.symclone",
                             "sync.symsrdfs",
                             "sync.hp3par",
                             "sync.ibmdssnap",
                             "sync.evasnap",
                             "sync.necismsnap",
                             "sync.dcssnap",
                             "sync.dcsckpt",
                             "sync.dds",
                             "sync.zfs",
                             "sync.btrfs",
                             "sync.docker",
                             "sync.netapp",
                             "sync.nexenta",
                             "app",
                             "hb.openha",
                             "hb.sg",
                             "hb.rhcs",
                             "hb.vcs",
                             "hb.ovm",
                             "hb.linuxha"]
        Resource.__init__(self, type=type, optional=optional,
                          disabled=disabled, tags=tags)
        self.log = rcLogger.initLogger(self.svcname.upper())
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

    def __cmp__(self, other):
        """order by service name
        """
        return cmp(self.svcname, other.svcname)

    def purge_status_last(self):
        for rset in self.resSets:
            rset.purge_status_last()

    def get_subset_parallel(self, rtype):
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
            rtype = "%s:%s" % (r.type, r.subset)
        else:
            rtype = r.type

        if rtype in self.type2resSets:
            self.type2resSets[rtype] += r

        elif hasattr(r, 'resources'):
            # this is a ResourceSet or ResourceSet-derived class
            self.resSets.append(r)
            self.type2resSets[rtype] = r

        elif isinstance(r, Resource):
            parallel = self.get_subset_parallel(rtype)
            if hasattr(r, 'rset_class'):
                R = r.rset_class(type=rtype, resources=[r], parallel=parallel)
            else:
                R = ResourceSet(type=rtype, resources=[r], parallel=parallel)
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
          'push_appinfo',
          'print_status',
          'print_resource_status',
          'status',
          'freeze',
          'frozen',
          'thaw',
          'get',
          'freezestop',
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
        if self.lockfd is not None:
            # already acquired
            return
        lockfile = os.path.join(rcEnv.pathlock, self.svcname)
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            raise ex.excError("timed out waiting for lock")
        except lock.lockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param")
        except lock.lockCreateError:
            raise ex.excError("can not create lock file %s"%lockfile)
        except lock.lockAcquire as e:
            raise ex.excError("another action is currently running (pid=%s)"%e.pid)
        except ex.excSignal:
            raise ex.excError("interrupted by signal")
        except:
            import traceback
            traceback.print_exc()
            raise ex.excError("unexpected locking error")
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
         rsets = []
         for t in l:
             for rs in self.resSets:
                 if ':' in rs.type:
                     # remove subset suffix
                     rstype = rs.type[:rs.type.index(':')]
                 else:
                     rstype = rs.type
                 if '.' in t:
                     # exact match
                     if rstype == t:
                         rsets.append(rs)
                 elif '.' in rstype and not strict:
                     # group match
                     _t = rstype.split('.')
                     if _t[0] == t:
                         rsets.append(rs)
                 else:
                     if rstype == t:
                         rsets.append(rs)
         rsets.sort()
         return rsets

    def has_res_set(self, type, strict=False):
        if len(self.get_res_sets(type, strict=strict)) > 0:
            return True
        else:
            return False

    def all_set_action(self, action=None, tags=set([])):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.resSets, action=action, tags=tags)

    def sub_set_action(self, type=None, action=None, tags=set([]), xtags=set([]), strict=False):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.get_res_sets(type, strict=strict), action=action, tags=tags, xtags=xtags)

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

    def set_action(self, sets=[], action=None, tags=set([]), xtags=set([]), strict=False):
        """ TODO: r.is_optional() not doing what's expected if r is a rset
        """
        list_actions_no_pre_action = [
          "delete",
          "enable",
          "disable",
          "status",
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
                r.action(action, tags=tags, xtags=xtags)
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
            if action in list_actions_no_post_action or r.skip:
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
            }

        containers = self.get_resources('container')
        if len(containers) > 0:
            d['encap'] = {}
            for container in containers:
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
                                       'monitor':monitor,
                                       'disable': disable,
                                       'optional': optional,
                                       'encap': encap}
        ss = self.group_status()
        for g in ss:
            d[g] = str(ss[g])
        print(json.dumps(d))

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
        print(json.dumps(svcenv))

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

        print(self.svcname)
        fmt = "%-20s %4s %-10s %s"
        print(fmt%("overall", '', rcStatus.colorize(self.group_status()['overall']), ''))
        fmt = "|- %-17s %4s %-10s %s"
        print(fmt%("avail", '', rcStatus.colorize(self.group_status()['avail']), ''))

        encap_res_status = {}
        for container in self.get_resources('container'):
            try:
                res = self.encap_json_status(container)['resources']
                encap_res_status[container.rid] = res
            except ex.excNotAvailable as e:
                encap_res_status[container.rid] = {}
            except Exception as e:
                print(e)
                encap_res_status[container.rid] = {}

        l = []
        cr = {}
        for rs in self.get_res_sets(self.status_types, strict=True):
            for r in [_r for _r in sorted(rs.resources) if not _r.rid.startswith('sync') and not _r.rid.startswith('hb')]:
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
                    fmt = "|  '- %-14s %4s %-10s %s"
                    pfx = "|     %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "|  |- %-14s %4s %-10s %s"
                    pfx = "|  |  %-14s %4s %-10s "%('','','')
                    if e[0] in cr:
                        subpfx = "|  |  |  %-11s %4s %-10s "%('','','')
                    else:
                        subpfx = None
                    print_res(e, fmt, pfx, subpfx=subpfx)
                if e[0] in cr:
                    _last = len(cr[e[0]]) - 1
                    if _last >= 0:
                        for _i, _e in enumerate(cr[e[0]]):
                            if _i == _last:
                                fmt = "|  |  '- %-11s %4s %-10s %s"
                                pfx = "|  |     %-11s %4s %-10s "%('','','')
                                print_res(_e, fmt, pfx)
                            else:
                                fmt = "|  |  |- %-11s %4s %-10s %s"
                                pfx = "|  |  |  %-11s %4s %-10s "%('','','')
                                print_res(_e, fmt, pfx)

        fmt = "|- %-17s %4s %-10s %s"
        print(fmt%("sync", '', rcStatus.colorize(str(self.group_status()['sync'])), ''))

        l = []
        for rs in self.get_res_sets(self.status_types, strict=True):
            for r in [_r for _r in sorted(rs.resources) if _r.rid.startswith('sync')]:
                rid, status, label, log, monitor, disable, optional, encap = r.status_quad()
                if rid in encap_res_status:
                    status = rcStatus.Status(rcStatus.status_value(encap_res_status[rid]['status']))
                l.append((rid, status, label, log, monitor, disable, optional, encap))
        last = len(l) - 1
        if last >= 0:
            for i, e in enumerate(l):
                if i == last:
                    fmt = "|  '- %-14s %4s %-10s %s"
                    pfx = "|     %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)
                else:
                    fmt = "|  |- %-14s %4s %-10s %s"
                    pfx = "|  |  %-14s %4s %-10s "%('','','')
                    print_res(e, fmt, pfx)

        fmt = "'- %-17s %4s %-10s %s"
        print(fmt%("hb", '', rcStatus.colorize(str(self.group_status()['hb'])), ''))

        l = []
        for rs in self.get_res_sets(self.status_types):
            for r in [_r for _r in sorted(rs.resources) if _r.rid.startswith('hb')]:
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
                "res_desc",
                "res_status",
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
                               repr(r.label),
                               repr(str(rstatus)),
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
                                   repr(str(rid)),
                                   repr(str(encap_res_status['resources'][rid]['label'])),
                                   repr(str(rstatus)),
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
                rset_status[rs.type] = rs.status()
        return rset_status

    def resource_monitor(self):
        self.purge_status_last()
        if self.group_status_cache is None:
            self.group_status(excluded_groups=set(['sync']))
        has_hb = False
        for r in self.get_resources('hb'):
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
                if self.restart_resource(r):
                    # restart suceeded : don't TOC because of this resource
                    continue
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

    def restart_resource(self, r):
        if r.nb_restart == 0:
            return False
        if not hasattr(r, 'start'):
            self.log.error("resource restart configured on resource %s with no 'start' action support"%r.rid)
            return False
        import time
        for i in range(r.nb_restart):
            try:
                self.log.info("restart resource %s. try number %d/%d"%(r.rid, i+1, r.nb_restart))
                r.start()
            except Exception as e:
                self.log.error("restart resource failed: " + str(e))
            if r._status() == rcStatus.UP:
                self.log.info("monitored resource %s restarted. abording TOC."%r.rid)
                return True
            if i + 1 < r.nb_restart:
                time.sleep(1)
        return False

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
        vmhostname = container.vm_hostname()
        try:
            autostart_node = conf_get_string_scope(self, self.config, 'DEFAULT', 'autostart_node', impersonate=vmhostname).split()
        except:
            autostart_node = []
        if cmd == ["start"] and container.booted and vmhostname in autostart_node:
            self.log.info("skip encap service start in container %s: already started on boot"%vmhostname)
            return '', '', 0
        if not self.has_encap_resources or container.status() == rcStatus.DOWN:
            # no need to run encap cmd (no encap resource)
            return '', '', 0

        if self.options.slave is not None and not \
           (container.name in self.options.slave or \
            container.rid in self.options.slave):
            # no need to run encap cmd (container not specified in --slave)
            return '', '', 0

        if cmd == ['start'] and not self.need_start_encap(container):
            self.log.info("skip start in container %s: the encap service is configured to start on container boot."%container.name)
            return '', '', 0

        options = []
        if self.options.dry_run:
            options.append('--dry-run')
        if self.options.refresh:
            options.append('--refresh')

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

    def encap_json_status(self, container, refresh=False):
        if not refresh and hasattr(self, 'encap_json_status_cache') and container.rid in self.encap_json_status_cache:
            return self.encap_json_status_cache[container.rid]
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

        # feed cache
        if not hasattr(self, 'encap_json_status_cache'):
            self.encap_json_status_cache = {}
        self.encap_json_status_cache[container.rid] = gs

        return gs

    def group_status(self,
                     groups=set(["container", "ip", "disk", "fs", "share", "sync", "app", "hb"]),
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

        for t in [_t for _t in self.status_types if not _t.startswith('sync') and not _t.startswith('hb')]:
            group = t.split('.')[0]
            if group not in groups:
                continue
            for r in self.get_res_sets(t, strict=True):
                s = rcStatus.Status(rset_status[r.type])
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
            for r in self.get_res_sets(t, strict=True):
                s = rset_status[r.type]
                status['hb'] += s
                status["overall"] += s

        for t in [_t for _t in self.status_types if _t.startswith('sync')]:
            if 'sync' not in groups:
                continue
            for r in self.get_res_sets(t, strict=True):
                """ sync are expected to be up
                """
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
        print('\n'.join(self.disklist()))

    def print_devlist(self):
        print('\n'.join(self.devlist()))

    def json_disklist(self):
        import json
        print(json.dumps(list(self.disklist())))

    def json_devlist(self):
        import json
        print(json.dumps(list(self.devlist())))

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
        self.master_shutdownfs()
        self.master_shutdownip()

    def _slave_action(fn):
        def _fn(self):
            if self.encap or not self.has_encap_resources:
                return
            if self.running_action not in ('migrate', 'boot', 'shutdown', 'prstart', 'prstop', 'restart', 'start', 'stop', 'startstandby', 'stopstandby') and \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                raise ex.excAbortAction("specify either --master, --slave(s) or both (%s)"%fn.__name__)
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
               self.running_action not in ('migrate', 'boot', 'shutdown', 'restart', 'start', 'stop', 'startstandby', 'stopstandby') and \
               self.has_encap_resources and \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                raise ex.excAbortAction("specify either --master, --slave(s) or both (%s)"%fn.__name__)
            if self.options.master or \
               (not self.options.master and not self.options.slaves and self.options.slave is None):
                fn(self)
        return _fn

    def start(self):
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
        self.startcontainer()
        self.master_startapp()
        self.slave_start()
        self.master_starthb()

    @_slave_action
    def slave_start(self):
        self.encap_cmd(['start'], verbose=True)

    def rollback(self):
        self.rollbackhb()
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

    def cluster_mode_safety_net(self):
        if not self.has_res_set(['hb.ovm', 'hb.openha', 'hb.linuxha', 'hb.sg', 'hb.rhcs', 'hb.vcs']):
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

    @_master_action
    def rollbackhb(self):
        self.master_hb('rollback')

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
        self.sub_set_action("disk.loop", "startstandby")
        self.sub_set_action("disk.gandi", "startstandby")
        self.sub_set_action("disk.scsireserv", "startstandby", xtags=set(['zone']))
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['prevg']))
        self.sub_set_action("disk.zpool", "startstandby", xtags=set(['zone']))
        self.sub_set_action("disk.vg", "startstandby")
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['postvg']))

    @_master_action
    def master_startdisk(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("sync.dcsckpt", "start")
        self.sub_set_action("sync.nexenta", "start")
        self.sub_set_action("sync.symclone", "start")
        self.sub_set_action("sync.symsrdfs", "start")
        self.sub_set_action("sync.hp3par", "start")
        self.sub_set_action("sync.ibmdssnap", "start")
        self.sub_set_action("disk.loop", "start")
        self.sub_set_action("disk.gandi", "start")
        self.sub_set_action("disk.scsireserv", "start", xtags=set(['zone']))
        self.sub_set_action("disk.drbd", "start", tags=set(['prevg']))
        self.sub_set_action("disk.zpool", "start", xtags=set(['zone']))
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
        self.sub_set_action("disk.zpool", "stop", xtags=set(['zone']))
        self.sub_set_action("disk.drbd", "stop", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "stop", xtags=set(['zone']))
        self.sub_set_action("disk.loop", "stop")
        self.sub_set_action("disk.gandi", "stop")

    @_master_action
    def master_shutdowndisk(self):
        self.sub_set_action("disk.drbd", "shutdown", tags=set(['postvg']))
        self.sub_set_action("disk.vg", "shutdown")
        self.sub_set_action("disk.zpool", "shutdown", xtags=set(['zone']))
        self.sub_set_action("disk.drbd", "shutdown", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "shutdown", xtags=set(['zone']))
        self.sub_set_action("disk.loop", "shutdown")
        self.sub_set_action("disk.gandi", "shutdown")

    def rollbackdisk(self):
        self.sub_set_action("disk.drbd", "rollback", tags=set(['postvg']))
        self.sub_set_action("disk.vg", "rollback")
        self.sub_set_action("disk.zpool", "rollback", xtags=set(['zone']))
        self.sub_set_action("disk.drbd", "rollback", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "rollback", xtags=set(['zone']))
        self.sub_set_action("disk.loop", "rollback")
        self.sub_set_action("disk.gandi", "rollback")

    def abort_start(self):
        for r in self.get_resources():
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
        self.sub_set_action("ip", "startstandby", xtags=set(['zone']))

    @_master_action
    def master_startip(self):
        self.sub_set_action("ip", "start", xtags=set(['zone']))

    def stopip(self):
        self.slave_stopip()
        self.master_stopip()

    @_slave_action
    def slave_stopip(self):
        self.encap_cmd(['stopip'], verbose=True)

    @_master_action
    def master_stopip(self):
        self.sub_set_action("ip", "stop", xtags=set(['zone']))

    @_master_action
    def master_shutdownip(self):
        self.sub_set_action("ip", "shutdown", xtags=set(['zone']))

    def rollbackip(self):
        self.sub_set_action("ip", "rollback", xtags=set(['zone']))

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

    @_master_action
    def startstandbycontainer(self):
        self.sub_set_action("container", "startstandby")
        self.refresh_ip_status()

    @_master_action
    def startcontainer(self):
        self.sub_set_action("container", "start")
        self.refresh_ip_status()

    def refresh_ip_status(self):
        """ Used after start/stop container because the ip resource
            status change after its own start/stop
        """
        for r in self.get_resources("ip"):
            r.status(refresh=True)

    @_master_action
    def shutdowncontainer(self):
        self.sub_set_action("container.vbox", "shutdown")
        self.sub_set_action("container.ldom", "shutdown")
        self.sub_set_action("container.hpvm", "shutdown")
        self.sub_set_action("container.xen", "shutdown")
        self.sub_set_action("container.esx", "shutdown")
        self.sub_set_action("container.ovm", "shutdown")
        self.sub_set_action("container.kvm", "shutdown")
        self.sub_set_action("container.amazon", "shutdown")
        self.sub_set_action("container.openstack", "shutdown")
        self.sub_set_action("container.vcloud", "shutdown")
        self.sub_set_action("container.jail", "shutdown")
        self.sub_set_action("container.lxc", "shutdown")
        self.sub_set_action("container.docker", "shutdown")
        self.sub_set_action("container.vz", "shutdown")
        self.sub_set_action("container.srp", "shutdown")
        self.refresh_ip_status()

    @_master_action
    def stopcontainer(self):
        self.sub_set_action("container", "stop")
        self.refresh_ip_status()

    def rollbackcontainer(self):
        self.sub_set_action("container", "rollback")
        self.refresh_ip_status()

    def provision(self):
        self.sub_set_action("disk", "provision", xtags=set(['zone']))
        self.sub_set_action("fs", "provision", xtags=set(['zone']))
        self.sub_set_action("container", "provision")
        self.sub_set_action("ip", "provision", xtags=set(['zone']))
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
        self.startstandbycontainer()
        self.master_startstandbyapp()
        self.master_starthb()

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
            syncnodes and syncdrp. Typically make use of files
            received in var/
        """
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
        rcmd += ['--waitlock', str(waitlock)] + action.split()
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
        self.sub_set_action("sync.docker", "syncnodes")
        self.sub_set_action("sync.dds", "syncnodes")
        self.sub_set_action("sync.symsrdfs", "syncnodes")
        self.remote_postsync()

    def syncdrp(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncdrp")
        self.sub_set_action("sync.zfs", "syncdrp")
        self.sub_set_action("sync.btrfs", "syncdrp")
        self.sub_set_action("sync.docker", "syncdrp")
        self.sub_set_action("sync.dds", "syncdrp")
        self.sub_set_action("sync.symsrdfs", "syncdrp")
        self.remote_postsync()

    def syncswap(self):
        self.sub_set_action("sync.netapp", "syncswap")
        self.sub_set_action("sync.symsrdfs", "syncswap")
        self.sub_set_action("sync.hp3par", "syncswap")
        self.sub_set_action("sync.nexenta", "syncswap")

    def syncrevert(self):
        self.sub_set_action("sync.hp3par", "syncrevert")

    def syncresume(self):
        self.sub_set_action("sync.netapp", "syncresume")
        self.sub_set_action("sync.symsrdfs", "syncresume")
        self.sub_set_action("sync.hp3par", "syncresume")
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
        self.sub_set_action("sync.ibmdssnap", "syncresync")
        self.sub_set_action("sync.evasnap", "syncresync")
        self.sub_set_action("sync.necismsnap", "syncresync")
        self.sub_set_action("sync.dcssnap", "syncresync")
        self.sub_set_action("sync.dds", "syncresync")

    def syncbreak(self):
        self.sub_set_action("sync.netapp", "syncbreak")
        self.sub_set_action("sync.nexenta", "syncbreak")
        self.sub_set_action("sync.symclone", "syncbreak")
        self.sub_set_action("sync.hp3par", "syncbreak")
        self.sub_set_action("sync.ibmdssnap", "syncbreak")
        self.sub_set_action("sync.dcsckpt", "syncbreak")

    def syncupdate(self):
        self.sub_set_action("sync.netapp", "syncupdate")
        self.sub_set_action("sync.hp3par", "syncupdate")
        self.sub_set_action("sync.nexenta", "syncupdate")
        self.sub_set_action("sync.dcsckpt", "syncupdate")
        self.sub_set_action("sync.dds", "syncupdate")
        self.sub_set_action("sync.zfs", "syncnodes")

    def syncfullsync(self):
        self.sub_set_action("sync.dds", "syncfullsync")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncfullsync")

    def syncsplit(self):
        self.sub_set_action("sync.symsrdfs", "syncsplit")

    def syncestablish(self):
        self.sub_set_action("sync.symsrdfs", "syncestablish")

    def syncverify(self):
        self.sub_set_action("sync.dds", "syncverify")

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
                except ex.excError:
                    return False
                if ret: return True
        return False

    def syncall(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncnodes")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.sub_set_action("sync.btrfs", "syncnodes")
        self.sub_set_action("sync.docker", "syncnodes")
        self.sub_set_action("sync.dds", "syncnodes")
        self.sub_set_action("sync.symsrdfs", "syncnodes")
        self.sub_set_action("sync.rsync", "syncdrp")
        self.sub_set_action("sync.zfs", "syncdrp")
        self.sub_set_action("sync.btrfs", "syncdrp")
        self.sub_set_action("sync.docker", "syncdrp")
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
        print("send %s to collector ... OK"%self.pathenv)
        try:
            import time
            with open(self.push_flag, 'w') as f:
                f.write(str(time.time()))
            ret = "OK"
        except:
            ret = "ERR"
        print("update %s timestamp"%self.push_flag, "...", ret)

    def push_encap_env(self):
        if self.encap or not self.has_encap_resources:
            return

        for r in self.get_resources('container'):
            if r.status() != rcStatus.UP:
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
        print("send %s to %s ..."%(self.pathenv, r.name), "OK" if ret == 0 else "ERR\n%s"%err)
        if ret != 0:
            raise ex.excError()

        cmd = ['install', '--envfile', self.pathenv]
        out, err, ret = self._encap_cmd(cmd, container=r)
        print("install %s slave service ..."%r.name, "OK" if ret == 0 else "ERR\n%s"%err)
        if ret != 0:
            raise ex.excError()

    def tag_match(self, rtags, keeptags):
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def set_skip_resources(self, keeprid=[], keeptags=set([]), xtags=set([])):
        if len(keeprid) > 0:
            ridfilter = True
        else:
            ridfilter = False

        if len(keeptags) > 0 or len(xtags) > 0:
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
            if tagsfilter and self.tag_match(r.tags, keeptags):
                continue
            r.skip = True

    def setup_environ(self):
        """ Those are available to startup scripts and triggers
        """
        os.environ['OPENSVC_SVCNAME'] = self.svcname
        for r in self.get_resources():
            r.setup_environ()

    def expand_rid(self, rid):
        l = []
        for e in self.resources_by_id.keys():
            if e is None:
                continue
            if '#' not in e:
                if e == rid:
                    l.append(e)
                else:
                    continue
            elif e[:e.index('#')] == rid:
                l.append(e)
        return l

    def expand_rids(self, rid):
        l = []
        for e in set(rid):
            if '#' in e:
                l.append(e)
                continue
            l += self.expand_rid(e)
        return l

    def action(self, action, rid=[], tags=set([]), xtags=set([]), waitlock=60):
        rid = self.expand_rids(rid)
        self.action_rid = rid
        if self.node is None:
            self.node = node.Node()
        self.action_start_date = datetime.datetime.now()
        if self.svctype != 'PRD' and rcEnv.host_mode == 'PRD':
            self.log.error("Abort action for non PRD service on PRD node")
            return 1

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
          'push_appinfo',
          'edit_config',
          'print_config',
          'print_env_mtime',
          'print_status',
          'print_resource_status',
          'print_disklist',
          'print_devlist',
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
          'syncdrp',
          'syncnodes',
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
            except ex.excAbortAction as e:
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
            if not self.options.dry_run:
                self.log.debug("purge all resource status file caches")
                self.purge_status_last()

        self.setup_environ()
        self.setup_signal_handlers()
        self.set_skip_resources(keeprid=rid, keeptags=tags, xtags=xtags)
        actions_list_no_log = [
          'get',
          'set',
          'push',
          'push_appinfo',
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
        elif action in ["syncall", "syncdrp", "syncnodes", "syncupdate"]:
            if action == "syncall" or "syncupdate": kwargs = {}
            elif action == "syncnodes": kwargs = {'target': 'nodes'}
            elif action == "syncdrp": kwargs = {'target': 'drpnodes'}
            if not self.can_sync(**kwargs):
                self.log.debug("nothing to sync for the service for now")
                return 0
            try:
                # timeout=1, delay=1 => immediate response
                self.svclock(action, timeout=1, delay=1)
            except:
                if not self.cron:
                    self.log.info("%s action is already running"%action)
                return 0
            err = self.do_logged_action(action, waitlock=waitlock)
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
                self.running_action = action
                err = getattr(self, action)()
                if err is None:
                    err = 0
                self.running_action = None
            else:
                self.log.error("unsupported action")
                err = 1
        except ex.excAbortAction as e:
            s = "'%s' action stopped on execution error"%action
            if len(str(e)) > 0:
                s += ": %s"%str(e)
            self.log.error(s)
            err = 1
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
        return err

    def rollback_handler(self, action):
	if self.options.disable_rollback:
            self.log.info("skip rollback %s: as instructed by --disable-rollback"%action)
            return
        if 'start' not in action:
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
            self.log.error("a destination node must be provided for the switch action")
            raise ex.excError
        if self.destination_node not in self.nodes:
            self.log.error("destination node %s is not in service node list"%self.destination_node)
            raise ex.excError
        self.master_prstop()
        try:
            self.remote_action(node=self.destination_node, action='startfs --master')
            self._migrate()
        except:
            if self.has_res_set(['disk.scsireserv']):
                self.log.error("scsi reservations where dropped. you have to acquire them now using the 'prstart' action either on source node or destination node, depending on your problem analysis.")
            raise
        self.master_stopfs()
        self.remote_action(node=self.destination_node, action='prstart --master')

    def switch(self):
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
        self.freezer.freeze()

    def thaw(self):
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
