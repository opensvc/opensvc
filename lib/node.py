#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>
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
from svc import Svc
from freezer import Freezer
import svcBuilder
import xmlrpcClient
import os
import datetime
import time
import sys
import json
from rcGlobalEnv import rcEnv
import rcCommandWorker
import socket
import rcLogger
import rcUtilities
import rcExceptions as ex
from subprocess import *

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

class Options(object):
    def __init__(self):
        self.cron = False
        self.syncrpc = False
        self.force = False
        self.debug = False
        self.stats_dir = None
        self.begin = None
        self.end = None
        self.moduleset = ""
        self.module = ""
        self.ruleset_date = ""
        os.environ['LANG'] = 'C'

class Node(Svc, Freezer):
    """ Defines a cluster node.  It contain list of Svc.
        Implements node-level actions and checks.
    """
    def __str__(self):
        s = self.nodename
        return s

    def __init__(self):
        self.auth_config = None
        self.delay_done = False
        self.nodename = socket.gethostname().lower()
        self.authconf = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'etc', 'auth.conf'))
        self.nodeconf = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'etc', 'node.conf'))
        self.dotnodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', '.node.conf')
        self.setup_sync_flag = os.path.join(rcEnv.pathvar, 'last_setup_sync')
        self.config_defaults = {
          'clusters': '',
          'host_mode': 'TST',
          'push_interval': 121,
          'push_days': '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]',
          'push_period': '["04:00", "06:00"]',
          'sync_interval': 121,
          'sync_days': '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]',
          'sync_period': '["04:00", "06:00"]',
          'comp_check_interval': 241,
          'comp_check_days': '["sunday"]',
          'comp_check_period': '["02:00", "06:00"]',
        }
        self.load_config()
        try:
            self.clusters = list(set(self.config.get('node', 'clusters').split()))
        except:
            self.clusters = []
        self.options = Options()
        self.svcs = None
        Freezer.__init__(self, '')
        self.action_desc = {
          'Node actions': {
            'shutdown': 'shutdown the node to powered off state',
            'reboot': 'reboot the node',
            'provision': 'provision the resources described in --resource arguments',
            'updatepkg': 'upgrade the opensvc agent version. the packages must be available behind the node.repo/packages url.',
            'updatecomp': 'upgrade the opensvc compliance modules. the modules must be available as a tarball behind the node.repo/compliance url.',
            'scanscsi': 'scan the scsi hosts in search of new disks',
            'dequeue_actions': "dequeue and execute actions from the collector's action queue for this node and its services.",
          },
          'Service actions': {
            'discover': 'discover vservices accessible from this host, cloud nodes for example',
            'syncservices':   'send var files, config files and configured replications to other nodes for each node service',
            'updateservices': 'refresh var files associated with services',
          },
          'Node configuration edition': {
            'register': 'obtain a registration number from the collector, used to authenticate the node',
            'get': 'get the value of the node configuration parameter pointed by --param',
            'set': 'set a node configuration parameter (pointed by --param) value (pointed by --value)',
            'unset': 'unset a node configuration parameter (pointed by --param)',
          },
          'Push data to the collector': {
            'pushasset':      'push asset information to collector',
            'pushservices':   'push services configuration to collector',
            'pushstats':      'push performance metrics to collector. By default pushed stats interval begins yesterday at the beginning of the allowed interval and ends now. This interval can be changed using --begin/--end parameters. The location where stats files are looked up can be changed using --stats-dir.',
            'pushdisks':      'push disks usage information to collector',
            'pushpkg':        'push package/version list to collector',
            'pushpatch':      'push patch/version list to collector',
            'pushsym':        'push symmetrix configuration to collector',
            'pusheva':        'push HP EVA configuration to collector',
            'pushnecism':     'push NEC ISM configuration to collector',
            'pushhds':        'push HDS configuration to collector',
            'pushdcs':        'push Datacore configuration to collector',
            'pushibmsvc':     'push IBM SVC configuration to collector',
            'pushvioserver':  'push IBM VIO server configuration to collector',
            'pushbrocade':    'push Brocade switch configuration to collector',
            'pushnsr':        'push EMC Networker index to collector',
            'push_appinfo':   'push services application launchers appinfo key/value pairs to collector',
            'checks':         'run node sanity checks, push results to collector',
          },
          'Misc': {
            'prkey':          'show persistent reservation key of this node',
          },
          'Compliance': {
            'compliance_check': 'run compliance checks. --ruleset <md5> instruct the collector to provide an historical ruleset.',
            'compliance_fix':   'run compliance fixes. --ruleset <md5> instruct the collector to provide an historical ruleset.',
            'compliance_fixable': 'verify compliance fixes prerequisites. --ruleset <md5> instruct the collector to provide an historical ruleset.',
            'compliance_list_module': 'list compliance modules available on this node',
            'compliance_show_moduleset': 'show compliance rules applying to this node',
            'compliance_list_moduleset': 'list available compliance modulesets. --moduleset f% limit the scope to modulesets matching the f% pattern.',
            'compliance_attach_moduleset': 'attach moduleset specified by --moduleset for this node',
            'compliance_detach_moduleset': 'detach moduleset specified by --moduleset for this node',
            'compliance_list_ruleset': 'list available compliance rulesets. --ruleset f% limit the scope to rulesets matching the f% pattern.',
            'compliance_show_ruleset': 'show compliance rules applying to this node',
            'compliance_show_status': 'show compliance modules status',
            'compliance_attach_ruleset': 'attach ruleset specified by --ruleset for this node',
            'compliance_detach_ruleset': 'detach ruleset specified by --ruleset for this node',
          },
          'Collector management': {
            'collector_events': 'display node events during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
            'collector_alerts': 'display node alerts',
            'collector_checks': 'display node checks',
            'collector_disks': 'display node disks',
            'collector_status': 'display node services status according to the collector',
            'collector_list_actions': 'list actions on the node, whatever the service, during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
            'collector_ack_action': 'acknowledge an action error on the node. an acknowlegment can be completed by --author (defaults to root@nodename) and --comment',
            'collector_show_actions': 'show actions detailled log. a single action is specified by --id. a range is specified by --begin/--end dates. --end defaults to now. --begin defaults to 7 days ago',
            'collector_list_nodes': 'show the list of nodes matching the filterset pointed by --filterset',
            'collector_list_services': 'show the list of services matching the filterset pointed by --filterset',
            'collector_list_filtersets': 'show the list of filtersets available on the collector. if specified, --filterset <pattern> limits the resulset to filtersets matching <pattern>',
            'collector_json_list_unavailability_ack': 'same as "collector list unavailability ack", output in JSON',
            'collector_json_list_actions': 'same as "collector list actions", output in JSON',
            'collector_json_show_actions': 'same as "collector show actions", output in JSON',
            'collector_json_status': 'same as "collector status", output in JSON',
            'collector_json_checks': 'same as "collector checks", output in JSON',
            'collector_json_disks': 'same as "collector disks", output in JSON',
            'collector_json_alerts': 'same as "collector alerts", output in JSON',
            'collector_json_events': 'same as "collector events", output in JSON',
            'collector_json_list_nodes': 'same as "collector list nodes", output in JSON',
            'collector_json_list_services': 'same as "collector list services", output in JSON',
            'collector_json_list_filtersets': 'same as "collector list filtersets", output in JSON',
          },
        }
        self.collector = xmlrpcClient.Collector()
        self.cmdworker = rcCommandWorker.CommandWorker()
        try:
            rcos = __import__('rcOs'+rcEnv.sysname)
        except ImportError:
            rcos = __import__('rcOs')
        self.os = rcos.Os()
        rcEnv.logfile = os.path.join(rcEnv.pathlog, "node.log")
        self.log = rcLogger.initLogger(rcEnv.nodename)

    def call(self, cmd=['/bin/false'], cache=False, info=False,
             errlog=True, err_to_warn=False, err_to_info=False,
             outlog=False):
        """Use subprocess module functions to do a call
        """
        return rcUtilities.call(cmd, log=self.log,
                                cache=cache,
                                info=info, errlog=errlog,
                                err_to_warn=err_to_warn,
                                err_to_info=err_to_info,
                                outlog=outlog)

    def vcall(self, cmd, err_to_warn=False, err_to_info=False):
        """Use subprocess module functions to do a call and
        log the command line using the resource logger
        """
        return rcUtilities.vcall(cmd, log=self.log,
                                 err_to_warn=err_to_warn,
                                 err_to_info=err_to_info)

    def supported_actions(self):
        a = []
        for s in self.action_desc:
            a += self.action_desc[s].keys()
        return a

    def build_services(self, *args, **kwargs):
        if self.svcs is not None and \
           ('svcnames' not in kwargs or \
           (type(kwargs['svcnames']) == list and len(kwargs['svcnames'])==0)):
            return

        if 'svcnames' in kwargs and \
           type(kwargs['svcnames']) == list and \
           len(kwargs['svcnames'])>0 and \
           self.svcs is not None:
            svcnames_request = set(kwargs['svcnames'])
            svcnames_actual = set([s.svcname for s in self.svcs])
            svcnames_request = list(svcnames_request-svcnames_actual)
            if len(svcnames_request) == 0:
                return

        self.svcs = []
        autopush = True
        if 'autopush' in kwargs:
            if not kwargs['autopush']:
                autopush = False
            del kwargs['autopush']
        svcs = svcBuilder.build_services(*args, **kwargs)
        for svc in svcs:
            self += svc
        if autopush:
            for svc in self.svcs:
                if svc.collector_outdated():
                    svc.action('push')

    def close(self):
        self.collector.stop_worker()
        self.cmdworker.stop_worker()

    def _setup_sync_conf(self):
        h = {}
        self.build_services()
        for svc in self.svcs:
            for rs in [_rs for _rs in svc.resSets if _rs.type.startswith('sync')]:
                for r in rs.resources:
                    s = '#'.join((svc.svcname, r.rid))
                    if not self.config.has_section(s):
                        self.config.add_section(s)
                    self.config.set(s, 'svcname', svc.svcname)
                    self.config.set(s, 'rid', r.rid)
                    self.config.set(s, 'interval', r.sync_interval)
                    self.config.set(s, 'days', json.dumps(r.sync_days))
                    self.config.set(s, 'period', json.dumps(r.sync_period))
        self.write_dotconfig()
        with open(self.setup_sync_flag, 'w') as f:
            f.write(str(time.time()))

    def write_dotconfig(self):
        for o in self.config_defaults:
            if self.config.has_option('DEFAULT', o):
                self.config.remove_option('DEFAULT', o)
        for s in self.config.sections():
            if '#sync#' not in s:
                self.config.remove_section(s)
        try:
            fp = open(self.dotnodeconf, 'w')
            self.config.write(fp)
            fp.close()
        except:
            print("failed to write new %s"%self.dotnodeconf, file=sys.stderr)
            raise Exception()
        self.load_config()

    def write_config(self):
        for o in self.config_defaults:
            if self.config.has_option('DEFAULT', o):
                self.config.remove_option('DEFAULT', o)
        for s in self.config.sections():
            if '#sync#' in s:
                self.config.remove_section(s)
        try:
            fp = open(self.nodeconf, 'w')
            self.config.write(fp)
            fp.close()
        except:
            print("failed to write new %s"%self.nodeconf, file=sys.stderr)
            raise Exception()
        self.load_config()

    def load_config(self):
        self.config = ConfigParser.RawConfigParser(self.config_defaults)
        self.config.read(self.nodeconf)
        self.config.read(self.dotnodeconf)

    def load_auth_config(self):
        if self.auth_config is not None:
            return
        self.auth_config = ConfigParser.ConfigParser()
        self.auth_config.read(self.authconf)

    def setup_sync_outdated(self):
        """ return True if one env file has changed in the last 10'
            else return False
        """
        import datetime
        import glob
        envs = glob.glob(os.path.join(rcEnv.pathetc, '*.env'))
        if not os.path.exists(self.setup_sync_flag):
            return True
        for pathenv in envs:
            try:
                mtime = os.stat(pathenv).st_mtime
                f = open(self.setup_sync_flag)
                last = float(f.read())
                f.close()
            except:
                return True
            if mtime > last:
                return True
        return False

    def setup_sync_conf(self):
        if self.setup_sync_outdated():
            self._setup_sync_conf()
            return
        elif not self.config.has_section('sync'):
            self._setup_sync_conf()
            return
        elif not self.config.has_option('sync', 'interval') or \
           not self.config.has_option('sync', 'days') or \
           not self.config.has_option('sync', 'period'):
            self._setup_sync_conf()

    def format_desc(self):
        from textwrap import TextWrapper
        from compliance import Compliance
        from collector import Collector
        wrapper = TextWrapper(subsequent_indent="%19s"%"", width=78)
        desc = ""
        for s in sorted(self.action_desc):
            l = len(s)
            desc += s+'\n'
            for i in range(0, l):
                desc += '-'
            desc += "\n\n"
            for a in sorted(self.action_desc[s]):
                if a.startswith("compliance_"):
                    o = Compliance(self.skip_action, self.options, self.collector)
                    if not hasattr(o, a):
                        continue
                elif a.startswith("collector_"):
                    o = Collector(self.options, self.collector)
                    if not hasattr(o, a):
                        continue
                elif not hasattr(self, a):
                    continue
                fancya = a.replace('_', ' ')
                if len(a) < 16:
                    text = "  %-16s %s\n"%(fancya, self.action_desc[s][a])
                    desc += wrapper.fill(text)
                else:
                    text = "  %-16s"%(fancya)
                    desc += wrapper.fill(text)
                    desc += '\n'
                    text = "%19s%s"%(" ", self.action_desc[s][a])
                    desc += wrapper.fill(text)
                desc += '\n\n'
        return desc[0:-2]

    def __iadd__(self, s):
        if not isinstance(s, Svc):
            return self
        if self.svcs is None:
            self.svcs = []
        s.node = self
        if not hasattr(s, "clustername") and len(self.clusters) == 1:
            s.clustername = self.clusters[0]
        self.svcs.append(s)
        return self

    def action(self, a):
        if a.startswith("compliance_"):
            from compliance import Compliance
            o = Compliance(self.skip_action, self.options, self.collector)
            if self.options.cron and a == "compliance_check" and \
               self.config.has_option('compliance', 'auto_update') and \
               self.config.getboolean('compliance', 'auto_update'):
                o.updatecomp = True
                o.node = self
            return getattr(o, a)()
        elif a.startswith("collector_"):
            from collector import Collector
            o = Collector(self.options, self.collector)
            return getattr(o, a)()
        else:
            return getattr(self, a)()

    def need_action_interval(self, timestamp_f, delay=10):
        """ Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval
        """
        if delay == 0:
            return False
        if not os.path.exists(timestamp_f):
            return True
        try:
            with open(timestamp_f, 'r') as f:
                d = f.read()
                last = datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n")
                limit = last + datetime.timedelta(minutes=delay)
                if datetime.datetime.now() < limit:
                    return False
                else:
                    return True
                f.close()
        except:
            return True

        # never reach here
        return True

    def timestamp(self, timestamp_f, interval):
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d, 0o755)
        with open(timestamp_f, 'w') as f:
            f.write(str(datetime.datetime.now())+'\n')
            f.close()
        return True

    def skip_action_interval(self, timestamp_f, interval):
        return not self.need_action_interval(timestamp_f, interval)

    def skip_probabilistic(self, period, interval):
        if len(period) == 0:
            return False

        try:
            start, end, now = self.get_period_minutes(period)
        except:
            return False

        if start > end:
            end += 1440
        if now < start:
            now += 1440

        length = end - start

        if interval <= length:
            # don't skip if interval <= period length, because the user
            # expects the action to run multiple times in the period
            return False

        elapsed = now - start
        elapsed_pct = int(100.0 * elapsed / length)

        """
            proba
              ^
        100%  |
         75%  |X
         50%  |XX
         25%  |XXX
          0%  ----|----|-> elapsed
             0%  50%  100%

        The idea is to skip 75% of actions in period's first run,
        skip none after half the interval is consumed, and decrease
        skip probabilty linearly in-between.

        This algo is meant to level collector's load which peaks
        when all daily cron trigger at the same minute.
        """
        p = 75.0 * (100.0 - min(elapsed_pct * 2, 100)) / 100
        import random
        r = random.random()*100.0

        """
        print("start:", start)
        print("end:", end)
        print("now:", now)
        print("length:", length)
        print("elapsed:", elapsed)
        print("elapsed_pct:", elapsed_pct)
        print("p:", p)
        print("r:", r)
        """

        if r >= p:
            print("win probabilistic challenge: %d, over %d"%(r, p))
            return False

        return True

    def get_period_minutes(self, period):
        start_s, end_s = period
        try:
            start_t = time.strptime(start_s, "%H:%M")
            end_t = time.strptime(end_s, "%H:%M")
            start = start_t.tm_hour * 60 + start_t.tm_min
            end = end_t.tm_hour * 60 + end_t.tm_min
        except:
            print("malformed time string: %s"%str(period), file=sys.stderr)
            raise Exception("malformed time string: %s"%str(period))
        now = datetime.datetime.now()
        now_m = now.hour * 60 + now.minute
        return start, end, now_m

    def in_period(self, period):
        if isinstance(period[0], list):
            r = False
            for p in period:
                 if self.in_period(p):
                     return True
            return False
        elif not isinstance(period[0], unicode) or len(period) != 2 or \
             not isinstance(period[1], unicode):
            print("malformed period: %s"%str(period), file=sys.stderr)
            return False

        if len(period) == 0:
            return True
        try:
            start, end, now = self.get_period_minutes(period)
        except:
            return False

        if start <= end:
            if now >= start and now <= end:
                return True
        elif start > end:
            """
                  XXXXXXXXXXXXXXXXX
                  23h     0h      1h
            """
            if (now >= start and now <= 1440) or \
               (now >= 0 and now <= end):
                return True
        return False

    def in_days(self, days):
        now = datetime.datetime.now()
        today = now.strftime('%A').lower()
        if today in map(lambda x: x.lower(), days):
            return True
        return False

    def skip_action_probabilistic(self, section, option, interval):
        if option is None:
            return False

        if self.config.has_section(section) and \
           self.config.has_option(section, 'period'):
            period_s = self.config.get(section, 'period')
        elif self.config.has_option('DEFAULT', option):
            period_s = self.config.get('DEFAULT', option)
        else:
            return False

        try:
            period = json.loads(period_s)
        except:
            print("malformed parameter value: %s.period"%section, file=sys.stderr)
            return True

        if isinstance(period[0], list):
            matching_period = None
            for p in period:
                if self.in_period(p):
                    matching_period = p
                    break
            if matching_period is None:
                return True
            period = matching_period

        return self.skip_probabilistic(period, interval)

    def skip_action_period(self, section, option):
        if option is None:
            return False

        if self.config.has_section(section) and \
           self.config.has_option(section, 'period'):
            period_s = self.config.get(section, 'period')
        elif self.config.has_option('DEFAULT', option):
            period_s = self.config.get('DEFAULT', option)
        else:
            return False

        try:
            period = json.loads(period_s)
        except:
            print("malformed parameter value: %s.period"%section, file=sys.stderr)
            return True

        if self.in_period(period):
            return False

        return True

    def skip_action_days(self, section, option):
        if option is None:
            return False

        if self.config.has_section(section) and \
           self.config.has_option(section, 'days'):
            days_s = self.config.get(section, 'days')
        elif self.config.has_option('DEFAULT', option):
            days_s = self.config.get('DEFAULT', option)
        else:
            return False

        try:
            days = json.loads(days_s)
        except:
            print("malformed parameter value: %s.days"%section, file=sys.stderr)
            return True

        if self.in_days(days):
            return False

        return True

    def get_interval(self, section, option, cmdline_parm):
        # get interval from config file
        if self.config.has_section(section) and \
           self.config.has_option(section, 'interval'):
            interval = self.config.getint(section, 'interval')
        else:
            interval = self.config.getint('DEFAULT', option)

        # override with command line
        if cmdline_parm is not None:
            v = getattr(self.options, cmdline_parm)
            if v is not None:
                interval = v

        return interval

    def skip_action(self, section, option, fname,
                    cmdline_parm=None,
                    period_option=None,
                    days_option=None):

        def err(msg):
            print('%s: skip:'%section, msg, '(--force to bypass)')

        if not self.options.cron:
            return False

        if self.options.force:
            return False

        # check if we are in allowed period
        if self.skip_action_period(section, period_option):
            err('out of allowed periods')
            return True

        # check if we are in allowed days of week
        if self.skip_action_days(section, days_option):
            err('out of allowed days')
            return True

        interval = self.get_interval(section, option, cmdline_parm)
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', fname)

        # check if we are in allowed days of week
        if self.skip_action_interval(timestamp_f, interval):
            err('last run < interval')
            return True

        # probabilistic skip
        if '#sync#' not in section and \
           self.skip_action_probabilistic(section, period_option, interval):
            err('checks passed but skip to level collector load')
            return True

        # don't update the timestamp in force mode
        # to not perturb the schedule
        if not self.options.force:
            self.timestamp(timestamp_f, interval)

        # ok. we have some action to perform.
        # now wait for a random delay <5min to not overload the
        # collector listeners at 10 minutes intervals.
        # only delay for the first action of this Node() object
        # lifespan
        if not self.delay_done:
            import random
            import time
            delay = int(random.random()*300)
            print("delay action for %d secs to level database load"%delay)
            time.sleep(delay)
            self.delay_done = True

        return False

    def pushstats(self):
        if self.skip_action('stats', 'push_interval', 'last_stats_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        if self.config.has_section('stats'):
            period = self.config.get('stats', 'push_period')
        else:
            period = self.config.get('DEFAULT', 'push_period')
        try:
            period = json.loads(period)
        except:
            return

        try:
            start, end, now = self.get_period_minutes(period)
        except:
            return

        disable = self.config.get("stats", "disable")
        if isinstance(disable, str):
            try:
                disable = json.loads(disable)
            except:
                if ',' in disable:
                    disable = disable.replace(' ','').split(',')
                else:
                    disable = disable.split(' ')
        else:
            disable = []

        # set interval to grab from the begining of the last allowed period
        # to now
        interval = 1440 + now - start

        self.collector.call('push_stats', force=self.options.force,
                                stats_dir=self.options.stats_dir,
                                stats_start=self.options.begin,
                                stats_end=self.options.end,
                                interval=interval,
                                disable=disable)

    def pushpkg(self):
        if self.skip_action('packages', 'push_interval', 'last_pkg_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_pkg')

    def pushpatch(self):
        if self.skip_action('patches', 'push_interval', 'last_patch_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_patch')

    def pushasset(self):
        if self.skip_action('asset', 'push_interval', 'last_asset_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_asset', self)

    def pushnsr(self):
        if self.skip_action('nsr', 'push_interval', 'last_nsr_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_nsr')

    def pushdcs(self):
        if self.skip_action('dcs', 'push_interval', 'last_dcs_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_dcs', self.options.objects)

    def pushhds(self):
        if self.skip_action('hds', 'push_interval', 'last_hds_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_hds', self.options.objects)

    def pushnecism(self):
        if self.skip_action('necism', 'push_interval', 'last_necism_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_necism', self.options.objects)

    def pusheva(self):
        if self.skip_action('eva', 'push_interval', 'last_eva_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_eva', self.options.objects)

    def pushibmsvc(self):
        if self.skip_action('ibmsvc', 'push_interval', 'last_ibmsvc_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_ibmsvc', self.options.objects)

    def pushvioserver(self):
        if self.skip_action('ibmsvc', 'push_interval', 'last_vioserver_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_vioserver', self.options.objects)

    def pushsym(self):
        if self.skip_action('sym', 'push_interval', 'last_sym_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_sym', self.options.objects)

    def pushbrocade(self):
        if self.skip_action('brocade', 'push_interval', 'last_brocade_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_brocade', self.options.objects)

    def pushdisks(self):
        if self.skip_action('sym', 'push_interval', 'last_disks_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        if self.svcs is None:
            self.build_services()

        self.collector.call('push_disks', self)

    def need_sync(self):
        l = []
        for s in self.config.sections():
            if '#sync#' not in s:
                continue
            ts = '_'.join(('last_sync',
                           self.config.get(s, 'svcname'),
                           self.config.get(s, 'rid')))
            if self.skip_action(s, 'sync_interval', ts,
                                period_option='sync_period',
                                days_option='sync_days'):
                    continue
            l.append(self.config.get(s, 'svcname'))
        return l

    def shutdown(self):
        print("TODO")

    def reboot(self):
        print("TODO")

    def syncservices(self):
        self.setup_sync_conf()
        svcnames = self.need_sync()
        if len(svcnames) == 0:
            return

        from multiprocessing import Process
        p = {}

        if self.svcs is None:
            self.build_services(svcnames=svcnames)

        for svc in self.svcs:
            p[svc.svcname] = rcCommandWorker.CommandWorker(name=svc.svcname)
            cmd = [os.path.join(rcEnv.pathetc, svc.svcname), 'syncall']
            if self.options.force:
                cmd.append('--force')
            if self.options.cron:
                cmd.append('--cron')
            p[svc.svcname].enqueue(cmd)

        for svcname in p:
            p[svcname].stop_worker()

    def updateservices(self):
        if self.svcs is None:
            self.build_services()
        for svc in self.svcs:
            svc.cron = self.options.cron
            svc.action('presync')

    def pushservices(self):
        if self.skip_action('svcconf', 'push_interval', 'last_svcconf_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        if self.svcs is None:
            self.build_services()
        for svc in self.svcs:
            svc.cron = self.options.cron
            svc.action('push')

    def push_appinfo(self):
        if self.skip_action('appinfo', 'push_interval', 'last_appinfo_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        if self.svcs is None:
            self.build_services()
        for svc in self.svcs:
            svc.cron = self.options.cron
            svc.action('push_appinfo')

    def prkey(self):
        from rcGlobalEnv import rcEnv
        m = __import__('hostid'+rcEnv.sysname)
        print(m.hostid())

    def checks(self):
        if self.skip_action('checks', 'push_interval', 'last_checks_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        import checks
        if self.svcs is None:
            self.build_services()
        c = checks.checks(self.svcs)
        c.node = self
        c.do_checks()

    def unset(self):
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if not self.config.has_section(section):
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
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if not self.config.has_section(section):
            print("section '%s' not found"%section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section '%s'"%(option, section), file=sys.stderr)
            return 1
        print(self.config.get(section, option))
        return 0

    def set(self):
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
        if not self.config.has_section(section):
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

    def register(self):
        u = self.collector.call('register_node')
        if u is None:
            print("failed to obtain a registration number", file=sys.stderr)
            return 1
        elif isinstance(u, list):
            print(u[0], file=sys.stderr)
            return 1
        try:
            if not self.config.has_section('node'):
                self.config.add_section('node')
            self.config.set('node', 'uuid', u)
            self.write_config()
        except:
            print("failed to write registration number: %s"%u, file=sys.stderr)
            return 1
        print("registered")
        return 0

    def service_action_worker(self, s, **kwargs):
        r = s.action(**kwargs)
        self.close()
        sys.exit(r)

    def devlist(self):
        try:
            m = __import__("rcDevTree"+rcEnv.sysname)
        except ImportError:
            return
        tree = m.DevTree()
        tree.load()
        l = []
        for dev in tree.get_top_devs():
            if len(dev.devpath) > 0:
                l.append(dev.devpath[0])
        return l

    def updatecomp(self):
        if self.config.has_option('node', 'repocomp'):
            pkg_name = self.config.get('node', 'repocomp').strip('/') + "/current"
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + "/compliance/current"
        else:
            if self.options.cron:
                return 0
            print("node.repo or node.repocomp must be set in node.conf", file=sys.stderr)
            return 1
        import tempfile
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        print("get %s (%s)"%(pkg_name, tmpf))
        import urllib
        try:
            fname, headers = urllib.urlretrieve(pkg_name, tmpf)
        except IOError:
            import traceback
            e = sys.exc_info()
            if self.options.cron:
                return 0
            print("download failed", ":", e[1], file=sys.stderr)
            return 1
        if 'invalid file' in headers.values():
            if self.options.cron:
                return 0
            print("invalid file", file=sys.stderr)
            return 1
        with open(fname, 'r') as f:
            content = f.read()
        if content.startswith('<') and '404 Not Found' in content:
            try:
                os.unlink(fname)
            except:
                pass
            if self.options.cron:
                return 0
            print("not found", file=sys.stderr)
            return 1
        tmpp = os.path.join(rcEnv.pathtmp, 'compliance')
        backp = os.path.join(rcEnv.pathtmp, 'compliance.bck')
        compp = os.path.join(rcEnv.pathvar, 'compliance')
        import shutil
        try:
            shutil.rmtree(backp)
        except:
            pass
        print("extract compliance in", rcEnv.pathtmp)
        import tarfile
        tar = tarfile.open(f.name)
        os.chdir(rcEnv.pathtmp)
        try:
            tar.extractall()
            tar.close()
        except:
            print("failed to unpack", file=sys.stderr)
            return 1
        f.close()
        print("install new compliance")
        shutil.move(compp, backp)
        shutil.move(tmpp, compp)

    def updatepkg(self):
        if not os.path.exists(os.path.join(rcEnv.pathlib, 'rcUpdatePkg'+rcEnv.sysname+'.py')):
            print("updatepkg not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        m = __import__('rcUpdatePkg'+rcEnv.sysname)
        if self.config.has_option('node', 'repopkg'):
            pkg_name = self.config.get('node', 'repopkg').strip('/') + "/" + m.repo_subdir + '/current'
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + "/packages/" + m.repo_subdir + '/current'
        else:
            print("node.repo or node.repopkg must be set in node.conf", file=sys.stderr)
            return 1
        import tempfile
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        print("get %s (%s)"%(pkg_name, tmpf))
        import urllib
        try:
            fname, headers = urllib.urlretrieve(pkg_name, tmpf)
        except IOError:
            import traceback
            e = sys.exc_info()
            print("download failed", ":", e[1], file=sys.stderr)
            return 1
        if 'invalid file' in headers.values():
            print("invalid file", file=sys.stderr)
            return 1
        with open(fname, 'r') as f:
            content = f.read()
        if content.startswith('<') and '404 Not Found' in content:
            print("not found", file=sys.stderr)
            try:
                os.unlink(fname)
            except:
                pass
            return 1
        print("updating opensvc")
        m.update(tmpf)
        print("clean up")
        f.close()
        return 0

    def provision(self):
        self.provision_resource = []
        for rs in self.options.resource:
            try:
                d = json.loads(rs)
            except:
                print("JSON read error: %s", rs, file=sys.stderr)
                return 1
            if 'rtype' not in d:
                print("'rtype' key must be set in resource dictionary: %s", rs, file=sys.stderr)
                return 1

            rtype = d['rtype']
            if len(rtype) < 2:
                print("invalid 'rtype' value: %s", rs, file=sys.stderr)
                return 1
            rtype = rtype[0].upper() + rtype[1:].lower()

            if 'type' in d:
                rtype +=  d['type'][0].upper() + d['type'][1:].lower()
            modname = 'prov' + rtype
            try:
                m = __import__(modname)
            except ImportError:
                print("provisioning is not available for resource type:", d['rtype'], "(%s)"%modname, file=sys.stderr)
                return 1
            if not hasattr(m, "d_provisioner"):
                print("provisioning with nodemgr is not available for this resource type:", d['rtype'], file=sys.stderr)
                return 1

            self.provision_resource.append((m, d))

        for o, d in self.provision_resource:
            getattr(o, "d_provisioner")(d)

    def get_ruser(self, node):
        default = "root"
        if not self.config.has_option('node', "ruser"):
            return default
        h = {}
        s = self.config.get('node', 'ruser').split()
        for e in s:
            l = e.split("@")
            if len(l) == 1:
                default = e
            elif len(l) == 2:
                _ruser, _node = l
                h[_node] = _ruser
            else:
                continue
        if node in h:
            return h[node]
        return default

    def dequeue_actions(self):
        actions = self.collector.call('collector_get_action_queue')
        data = []
        for action in actions:
            ret, out, err = self.dequeue_action(action)
            data.append((action.get('id'), ret, out, err))
        self.collector.call('collector_update_action_queue', data)

    def dequeue_action(self, action):
        if rcEnv.sysname == "Windows":
            nodemgr = os.path.join(rcEnv.pathsvc, "nodemgr.cmd")
            svcmgr = os.path.join(rcEnv.pathsvc, "svcmgr.cmd")
        else:
            nodemgr = os.path.join(rcEnv.pathbin, "nodemgr")
            svcmgr = os.path.join(rcEnv.pathbin, "svcmgr")
        if action.get("svcname") is None:
            cmd = [nodemgr]
        else:
            cmd = [svcmgr, "-s", action.get("svcname")]
        cmd += action.get("command", "").split()
        print("dequeue action %s" % " ".join(cmd))
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        return p.returncode, out, err

    def scanscsi(self):
        try:
            m = __import__("rcDiskInfo"+rcEnv.sysname)
        except ImportError:
            print("scanscsi is not supported on", rcEnv.sysname, file=sys.stderr)
            return 1
        o = m.diskInfo()
        if not hasattr(o, 'scanscsi'):
            print("scanscsi is not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        return o.scanscsi()

    def discover(self):
        self.cloud_init()

    def cloud_init(self):
        r = 0
        for s in self.config.sections():
            try:
                self.cloud_init_section(s)
            except ex.excInitError as e:
                print(str(e), file=sys.stderr)
                r |= 1
        return r

    def cloud_get(self, s):
        if not s.startswith("cloud"):
            return

        if not s.startswith("cloud#"):
            raise ex.excInitError("cloud sections must have a unique name in the form '[cloud#n] in %s"%self.nodeconf)

        if hasattr(self, "clouds") and s in self.clouds:
            return self.clouds[s]

        try:
            cloud_type = self.config.get(s, 'type')
        except:
            raise ex.excInitError("type option is mandatory in cloud section in %s"%self.nodeconf)

        # noop if already loaded
        self.load_auth_config()
        try:
            auth_dict = {}
            for key, val in self.auth_config.items(s):
                auth_dict[key] = val
        except:
            raise ex.excInitError("%s must have a '%s' section"%(self.authconf, s))

        if len(cloud_type) == 0:
            raise ex.excInitError("invalid cloud type in %s"%self.nodeconf)

        mod_name = "rcCloud" + cloud_type[0].upper() + cloud_type[1:].lower()

        try:
            m = __import__(mod_name)
        except ImportError:
            raise ex.excInitError("cloud type '%s' is not supported"%cloud_type)

        if not hasattr(self, "clouds"):
            self.clouds = {}
        c = m.Cloud(s, auth_dict)
        self.clouds[s] = c
        return c

    def cloud_init_section(self, s):
        c = self.cloud_get(s)

        if c is None:
            return

        cloud_id = c.cloud_id()
        svcnames = c.list_svcnames()

        self.cloud_purge_services(cloud_id, map(lambda x: x[1], svcnames))

        for vmname, svcname in svcnames:
            self.cloud_init_service(c, vmname, svcname)

    def cloud_purge_services(self, suffix, svcnames):
        import glob
        envs = glob.glob(os.path.join(rcEnv.pathetc, '*.env'))
        for env in envs:
            svcname = os.path.basename(env).rstrip('.env')
            if svcname.endswith(suffix) and svcname not in svcnames:
                print("purge_service(svcname)", svcname)

    def cloud_init_service(self, c, vmname, svcname):
        import glob
        envs = glob.glob(os.path.join(rcEnv.pathetc, '*.env'))
        env = os.path.join(rcEnv.pathetc, svcname+'.env')
        if env in envs:
            print(svcname, "is already defined")
            return
        print("initialize", svcname)

        defaults = {
          'app': c.app_id(svcname),
          'mode': c.mode,
          'nodes': rcEnv.nodename,
          'service_type': 'TST',
          'vm_name': vmname,
          'cloud_id': c.cid,
        }
        config = ConfigParser.RawConfigParser(defaults)

        try:
            fp = open(env, 'w')
            config.write(fp)
            fp.close()
        except:
            print("failed to write %s"%env, file=sys.stderr)
            raise Exception()

        d = env.rstrip('.env')+'.dir'
        s = env.rstrip('.env')+'.d'
        x = os.path.join(rcEnv.pathbin, "svcmgr")
        b = env.rstrip('.env')
        try:
            os.makedirs(d)
        except:
            pass
        try:
            os.symlink(d, s)
        except:
            pass
        try:
            os.symlink(x, b)
        except:
            pass


if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
