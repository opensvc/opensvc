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

from svc import Svc
from freezer import Freezer
import svcBuilder
import xmlrpcClient
import os
import ConfigParser
import datetime
import time
import sys
import json
from rcGlobalEnv import rcEnv
import rcCommandWorker
import socket
import rcLogger
import rcUtilities

class Options(object):
    def __init__(self):
        self.cron = False
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
        self.delay_done = False
        self.nodename = socket.gethostname()
        self.nodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', 'node.conf')
        self.dotnodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', '.node.conf')
        self.setup_sync_flag = os.path.join(rcEnv.pathvar, 'last_setup_sync')
        self.config_defaults = {
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
        self.options = Options()
        self.svcs = None
        Freezer.__init__(self, '')
        self.action_desc = {
          'Node actions': {
            'shutdown': 'shutdown the node to powered off state',
            'reboot': 'reboot the node',
          },
          'Service actions': {
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
            'pushpkg':        'push package/version list to collector',
            'pushpatch':      'push patch/version list to collector',
            'pushsym':        'push symmetrix configuration to collector',
            'pusheva':        'push HP EVA configuration to collector',
            'pushdcs':        'push Datacore configuration to collector',
            'pushibmsvc':     'push IBM SVC configuration to collector',
            'push_appinfo':   'push services application launchers appinfo key/value pairs to database',
            'checks':         'run node sanity checks, push results to collector',
          },
          'Misc': {
            'prkey':          'show persistent reservation key of this node',
          },
          'Compliance': {
            'compliance_check': 'run compliance checks',
            'compliance_fix':   'run compliance fixes',
            'compliance_fixable': 'verify compliance fixes prerequisites',
            'compliance_show_moduleset': 'show compliance rules applying to this node',
            'compliance_list_moduleset': 'list available compliance modulesets. --moduleset f% limit the scope to modulesets matching the f% pattern.',
            'compliance_attach_moduleset': 'attach moduleset specified by --moduleset for this node',
            'compliance_detach_moduleset': 'detach moduleset specified by --moduleset for this node',
            'compliance_list_ruleset': 'list available compliance rulesets. --ruleset f% limit the scope to rulesets matching the f% pattern.',
            'compliance_show_ruleset': 'show compliance rules applying to this node',
            'compliance_attach_ruleset': 'attach ruleset specified by --ruleset for this node',
            'compliance_detach_ruleset': 'detach ruleset specified by --ruleset for this node',
          },
          'Collector management': {
            'collector_events': 'display node events during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
            'collector_alerts': 'display node alerts',
            'collector_checks': 'display node checks',
            'collector_status': 'display node services status according to the collector',
            'collector_list_actions': 'list actions on the node, whatever the service, during the period specified by --begin/--end. --end defaults to now. --begin defaults to 7 days ago',
            'collector_ack_action': 'acknowledge an action error on the node. an acknowlegment can be completed by --author (defaults to root@nodename) and --comment',
            'collector_show_actions': 'show actions detailled log. a single action is specified by --id. a range is specified by --begin/--end dates. --end defaults to now. --begin defaults to 7 days ago',
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
        if self.svcs is not None:
            return
        autopush = True
        if 'autopush' in kwargs:
            if not kwargs['autopush']:
                autopush = False
            del kwargs['autopush']
        self.svcs = svcBuilder.build_services(*args, **kwargs)
        for svc in self.svcs:
             svc.node = self
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
            print >>sys.stderr, "failed to write new %s"%self.dotnodeconf
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
            print >>sys.stderr, "failed to write new %s"%self.nodeconf
            raise Exception()
        self.load_config()

    def load_config(self):
        self.config = ConfigParser.RawConfigParser(self.config_defaults)
        self.config.read(self.nodeconf)
        self.config.read(self.dotnodeconf)

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
                pass
        self.svcs.append(s)
        return self

    def action(self, a):
        if a.startswith("compliance_"):
            from compliance import Compliance
            o = Compliance(self.skip_action, self.options, self.collector)
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
            os.makedirs(timestamp_d ,0755)
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
        print "start:", start
        print "end:", end
        print "now:", now
        print "length:", length
        print "elapsed:", elapsed
        print "elapsed_pct:", elapsed_pct
        print "p:", p
        print "r:", r
        """

        if r >= p:
            print "win probabilistic challenge: %d, over %d"%(r, p)
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
            print >>sys.stderr, "malformed time string: %s"%str(period)
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
            print >>sys.stderr, "malformed period: %s"%str(period)
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
            print >>sys.stderr, "malformed parameter value: %s.period"%section
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
            print >>sys.stderr, "malformed parameter value: %s.period"%section
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
            print >>sys.stderr, "malformed parameter value: %s.days"%section
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
            print '%s: skip:'%section, msg, '(--force to bypass)'

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
            print "delay action for %d secs to level database load"%delay
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

        # set interval to grab from the begining of the last allowed period
        # to now
        interval = 1440 + now - start

        self.collector.call('push_stats', force=self.options.force,
                                stats_dir=self.options.stats_dir,
                                stats_start=self.options.begin,
                                stats_end=self.options.end,
                                interval=interval)

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

    def pushdcs(self):
        if self.skip_action('dcs', 'push_interval', 'last_dcs_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
            return

        self.collector.call('push_dcs')

    def pusheva(self):
        if self.skip_action('eva', 'push_interval', 'last_eva_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_eva')

    def pushibmsvc(self):
        if self.skip_action('ibmsvc', 'push_interval', 'last_ibmsvc_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_ibmsvc')

    def pushsym(self):
        if self.skip_action('sym', 'push_interval', 'last_sym_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        self.collector.call('push_sym')

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
        print "TODO"

    def reboot(self):
        print "TODO"

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
        print m.hostid()

    def checks(self):
        if self.skip_action('checks', 'push_interval', 'last_checks_push',
                            period_option='push_period',
                            days_option='push_days'):
            return

        import checks
        self.build_services()
        c = checks.checks(self.svcs)
        c.node = self
        c.do_checks()

    def unset(self):
        if self.options.param is None:
            print >>sys.stderr, "no parameter. set --param"
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print >>sys.stderr, "malformed parameter. format as 'section.key'"
            return 1
        section, option = l
        if not self.config.has_section(section):
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
        if self.options.param is None:
            print >>sys.stderr, "no parameter. set --param"
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print >>sys.stderr, "malformed parameter. format as 'section.key'"
            return 1
        section, option = l
        if not self.config.has_section(section):
            print >>sys.stderr, "section '%s' not found"%section
            return 1
        if not self.config.has_option(section, option):
            print >>sys.stderr, "option '%s' not found in section '%s'"%(option, section)
            return 1
        print self.config.get(section, option)
        return 0

    def set(self):
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
            print >>sys.stderr, "failed to obtain a registration number"
            return 1
        elif isinstance(u, list):
            print >>sys.stderr, u[0]
            return 1
        try:
            if not self.config.has_section('node'):
                self.config.add_section('node')
            self.config.set('node', 'uuid', u)
            self.write_config()
        except:
            print >>sys.stderr, "failed to write registration number: %s"%u
            return 1
        print "registered"
        return 0

    def service_action_worker(self, s, **kwargs):
        r = s.action(**kwargs)
        self.close()
        sys.exit(r)

if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
