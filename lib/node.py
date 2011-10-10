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

class Options(object):
    def __init__(self):
        self.cron = False
        self.force = False
        self.debug = False
        self.stats_dir = None
        self.stats_start = None
        self.stats_end = None
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
          'comp_check_interval': 61,
          'comp_check_days': '["sunday"]',
          'comp_check_period': '["05:00", "06:00"]',
        }
        self.load_config()
        self.options = Options()
        self.svcs = None
        Freezer.__init__(self, '')
        self.action_desc = {
          'syncservices':   'send var files, config files and configured replications to other nodes for each node service',
          'updateservices': 'refresh var files associated with services',
          'pushasset':      'push asset information to collector',
          'pushservices':   'push service configuration to collector',
          'pushstats':      'push performance metrics to collector',
          'pushpkg':        'push package/version list to collector',
          'pushpatch':      'push patch/version list to collector',
          'pushsym':        'push symmetrix configuration to collector',
          'prkey':          'show persistent reservation key of this node',
          'checks':         'run node sanity checks, push results to collector',
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
          'get': 'get the value of the node configuration parameter pointed by --param',
          'set': 'set a node configuration parameter (pointed by --param) value (pointed by --value)',
          'unset': 'unset a node configuration parameter (pointed by --param)',
          'register': 'obtain a registration number from the collector, used to authenticate the node',
        }
        self.collector = xmlrpcClient.Collector()
        self.cmdworker = rcCommandWorker.CommandWorker()
        try:
            rcos = __import__('rcOs'+rcEnv.sysname)
        except ImportError:
            rcos = __import__('rcOs')
        self.os = rcos.Os()

    def build_services(self, *args, **kwargs):
        if self.svcs is not None:
            return
        self.svcs = svcBuilder.build_services(*args, **kwargs)
        for svc in self.svcs:
             svc.node = self
        if ('autopush' not in kwargs or kwargs['autopush']):
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
        o = Compliance(self.skip_action, self.options, self.collector)
        wrapper = TextWrapper(subsequent_indent="%29s"%"", width=78)
        desc = "Supported commands:\n"
        for a in sorted(self.action_desc):
            if a.startswith("compliance_"):
                if not hasattr(o, a):
                    continue
            elif not hasattr(self, a):
                continue
            text = "  %-26s %s\n"%(a.replace('_', ' '),
                                   self.action_desc[a])
            desc += wrapper.fill(text)
            desc += '\n'
        return desc

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
        else:
            return getattr(self, a)()

    def check_timestamp(self, timestamp_f, comp='more', delay=10):
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
                if comp == "more" and datetime.datetime.now() < limit:
                    return False
                elif comp == "less" and datetime.datetime.now() < limit:
                    return False
                else:
                    return True
                f.close()
        except:
            return True
        return True

    def timestamp(self, timestamp_f, interval):
        if not self.check_timestamp(timestamp_f, 'more', interval):
            return False
        if self.options.force:
            # don't update the timestamp in force mode
            # to not perturb the schedule
            return True
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d ,0755)
        with open(timestamp_f, 'w') as f:
            f.write(str(datetime.datetime.now())+'\n')
            f.close()
        return True

    def in_period(self, period):
        if len(period) == 0:
            return True
        if isinstance(period[0], list):
            r = False
            for p in period:
                 r |= self.in_period(p)
            return r
        elif not isinstance(period[0], unicode) or len(period) != 2 or \
             not isinstance(period[1], unicode):
            print >>sys.stderr, "malformed period: %s"%str(period)
            return False
        start_s, end_s = period
        try:
            start_t = time.strptime(start_s, "%H:%M")
            end_t = time.strptime(end_s, "%H:%M")
            start = start_t.tm_hour * 60 + start_t.tm_min
            end = end_t.tm_hour * 60 + end_t.tm_min
        except:
            print >>sys.stderr, "malformed time string: %s"%str(period)
            return False
        now = datetime.datetime.now()
        now_m = now.hour * 60 + now.minute
        if start <= end:
            if now_m >= start and now_m <= end:
                return True
        elif start > end:
            """
                  XXXXXXXXXXXXXXXXX
                  23h     0h      1h
            """
            if (now_m >= start and now_m <= 1440) or \
               (now_m >= 0 and now_m <= end):
                return True
        return False

    def in_days(self, days):
        now = datetime.datetime.now()
        today = now.strftime('%A').lower()
        if today in map(lambda x: x.lower(), days):
            return True
        return False

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

    def skip_action(self, section, option, fname,
                    cmdline_parm=None,
                    period_option=None,
                    days_option=None,
                    force=False):

        def err(msg):
            print '%s: skip:'%section, msg, '(--force to bypass)'

        if force:
            return False

        # check if we are in allowed period
        if self.skip_action_period(section, period_option):
            err('out of allowed periods')
            return True

        # check if we are in allowed days of week
        if self.skip_action_days(section, days_option):
            err('out of allowed days')
            return True

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

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', fname)
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            err('last run < interval')
            return True

        return False

    def pushstats(self):
        if self.skip_action('stats', 'push_interval', 'last_stats_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
            return

        # get interval from config file
        if self.config.has_section('stats'):
            interval = self.config.getint('stats', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        self.collector.call('push_stats', force=self.options.force,
                                stats_dir=self.options.stats_dir,
                                stats_start=self.options.stats_start,
                                stats_end=self.options.stats_end,
                                interval=2*interval)

    def pushpkg(self):
        if self.skip_action('packages', 'push_interval', 'last_pkg_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
            return

        self.collector.call('push_pkg')

    def pushpatch(self):
        if self.skip_action('patches', 'push_interval', 'last_patch_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
            return

        self.collector.call('push_patch')

    def pushasset(self):
        if self.skip_action('asset', 'push_interval', 'last_asset_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
            return

        self.collector.call('push_asset', self)

    def pushsym(self):
        if self.skip_action('sym', 'push_interval', 'last_sym_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
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
                                days_option='sync_days',
                                force=self.options.force):
                    continue
            l.append(self.config.get(s, 'svcname'))
        return l

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
                            days_option='push_days',
                            force=self.options.force):
            return

        if self.svcs is None:
            self.build_services()
        for svc in self.svcs:
            svc.cron = self.options.cron
            svc.action('push')

    def prkey(self):
        from rcGlobalEnv import rcEnv
        m = __import__('hostid'+rcEnv.sysname)
        print m.hostid()

    def checks(self):
        if self.skip_action('checks', 'push_interval', 'last_checks_push',
                            period_option='push_period',
                            days_option='push_days',
                            force=self.options.force):
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
