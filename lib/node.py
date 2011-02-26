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

from svc import Svc
from freezer import Freezer
import svcBuilder
import xmlrpcClient
import os
import ConfigParser
import datetime
import time
import sys

class Options(object):
    def __init__(self):
        self.force = False
        self.debug = False
        self.stats_dir = None
        self.stats_start = None
        self.stats_end = None
        os.environ['LANG'] = 'C'

class Node(Svc, Freezer):
    """ Defines a cluster node.  It contain list of Svc.
        Implements node-level actions and checks.
    """
    def __init__(self):
        self.nodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', 'node.conf')
        config_defaults = {
          'host_mode': 'TST',
          'push_interval': 1439,
          'sync_interval': 1439,
          'sync_period': '["04:00", "06:00"]',
        }
        self.config = ConfigParser.RawConfigParser(config_defaults)
        self.config.read(self.nodeconf)
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
        }

    def format_desc(self):
        from textwrap import TextWrapper
        wrapper = TextWrapper(subsequent_indent="%29s"%"", width=78)
        desc = "Supported commands:\n"
        for a in sorted(self.action_desc):
            if not hasattr(self, a):
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
        except:
            print >>sys.stderr, "malformed time string: %s"%str(period)
            return False
        now = datetime.datetime.now()
        if start_t <= end_t:
            if now.hour >= start_t.tm_hour and \
               now.minute >= start_t.tm_min and \
               now.hour <= end_t.tm_hour and \
               now.minute <= end_t.tm_min:
                return True
        elif start_t > end_t:
            """
                  XXXXXXXXXXXXXXXXX
                  23h     0h      1h
            """
            if (now.hour >= start_t.tm_hour and \
                now.minute >= start_t.tm_min and \
                now.hour <= 23 and \
                now.minute <= 59) or \
               (now.hour >= 0 and \
                now.minute >= 0 and \
                now.hour <= end_t.tm_hour and \
                now.minute <= end_t.tm_min):
                end = end + datetime.timedelta(days=1)
                return True
        return False

    def skip_action_period(self, section, option):
        if option is None:
            return False

        if self.config.has_section(section):
            period_s = self.config.get(section, option)
        else:
            period_s = self.config.get('DEFAULT', option)

        try:
            import json
            period = json.loads(period_s)
        except:
            print >>sys.stderr, "malformed parameter value: %s.%s"%(section, option)
            return True

        if self.in_period(period):
            return False

        return True

    def skip_action(self, section, option, fname,
                    cmdline_parm=None,
                    period_option=None,
                    force=False):

        if force:
            return False

        # check if we are in allowed period
        if self.skip_action_period(section, period_option):
            return True

        # get interval from config file
        if self.config.has_section(section):
            interval = self.config.getint(section, option)
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
            return True

        return False

    def pushstats(self):
        if self.skip_action('stats', 'push_interval', 'last_stats_push',
                            force=self.options.force):
            return

        # get interval from config file
        if self.config.has_section('stats'):
            interval = self.config.getint('stats', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        xmlrpcClient.push_stats(force=self.options.force,
                                stats_dir=self.options.stats_dir,
                                stats_start=self.options.stats_start,
                                stats_end=self.options.stats_end,
                                interval=2*interval)

    def pushpkg(self):
        if self.skip_action('packages', 'push_interval', 'last_pkg_push',
                            force=self.options.force):
            return

        xmlrpcClient.push_pkg()

    def pushpatch(self):
        if self.skip_action('patches', 'push_interval', 'last_patch_push',
                            force=self.options.force):
            return

        xmlrpcClient.push_patch()

    def pushasset(self):
        if self.skip_action('asset', 'push_interval', 'last_asset_push',
                            force=self.options.force):
            return

        xmlrpcClient.push_asset(self)

    def pushsym(self):
        if self.skip_action('sym', 'push_interval', 'last_sym_push',
                            force=self.options.force):
            return

        xmlrpcClient.push_sym()

    def syncservices(self):
        if self.skip_action('sync', 'sync_interval', 'last_sync',
                            period_option='sync_period',
                            force=self.options.force):
            return

        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.force = self.options.force
            svc.action('syncall')

    def updateservices(self):
        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.action('presync')

    def pushservices(self):
        if self.skip_action('svcconf', 'push_interval', 'last_svcconf_push',
                            force=self.options.force):
            return

        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.action('push')

    def prkey(self):
        from rcGlobalEnv import rcEnv
        m = __import__('hostid'+rcEnv.sysname)
        print m.hostid()

    def checks(self):
        if self.skip_action('checks', 'push_interval', 'last_checks_push',
                            force=self.options.force):
            return

        import checks
        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        c = checks.checks(self.svcs)
        c.do_checks()

    def compliance_check(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_checks()

    def compliance_fix(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_fix()

    def compliance_fixable(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_fixable()

    def compliance_show_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_show_moduleset()

    def compliance_attach_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_attach_moduleset()

    def compliance_detach_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_detach_moduleset()

    def compliance_show_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_show_ruleset()

    def compliance_attach_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_attach_ruleset()

    def compliance_detach_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_detach_ruleset()

    def compliance_list_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_list_rulesets()

    def compliance_list_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_list_modulesets()

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
            fp = open(self.nodeconf, 'w')
            self.config.write(fp)
            fp.close()
        except:
            print >>sys.stderr, "failed to write new %s"%self.nodeconf
            return 1
        return 0

if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
